from __future__ import annotations

import json
import hashlib
import base64
import logging
import os
import random
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import quote

import requests

GRAPH = "https://graph.microsoft.com/v1.0"
RETRY_STATUSES = {429, 500, 502, 503, 504}
LOGGER = logging.getLogger(__name__)
USER_AGENT = "dataprepkit-sharepoint/1"
DEFAULT_TIMEOUT = (15, 120)


class SharePointDownloadError(RuntimeError):
    """Raised when one or more SharePoint files cannot be downloaded."""

    def __init__(self, failures: dict[str, str]):
        self.failures = failures
        super().__init__(f"{len(failures)} SharePoint file(s) failed: {failures}")


class SharePointManifestError(ValueError):
    """Raised when a download manifest cannot be read or is invalid."""




def get_graph_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    if not all((tenant_id, client_id, client_secret)):
        raise ValueError("tenant_id, client_id, and client_secret are required")
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    for attempt in range(4):
        try:
            response = requests.post(
                url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "scope": "https://graph.microsoft.com/.default",
                },
                headers={"User-Agent": USER_AGENT},
                timeout=(15, 30),
            )
            if response.status_code in RETRY_STATUSES and attempt < 3:
                response.close()
                time.sleep(min(60.0, 2 ** attempt + random.random()))
                continue
            response.raise_for_status()
            return response.json()["access_token"]
        except (requests.Timeout, requests.ConnectionError):
            if attempt == 3:
                raise
            time.sleep(min(60.0, 2 ** attempt + random.random()))
    raise RuntimeError("Token request retries exhausted")


def get_sharepoint_site_id(token: str, hostname: str, site_path: str) -> str:
    site_path = "/" + site_path.strip("/")
    url = f"{GRAPH}/sites/{hostname}:{quote(site_path, safe='/')}"
    with requests.get(
        url,
        headers={"Authorization": f"Bearer {token}", "User-Agent": USER_AGENT},
        timeout=(15, 30),
    ) as response:
        response.raise_for_status()
        return response.json()["id"]


def _delay(response, attempt: int) -> float:
    value = response.headers.get("Retry-After")
    if value:
        try:
            return max(0.0, float(value))
        except ValueError:
            try:
                parsed = parsedate_to_datetime(value)
                return max(0.0, (parsed - datetime.now(timezone.utc)).total_seconds())
            except (TypeError, ValueError, OverflowError):
                pass
    return min(60.0, 2 ** attempt + random.random())


def _validate_timeout(timeout: tuple[float, float]) -> None:
    if len(timeout) != 2 or any(value <= 0 for value in timeout):
        raise ValueError("timeout must contain two positive values")


def _request(
    url: str,
    headers: dict,
    max_retries: int,
    stream: bool = False,
    timeout: tuple[float, float] = DEFAULT_TIMEOUT,
):
    """Return an open response. The caller must close it."""
    for attempt in range(max_retries + 1):
        try:
            response = requests.get(
                url,
                headers={**headers, "User-Agent": USER_AGENT},
                timeout=timeout,
                stream=stream,
            )
        except (requests.Timeout, requests.ConnectionError, requests.ChunkedEncodingError):
            if attempt == max_retries:
                raise
            time.sleep(min(60.0, 2 ** attempt + random.random()))
            continue
        if response.status_code in RETRY_STATUSES and attempt < max_retries:
            delay = _delay(response, attempt)
            response.close()
            time.sleep(delay)
            continue
        try:
            response.raise_for_status()
        except Exception:
            response.close()
            raise
        return response
    raise RuntimeError("Request retries exhausted")


def _get_json(
    url: str,
    headers: dict,
    max_retries: int,
    timeout: tuple[float, float],
) -> dict:
    with _request(url, headers, max_retries, timeout=timeout) as response:
        return response.json()


def _get_all(
    url: str,
    headers: dict,
    max_retries: int,
    timeout: tuple[float, float],
) -> list[dict]:
    items = []
    while url:
        data = _get_json(url, headers, max_retries, timeout)
        items.extend(data.get("value", []))
        url = data.get("@odata.nextLink")
    return items


def _glob_regex(pattern: str):
    """* and ? match within a path segment; ** matches zero or more directories."""
    parts = pattern.split("/")
    expression = "^"
    for index, part in enumerate(parts):
        if part == "**":
            expression += ".*" if index == len(parts) - 1 else "(?:[^/]+/)*"
            continue
        for character in part:
            expression += (
                "[^/]*" if character == "*" else
                "[^/]" if character == "?" else
                re.escape(character)
            )
        if index < len(parts) - 1:
            expression += "/"
    return re.compile(expression + "$")


def _relative_output_path(pattern: str, matched_path: str) -> Path:
    """Literal input -> filename; glob input -> path from first wildcard segment."""
    pattern_parts = pattern.split("/")
    matched_parts = matched_path.split("/")
    wildcard_at = next(
        (index for index, part in enumerate(pattern_parts) if "*" in part or "?" in part),
        None,
    )
    destination_parts = matched_parts[-1:] if wildcard_at is None else matched_parts[wildcard_at:]
    if not destination_parts or any(
        part in ("", ".", "..") or "*" in part or "?" in part or "\\" in part
        for part in destination_parts
    ):
        raise ValueError(f"Unsafe resolved SharePoint destination: {matched_path!r}")
    return Path(*destination_parts)


def _save_metadata(path: Path, metadata: dict) -> None:
    temporary = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(metadata, handle, indent=2, ensure_ascii=False)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _content_hash(item: dict) -> tuple[str, str] | None:
    file_facet = item.get("file") or {}
    hashes = file_facet.get("hashes") or {}
    for algorithm in ("sha1Hash", "quickXorHash"):
        value = hashes.get(algorithm)
        if value:
            return algorithm.removesuffix("Hash").lower(), value
    return None


def _metadata_file(metadata_dir: Path, drive_id: str, item_id: str) -> Path:
    return metadata_dir / f"{drive_id}_{item_id}.json"


def _load_metadata(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        with path.open(encoding="utf-8") as handle:
            metadata = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise SharePointManifestError(f"Cannot read metadata {path}") from exc
    if not isinstance(metadata, dict):
        raise SharePointManifestError(f"Metadata must contain a JSON object: {path}")
    return metadata


class _QuickXorHash:
    """Streaming implementation of Microsoft QuickXorHash."""

    _WIDTH = 20
    _SHIFT = 11

    def __init__(self):
        self._data = bytearray(self._WIDTH)
        self._length = 0
        self._position = 0

    def update(self, chunk: bytes) -> None:
        for value in chunk:
            bit = (self._position * self._SHIFT) % (self._WIDTH * 8)
            index, offset = divmod(bit, 8)
            self._data[index] ^= (value << offset) & 0xFF
            self._data[(index + 1) % self._WIDTH] ^= value >> (8 - offset) if offset else 0
            self._position += 1
            self._length += 1

    def hexdigest(self) -> str:
        length = self._length.to_bytes(8, "little", signed=False)
        for index, value in enumerate(length):
            self._data[index] ^= value
        return base64.b64encode(bytes(reversed(self._data))).decode("ascii")


def _download_sharepoint_files(
    token: str,
    site_id: str,
    file_path: str,
    output_dir: str = ".",
    metadata_dir: str | None = None,
    download_metadata: bool = False,
    skip_unchanged: bool = True,
    max_workers: int = 5,
    max_retries: int = 4,
    checkpoint_every: int = 10,
    timeout: tuple[float, float] = DEFAULT_TIMEOUT,
    fail_on_metadata_error: bool = False,
) -> list[Path]:
    """Download matching files from the site's default document library.

    Examples:
      Engineering/All dimensions/file.xlsx -> output_dir/file.xlsx
      Engineering/All dimensions/*         -> output_dir/<matched filename>
      Engineering/All dimensions/**        -> output_dir/<relative tree>

    The JSON manifest is managed by the main thread. Use one sync at a time
    for a given manifest and destination directory.
    """
    if max_workers < 1 or max_retries < 0 or checkpoint_every < 1:
        raise ValueError("max_workers/checkpoint_every must be >= 1; max_retries >= 0")
    _validate_timeout(timeout)

    pattern = file_path.strip("/")
    if not pattern or "\\" in pattern or any(
        segment in ("", ".", "..") for segment in pattern.split("/")
    ):
        raise ValueError("Use a SharePoint-relative path with unescaped forward slashes")

    matcher = _glob_regex(pattern)
    has_wildcard = any("*" in segment or "?" in segment for segment in pattern.split("/"))
    destination_root = Path(output_dir)
    metadata_dir = (
        Path(metadata_dir)
        if metadata_dir is not None
        else destination_root / ".dataprepkit" / "metadata"
    )
    if any(char in str(destination_root) for char in ("*", "?")):
        raise ValueError(f"output_dir must not contain wildcards: {destination_root}")
    if any(char in str(metadata_dir) for char in ("*", "?")):
        raise ValueError(f"metadata_dir must not contain wildcards: {metadata_dir}")
    destination_root.mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    }
    drive_id = _get_json(f"{GRAPH}/sites/{site_id}/drive", headers, max_retries, timeout)["id"]

    def children_url(folder: str) -> str:
        if folder:
            return f"{GRAPH}/drives/{drive_id}/root:/{quote(folder, safe='/')}:/children"
        return f"{GRAPH}/drives/{drive_id}/root/children"

    matches = []
    if not has_wildcard:
        # Literal paths are resolved directly: no wildcard directory enumeration.
        item = _get_json(
            f"{GRAPH}/drives/{drive_id}/root:/{quote(pattern, safe='/')}:",
            headers,
            max_retries,
            timeout,
        )
        if "file" not in item:
            raise ValueError(f"SharePoint path is not a file: {pattern}")
        matches.append((pattern, item))
    else:
        fixed_parts = []
        for part in pattern.split("/"):
            if "*" in part or "?" in part:
                break
            fixed_parts.append(part)
        prefix = "/".join(fixed_parts)
        folders = [prefix]
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while folders:
                futures = {
                    executor.submit(
                        _get_all,
                        children_url(folder),
                        headers,
                        max_retries,
                        timeout,
                    ): folder
                    for folder in folders
                }
                next_folders = []
                for future in as_completed(futures):
                    folder = futures[future]
                    for item in future.result():
                        name = item["name"]
                        if name in ("", ".", "..") or "/" in name or "\\" in name:
                            raise ValueError(f"Unsafe SharePoint item name: {name!r}")
                        remote_path = f"{folder}/{name}" if folder else name
                        if "file" in item and matcher.fullmatch(remote_path):
                            matches.append((remote_path, item))
                        elif "folder" in item:
                            next_folders.append(remote_path)
                folders = next_folders

    matches.sort(key=lambda pair: pair[0])
    LOGGER.info("Matched %s SharePoint file(s)", len(matches))

    active_metadata = {
        _metadata_file(metadata_dir, drive_id, item["id"])
        for _, item in matches
    }
    for metadata_file in metadata_dir.glob("*.json"):
        if metadata_file not in active_metadata:
            metadata_file.unlink()

    # Validate every destination before creating any destination subfolders.
    destinations = {}
    for remote_path, _ in matches:
        relative_destination = _relative_output_path(pattern, remote_path)
        destination = (destination_root / relative_destination).resolve()
        try:
            destination.relative_to(destination_root.resolve())
        except ValueError as exc:
            raise ValueError(f"Unsafe resolved destination: {destination}") from exc
        if destination in destinations:
            raise ValueError(
                f"Destination collision: {destinations[destination]!r} and "
                f"{remote_path!r} both map to {destination}"
            )
        destinations[destination] = remote_path

    def process_file(remote_path: str, item: dict):
        item_id = item["id"]
        key = f"{drive_id}:{item_id}"
        metadata_file = _metadata_file(metadata_dir, drive_id, item_id)
        previous = _load_metadata(metadata_file)
        destination = (destination_root / _relative_output_path(pattern, remote_path)).resolve()

        current_hash = _content_hash(item)
        previous_hash = previous.get("content_hash")
        unchanged = (
            current_hash is not None
            and previous_hash == {
                "algorithm": current_hash[0],
                "value": current_hash[1],
            }
        )
        should_download = (
            not skip_unchanged
            or not destination.is_file()
            or not unchanged
        )

        if should_download:
            destination.parent.mkdir(parents=True, exist_ok=True)
            download_url = f"{GRAPH}/drives/{drive_id}/items/{item_id}/content"
            temporary = destination.with_name(f"{destination.name}.{uuid.uuid4().hex}.part")
            try:
                with _request(
                    download_url,
                    headers,
                    max_retries,
                    stream=True,
                    timeout=timeout,
                ) as response:
                    digest = hashlib.sha1()
                    quickxor = _QuickXorHash()
                    with temporary.open("wb") as handle:
                        for chunk in response.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                handle.write(chunk)
                                digest.update(chunk)
                                quickxor.update(chunk)
                if current_hash:
                    actual_hash = (
                        digest.hexdigest()
                        if current_hash[0] == "sha1"
                        else quickxor.hexdigest()
                    )
                    if actual_hash.lower() != current_hash[1].lower():
                        raise ValueError(
                            f"{current_hash[0]} checksum mismatch for {remote_path}"
                        )
                os.replace(temporary, destination)
            finally:
                temporary.unlink(missing_ok=True)

        updated = dict(item)
        if download_metadata:
            url = f"{GRAPH}/drives/{drive_id}/items/{item_id}/listItem?$expand=fields"
            try:
                updated["sharepoint_fields"] = _get_json(
                    url, headers, max_retries, timeout,
                ).get("fields", {})
            except requests.RequestException as exc:
                if fail_on_metadata_error:
                    raise
                LOGGER.warning("Custom metadata unavailable for %s: %s", remote_path, exc)
                if "sharepoint_fields" in previous:
                    updated["sharepoint_fields"] = previous["sharepoint_fields"]
        elif "sharepoint_fields" in previous:
            updated["sharepoint_fields"] = previous["sharepoint_fields"]

        now = datetime.now(timezone.utc).isoformat()
        updated.update({
            "file_path": remote_path,
            "lakehouse_path": str(destination),
            "last_checked": now,
            "last_downloaded": now if should_download else previous.get("last_downloaded"),
        })
        if current_hash:
            updated["content_hash"] = {
                "algorithm": current_hash[0],
                "value": current_hash[1],
            }
        return destination, metadata_file, updated, should_download

    successful = []
    failures = {}
    downloaded = skipped = pending = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_file, remote_path, item): remote_path
            for remote_path, item in matches
        }
        for future in as_completed(futures):
            remote_path = futures[future]
            try:
                destination, metadata_file, updated, did_download = future.result()
                _save_metadata(metadata_file, updated)
                successful.append(destination)
                if did_download:
                    downloaded += 1
                    LOGGER.info("Downloaded: %s -> %s", remote_path, destination)
                else:
                    skipped += 1
                    LOGGER.info("Skipped unchanged: %s", remote_path)
                pending += 1
                if pending >= checkpoint_every:
                    pending = 0
            except Exception as exc:
                failures[remote_path] = str(exc)
                LOGGER.error("Failed to download %s: %s", remote_path, exc)

    LOGGER.info(
        "SharePoint sync: matched=%s, downloaded=%s, skipped=%s, failed=%s",
        len(matches), downloaded, skipped, len(failures),
    )
    if failures:
        raise SharePointDownloadError(failures)
    return sorted(successful)


def download_sharepoint_file(
    hostname: str,
    site_path: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    file_path: str,
    output_dir: str = ".",
    metadata_dir: str | None = None,
    download_metadata: bool = False,
    skip_unchanged: bool = True,
    max_workers: int = 5,
    max_retries: int = 4,
    checkpoint_every: int = 10,
    timeout: tuple[float, float] = DEFAULT_TIMEOUT,
    fail_on_metadata_error: bool = False,
) -> list[Path]:
    """Authenticate, resolve the SharePoint site, and sync matching files.

    file_path is relative to the site's default document library.
    Wildcards: * and ? within a segment; ** across subdirectories.
    output_dir and metadata_dir must be literal filesystem paths.
    """
    token = get_graph_token(tenant_id, client_id, client_secret)
    site_id = get_sharepoint_site_id(token, hostname, site_path)
    return _download_sharepoint_files(
        token=token,
        site_id=site_id,
        file_path=file_path,
        output_dir=output_dir,
        metadata_dir=metadata_dir,
        download_metadata=download_metadata,
        skip_unchanged=skip_unchanged,
        max_workers=max_workers,
        max_retries=max_retries,
        checkpoint_every=checkpoint_every,
        timeout=timeout,
        fail_on_metadata_error=fail_on_metadata_error,
    )


