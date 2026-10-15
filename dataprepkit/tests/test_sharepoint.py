import hashlib
from pathlib import Path

import pytest

from dataprepkit import sharepoint


class FakeResponse:
    def __init__(self, payload=None, content=(), status_code=200):
        self.payload = payload
        self.content = content
        self.status_code = status_code
        self.headers = {}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def close(self):
        pass

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self.payload

    def iter_content(self, chunk_size):
        return iter(self.content)


def test_download_sharepoint_file_orchestrates_authentication(monkeypatch, tmp_path):
    calls = {}

    def fake_token(tenant_id, client_id, client_secret):
        calls["token"] = (tenant_id, client_id, client_secret)
        return "token"

    def fake_site(token, hostname, site_path):
        calls["site"] = (token, hostname, site_path)
        return "site-id"

    def fake_download(**kwargs):
        calls["download"] = kwargs
        return [Path("downloaded.xlsx")]

    monkeypatch.setattr(sharepoint, "get_graph_token", fake_token)
    monkeypatch.setattr(sharepoint, "get_sharepoint_site_id", fake_site)
    monkeypatch.setattr(sharepoint, "_download_sharepoint_files", fake_download)

    result = sharepoint.download_sharepoint_file(
        "contoso.sharepoint.com", "/sites/data", "tenant", "client", "secret",
        "input.xlsx", output_dir=str(tmp_path), skip_unchanged=False,
    )

    assert result == [Path("downloaded.xlsx")]
    assert calls["token"] == ("tenant", "client", "secret")
    assert calls["site"] == ("token", "contoso.sharepoint.com", "/sites/data")
    assert calls["download"]["site_id"] == "site-id"
    assert calls["download"]["skip_unchanged"] is False


def test_download_literal_file_writes_content_and_manifest(monkeypatch, tmp_path):
    responses = iter([
        FakeResponse({"id": "drive-id"}),
        FakeResponse({"id": "file-id", "name": "input.xlsx", "file": {}, "eTag": "v1"}),
        FakeResponse(content=[b"hello", b" world"]),
    ])
    monkeypatch.setattr(sharepoint.requests, "get", lambda *args, **kwargs: next(responses))

    result = sharepoint._download_sharepoint_files(
        "token", "site-id", "folder/input.xlsx", output_dir=str(tmp_path),
        max_retries=0,
    )

    destination = tmp_path / "input.xlsx"
    assert result == [destination]
    assert destination.read_bytes() == b"hello world"
    metadata = next((tmp_path / ".dataprepkit" / "metadata").glob("*.json"))
    assert '"file_path": "folder/input.xlsx"' in metadata.read_text(encoding="utf-8")


def test_download_rejects_unsafe_relative_paths(tmp_path):
    for file_path in ("", "../secret.xlsx", "folder\\file.xlsx", "folder//file.xlsx"):
        with pytest.raises(ValueError, match="SharePoint-relative path"):
            sharepoint._download_sharepoint_files(
                "token", "site-id", file_path, output_dir=str(tmp_path),
            )


def test_request_retries_transient_http_error(monkeypatch):
    responses = iter([FakeResponse(status_code=503), FakeResponse(payload={"ok": True})])
    monkeypatch.setattr(sharepoint.requests, "get", lambda *args, **kwargs: next(responses))
    monkeypatch.setattr(sharepoint.time, "sleep", lambda _: None)

    with sharepoint._request("https://example.test", {}, max_retries=1) as response:
        assert response.json() == {"ok": True}


def test_failed_file_raises_structured_exception(monkeypatch, tmp_path):
    responses = iter([
        FakeResponse({"id": "drive-id"}),
        FakeResponse({"id": "file-id", "file": {}}),
        FakeResponse(status_code=403),
    ])
    monkeypatch.setattr(sharepoint.requests, "get", lambda *args, **kwargs: next(responses))

    with pytest.raises(sharepoint.SharePointDownloadError) as error:
        sharepoint._download_sharepoint_files(
            "token", "site-id", "input.xlsx", output_dir=str(tmp_path),
            max_retries=0,
        )
    assert error.value.failures == {"input.xlsx": "HTTP 403"}


def test_token_requires_credentials():
    with pytest.raises(ValueError, match="required"):
        sharepoint.get_graph_token("", "client", "secret")


def test_corrupt_metadata_raises_clear_error(tmp_path):
    metadata = tmp_path / "metadata.json"
    metadata.write_text("not json", encoding="utf-8")

    with pytest.raises(sharepoint.SharePointManifestError, match="Cannot read metadata"):
        sharepoint._load_metadata(metadata)


def test_wildcard_download_preserves_relative_tree(monkeypatch, tmp_path):
    responses = iter([
        FakeResponse({"id": "drive-id"}),
        FakeResponse({
            "value": [
                {"id": "folder-id", "name": "nested", "folder": {}},
            ],
        }),
        FakeResponse({"value": [{"id": "file-id", "name": "input.xlsx", "file": {}}]}),
        FakeResponse(content=[b"data"]),
    ])
    monkeypatch.setattr(sharepoint.requests, "get", lambda *args, **kwargs: next(responses))

    result = sharepoint._download_sharepoint_files(
        "token", "site-id", "folder/**", output_dir=str(tmp_path), max_retries=0,
    )

    assert result == [tmp_path / "nested" / "input.xlsx"]


def test_invalid_metadata_shape_raises_clear_error(tmp_path):
    metadata = tmp_path / "metadata.json"
    metadata.write_text("[]", encoding="utf-8")

    with pytest.raises(sharepoint.SharePointManifestError, match="JSON object"):
        sharepoint._load_metadata(metadata)


def test_content_hash_skips_unchanged_file(monkeypatch, tmp_path):
    content = b"stable content"
    sha1 = hashlib.sha1(content).hexdigest()
    responses = iter([
        FakeResponse({"id": "drive-id"}),
        FakeResponse({"id": "file-id", "file": {"hashes": {"sha1Hash": sha1}}}),
        FakeResponse(content=[content]),
    ])
    monkeypatch.setattr(sharepoint.requests, "get", lambda *args, **kwargs: next(responses))
    sharepoint._download_sharepoint_files(
        "token", "site-id", "input.xlsx", output_dir=str(tmp_path), max_retries=0,
    )

    second_responses = iter([
        FakeResponse({"id": "drive-id"}),
        FakeResponse({"id": "file-id", "file": {"hashes": {"sha1Hash": sha1}}}),
    ])
    monkeypatch.setattr(
        sharepoint.requests, "get", lambda *args, **kwargs: next(second_responses)
    )
    result = sharepoint._download_sharepoint_files(
        "token", "site-id", "input.xlsx", output_dir=str(tmp_path), max_retries=0,
    )

    assert result == [tmp_path / "input.xlsx"]


def test_sha1_mismatch_raises_download_error(monkeypatch, tmp_path):
    responses = iter([
        FakeResponse({"id": "drive-id"}),
        FakeResponse({
            "id": "file-id",
            "file": {"hashes": {"sha1Hash": "0" * 40}},
        }),
        FakeResponse(content=[b"wrong content"]),
    ])
    monkeypatch.setattr(sharepoint.requests, "get", lambda *args, **kwargs: next(responses))

    with pytest.raises(sharepoint.SharePointDownloadError, match="checksum mismatch"):
        sharepoint._download_sharepoint_files(
            "token", "site-id", "input.xlsx", output_dir=str(tmp_path), max_retries=0,
        )


def test_missing_content_hash_does_not_skip_existing_file(monkeypatch, tmp_path):
    destination = tmp_path / "input.xlsx"
    destination.write_bytes(b"old content")
    responses = iter([
        FakeResponse({"id": "drive-id"}),
        FakeResponse({"id": "file-id", "file": {}, "eTag": "unchanged"}),
        FakeResponse(content=[b"new content"]),
    ])
    monkeypatch.setattr(sharepoint.requests, "get", lambda *args, **kwargs: next(responses))

    sharepoint._download_sharepoint_files(
        "token", "site-id", "input.xlsx", output_dir=str(tmp_path), max_retries=0,
    )

    assert destination.read_bytes() == b"new content"


def test_quickxor_hash_is_verified(monkeypatch, tmp_path):
    content = b"quickxor content"
    quickxor = sharepoint._QuickXorHash()
    quickxor.update(content)
    responses = iter([
        FakeResponse({"id": "drive-id"}),
        FakeResponse({
            "id": "file-id",
            "file": {"hashes": {"quickXorHash": quickxor.hexdigest()}},
        }),
        FakeResponse(content=[content]),
    ])
    monkeypatch.setattr(sharepoint.requests, "get", lambda *args, **kwargs: next(responses))

    result = sharepoint._download_sharepoint_files(
        "token", "site-id", "input.xlsx", output_dir=str(tmp_path), max_retries=0,
    )

    assert result == [tmp_path / "input.xlsx"]


def test_quickxor_empty_reference_vector():
    quickxor = sharepoint._QuickXorHash()

    assert quickxor.hexdigest() == "AAAAAAAAAAAAAAAAAAAAAAAAAAA="


def test_quickxor_mismatch_raises_download_error(monkeypatch, tmp_path):
    responses = iter([
        FakeResponse({"id": "drive-id"}),
        FakeResponse({
            "id": "file-id",
            "file": {"hashes": {"quickXorHash": "AAAAAAAAAAAAAAAAAAAAAA=="}},
        }),
        FakeResponse(content=[b"unexpected content"]),
    ])
    monkeypatch.setattr(sharepoint.requests, "get", lambda *args, **kwargs: next(responses))

    with pytest.raises(sharepoint.SharePointDownloadError, match="quickxor checksum mismatch"):
        sharepoint._download_sharepoint_files(
            "token", "site-id", "input.xlsx", output_dir=str(tmp_path), max_retries=0,
        )


def test_timeout_must_be_positive(tmp_path):
    with pytest.raises(ValueError, match="timeout"):
        sharepoint._download_sharepoint_files(
            "token", "site-id", "input.xlsx", output_dir=str(tmp_path), timeout=(0, 10),
        )


