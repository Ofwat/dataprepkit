"""Minimal lakehouse mount helper for Fabric."""

from __future__ import annotations

import os
import time
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass
try:
    import requests
except ImportError:  # pragma: no cover - optional dependency
    requests = None  # type: ignore[assignment]

try:
    import sempy.fabric as fabric
except ImportError:  # pragma: no cover - Fabric only
    fabric = None  # type: ignore[assignment]

try:
    from notebookutils import credentials, fs
except ImportError:  # pragma: no cover - Fabric only
    credentials = None  # type: ignore[assignment]
    fs = None  # type: ignore[assignment]


@dataclass(frozen=True)
class LakehouseMount:
    workspace_id: str
    lakehouse_id: str
    lakehouse_base_path: str
    source_data_path: str


class StorageMountError(RuntimeError):
    """Raised when the lakehouse cannot be mounted."""


def _resolve_ids(workspace_name: str, display_name: str) -> tuple[str, str]:
    if fabric is None:
        raise ImportError("sempy.fabric is required to resolve lakehouse metadata.")

    workspaces_df = fabric.list_workspaces()
    workspace_id = (
        workspaces_df[workspaces_df["Name"] == workspace_name]["Id"].to_list()[0]
    )
    items_df = fabric.list_items(workspace=workspace_id)
    lakehouse_id = (
        items_df[
            (items_df["Type"] == "Lakehouse")
            & (items_df["Display Name"] == display_name)
        ]["Id"]
        .to_list()[0]
    )
    return workspace_id, lakehouse_id


def _format_base_path(workspace_id: str, lakehouse_id: str) -> str:
    return f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}"


def mount_lakehouse(
    workspace_name: str,
    lakehouse_display_name: str,
    mount_point: str | None = None,
    max_retries: int = 20,
    retry_delay: float = 1.0,
) -> LakehouseMount:
    """Mount the Fabric lakehouse and return the mount metadata."""
    if fs is None:
        raise ImportError("notebookutils.fs is required to mount the lakehouse.")

    workspace_id, lakehouse_id = _resolve_ids(workspace_name, lakehouse_display_name)
    lakehouse_base_path = _format_base_path(workspace_id, lakehouse_id)
    mount_point = mount_point or str(Path.home() / "mounts" / lakehouse_display_name.replace(" ", "_"))
    os.makedirs(os.path.dirname(mount_point), exist_ok=True)

    last_error: Exception | None = None
    for _ in range(max_retries):
        try:
            fs.unmount(mount_point)
        except Exception:
            pass

        try:
            fs.mount(lakehouse_base_path, mount_point)
            source_data_path = fs.getMountPath(mount_point)
            return LakehouseMount(
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id,
                lakehouse_base_path=lakehouse_base_path,
                source_data_path=source_data_path,
            )
        except Exception as error:  # pragma: no cover
            last_error = error
            time.sleep(retry_delay)

    raise StorageMountError(
        f"Failed to mount after {max_retries} attempts: {last_error}"
    )


@dataclass(frozen=True)
class SQLDatabaseEndpoint:
    database_name: str | None
    server_fqdn: str | None
    resource_id: str | None


@dataclass(frozen=True)
class SQLMetadataRefresh:
    sql_endpoint_id: str
    operation_id: str | None
    location: str | None
    status_code: int
    data: dict


def _get_header(headers: dict, name: str) -> str | None:
    return headers.get(name) or headers.get(name.lower())


def _response_json(response) -> dict:
    return response.json() if response.content else {}


def _retry_after_seconds(headers: dict, default: float) -> float:
    retry_after = _get_header(headers, "Retry-After")
    if retry_after is None:
        return default
    try:
        return float(retry_after)
    except ValueError:
        return default


def get_sql_db_endpoint(workspace_name: str, sql_db_display_name: str) -> SQLDatabaseEndpoint:
    if credentials is None:
        raise ImportError("notebookutils.credentials is required for Fabric SQL metadata.")
    token = credentials.getToken("https://api.fabric.microsoft.com")

    if fabric is None:
        raise ImportError("sempy.fabric is required to list workspaces.")

    if requests is None:
        raise ImportError("requests is required to call the Fabric SQL metadata API.")

    ws_df = fabric.list_workspaces()
    matches = ws_df[ws_df["Name"] == workspace_name]

    if matches.empty:
        raise ValueError(f"Workspace '{workspace_name}' not found")

    workspace_id = matches["Id"].iloc[0]

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/sqlDatabases"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    for db in data.get("value", []):
        if db.get("displayName") == sql_db_display_name:
            props = db.get("properties", {})
            return SQLDatabaseEndpoint(
                database_name=props.get("databaseName"),
                server_fqdn=props.get("serverFqdn"),
                resource_id=db.get("id"),
            )

    raise ValueError(
        f"SQL database '{sql_db_display_name}' not found in workspace '{workspace_name}'"
    )


def refresh_sql_endpoint(
    workspace_name: str,
    sql_endpoint_display_name: str,
    recreate_tables: bool = False,
    timeout_minutes: int = 15,
    wait_for_completion: bool = True,
    poll_interval_seconds: float = 5.0,
    max_wait_seconds: float | None = None,
) -> SQLMetadataRefresh:
    if credentials is None:
        raise ImportError("notebookutils.credentials is required for Fabric SQL metadata.")
    token = credentials.getToken("https://api.fabric.microsoft.com")

    if fabric is None:
        raise ImportError("sempy.fabric is required to list workspaces.")

    if requests is None:
        raise ImportError("requests is required to call the Fabric SQL metadata API.")

    ws_df = fabric.list_workspaces()
    matches = ws_df[ws_df["Name"] == workspace_name]

    if matches.empty:
        raise ValueError(f"Workspace '{workspace_name}' not found")

    workspace_id = matches["Id"].iloc[0]
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    endpoints_url = (
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/sqlEndpoints"
    )
    endpoints_response = requests.get(endpoints_url, headers=headers)
    endpoints_response.raise_for_status()

    for endpoint in endpoints_response.json().get("value", []):
        if endpoint.get("displayName") != sql_endpoint_display_name:
            continue

        sql_endpoint_id = endpoint.get("id")
        refresh_url = (
            "https://api.fabric.microsoft.com/v1/workspaces/"
            f"{workspace_id}/sqlEndpoints/{sql_endpoint_id}/refreshMetadata"
        )
        payload = {
            "recreateTables": recreate_tables,
            "timeout": {"value": timeout_minutes, "timeUnit": "Minutes"},
        }
        refresh_response = requests.post(refresh_url, headers=headers, json=payload)
        refresh_response.raise_for_status()
        operation_id = _get_header(refresh_response.headers, "x-ms-operation-id")
        location = _get_header(refresh_response.headers, "Location")
        data = _response_json(refresh_response)

        if wait_for_completion and refresh_response.status_code == 202:
            if operation_id is None:
                raise RuntimeError(
                    "Fabric refresh returned 202 without an operation ID."
                )

            operation_url = (
                f"https://api.fabric.microsoft.com/v1/operations/{operation_id}"
            )
            elapsed_seconds = 0.0
            sleep_seconds = _retry_after_seconds(
                refresh_response.headers,
                poll_interval_seconds,
            )

            while True:
                if max_wait_seconds is not None and elapsed_seconds >= max_wait_seconds:
                    raise TimeoutError(
                        "Timed out waiting for SQL endpoint metadata refresh "
                        f"operation '{operation_id}'."
                    )
                time.sleep(sleep_seconds)
                elapsed_seconds += sleep_seconds

                status_response = requests.get(operation_url, headers=headers)
                status_response.raise_for_status()
                data = _response_json(status_response)
                location = _get_header(status_response.headers, "Location") or location

                if data.get("status") in {"Succeeded", "Failed"}:
                    break

                sleep_seconds = _retry_after_seconds(
                    status_response.headers,
                    poll_interval_seconds,
                )

        return SQLMetadataRefresh(
            sql_endpoint_id=sql_endpoint_id,
            operation_id=operation_id,
            location=location,
            status_code=refresh_response.status_code,
            data=data,
        )

    raise ValueError(
        "SQL endpoint "
        f"'{sql_endpoint_display_name}' not found in workspace '{workspace_name}'"
    )


@dataclass(frozen=True)
class WarehouseEndpoint:
    warehouse_name: str | None
    server_fqdn: str | None
    resource_id: str | None


def get_current_env(envs: list[str] | None = None) -> str:
    if envs is None:
        envs = ["dev", "prod", "preprod"]

    if fabric is None:
        raise ImportError("sempy.fabric is required to resolve the current environment.")

    workspace_name = fabric.resolve_workspace_name()
    if not workspace_name:
        raise ValueError("Resolved workspace_name is None or empty.")

    for env in envs:
        if workspace_name.startswith(env):
            return env

    raise RuntimeError(
        f"No matching environment found for workspace '{workspace_name}'. "
        f"Expected one of: {', '.join(envs)}"
    )


def get_warehouse_endpoint(
    workspace_name: str,
    warehouse_display_name: str,
) -> WarehouseEndpoint:
    if credentials is None:
        raise ImportError("notebookutils.credentials is required for Fabric warehouse metadata.")
    token = credentials.getToken("https://api.fabric.microsoft.com")

    if fabric is None:
        raise ImportError("sempy.fabric is required to list workspaces.")

    if requests is None:
        raise ImportError("requests is required to call the Fabric warehouse metadata API.")

    ws_df = fabric.list_workspaces()
    matches = ws_df[ws_df["Name"] == workspace_name]

    if matches.empty:
        raise ValueError(f"Workspace '{workspace_name}' not found")

    workspace_id = matches["Id"].iloc[0]
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    warehouse_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/warehouses"
    warehouse_response = requests.get(warehouse_url, headers=headers)
    warehouse_response.raise_for_status()
    warehouses = warehouse_response.json()

    for warehouse in warehouses.get("value", []):
        if warehouse.get("displayName") != warehouse_display_name:
            continue
        warehouse_id = warehouse.get("id")
        connection_url = (
            "https://api.fabric.microsoft.com/v1/workspaces/"
            f"{workspace_id}/warehouses/{warehouse_id}/connectionString"
        )
        connection_response = requests.get(connection_url, headers=headers)
        connection_response.raise_for_status()
        connection_data = connection_response.json()
        return WarehouseEndpoint(
            warehouse_name=warehouse.get("displayName"),
            server_fqdn=connection_data.get("connectionString"),
            resource_id=warehouse_id,
        )

    raise ValueError(
        f"Warehouse '{warehouse_display_name}' not found in workspace '{workspace_name}'"
    )


@dataclass(frozen=True)
class ArchivePath:
    table: str
    batch_id: str
    timestamp: str
    file_path: str


def archive_dataframe_path(table_name: str, batch_id: str, base_dir: str) -> ArchivePath:
    """Build the archive path metadata and filename for a DataFrame."""
    if not table_name:
        raise ValueError("table_name must be provided")
    if not batch_id:
        raise ValueError("batch_id must be provided")
    if not base_dir:
        raise ValueError("base_dir must be provided")

    table_dir = os.path.join(base_dir, table_name)
    os.makedirs(table_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S%f")[:-3]
    filename = f"{table_name}__{timestamp}__BATCH{batch_id}.parquet"
    file_path = os.path.join(table_dir, filename)

    return ArchivePath(
        table=table_name,
        batch_id=batch_id,
        timestamp=timestamp,
        file_path=file_path,
    )
