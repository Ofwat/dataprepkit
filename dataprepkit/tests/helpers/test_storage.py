import os
from pathlib import Path

import pandas as pd
import pytest

import dataprepkit.storage as storage
from dataprepkit.storage import (
    ArchivePath,
    SQLMetadataRefresh,
    WarehouseEndpoint,
    archive_dataframe_path,
    get_current_env,
    get_warehouse_endpoint,
    refresh_sql_endpoint,
)


def test_archive_dataframe_path_creates_folder(tmp_path):
    result = archive_dataframe_path("tbl_d_region", "42", str(tmp_path))
    assert isinstance(result, ArchivePath)
    assert result.table == "tbl_d_region"
    assert "BATCH42" in result.file_path
    assert result.file_path.endswith(".parquet")
    assert os.path.exists(result.file_path) is False
    assert os.path.basename(Path(result.file_path).parent) == "tbl_d_region"


@pytest.mark.parametrize(
    ("table_name", "batch_id", "base_dir"),
    [
        ("", "1", "/tmp"),
        ("table", "", "/tmp"),
        ("table", "1", ""),
    ],
)
def test_archive_dataframe_path_validates_inputs(table_name, batch_id, base_dir):
    with pytest.raises(ValueError):
        archive_dataframe_path(table_name, batch_id, base_dir)


def test_get_warehouse_endpoint_returns_connection_info(monkeypatch):
    class _Credentials:
        @staticmethod
        def getToken(resource):
            assert resource == "https://api.fabric.microsoft.com"
            return "token"

    class _Fabric:
        @staticmethod
        def list_workspaces():
            return pd.DataFrame([{"Name": "Workspace A", "Id": "ws-1"}])

    class _Response:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    calls = []

    class _Requests:
        @staticmethod
        def get(url, headers=None):
            calls.append((url, headers))
            if url.endswith("/warehouses"):
                return _Response(
                    {
                        "value": [
                            {"id": "wh-1", "displayName": "Warehouse A"},
                        ]
                    }
                )
            if url.endswith("/warehouses/wh-1/connectionString"):
                return _Response(
                    {
                        "connectionString": "warehouse.fabric.microsoft.com",
                    }
                )
            raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr(storage, "credentials", _Credentials)
    monkeypatch.setattr(storage, "fabric", _Fabric)
    monkeypatch.setattr(storage, "requests", _Requests)

    endpoint = get_warehouse_endpoint("Workspace A", "Warehouse A")

    assert isinstance(endpoint, WarehouseEndpoint)
    assert endpoint.warehouse_name == "Warehouse A"
    assert endpoint.server_fqdn == "warehouse.fabric.microsoft.com"
    assert endpoint.resource_id == "wh-1"
    assert calls[0][0] == "https://api.fabric.microsoft.com/v1/workspaces/ws-1/warehouses"
    assert (
        calls[1][0]
        == "https://api.fabric.microsoft.com/v1/workspaces/ws-1/warehouses/wh-1/connectionString"
    )


def test_get_warehouse_endpoint_raises_when_warehouse_missing(monkeypatch):
    class _Credentials:
        @staticmethod
        def getToken(resource):
            return "token"

    class _Fabric:
        @staticmethod
        def list_workspaces():
            return pd.DataFrame([{"Name": "Workspace A", "Id": "ws-1"}])

    class _Response:
        @staticmethod
        def raise_for_status():
            return None

        @staticmethod
        def json():
            return {"value": []}

    class _Requests:
        @staticmethod
        def get(url, headers=None):
            return _Response()

    monkeypatch.setattr(storage, "credentials", _Credentials)
    monkeypatch.setattr(storage, "fabric", _Fabric)
    monkeypatch.setattr(storage, "requests", _Requests)

    with pytest.raises(
        ValueError,
        match="Warehouse 'Warehouse A' not found in workspace 'Workspace A'",
    ):
        get_warehouse_endpoint("Workspace A", "Warehouse A")


def test_refresh_sql_endpoint_posts_refresh_request(monkeypatch):
    class _Credentials:
        @staticmethod
        def getToken(resource):
            assert resource == "https://api.fabric.microsoft.com"
            return "token"

    class _Fabric:
        @staticmethod
        def list_workspaces():
            return pd.DataFrame([{"Name": "Workspace A", "Id": "ws-1"}])

    class _Response:
        def __init__(self, payload=None, status_code=200, headers=None):
            self._payload = payload or {}
            self.status_code = status_code
            self.headers = headers or {}
            self.content = b"{}" if payload is not None else b""

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    calls = []

    class _Requests:
        @staticmethod
        def get(url, headers=None):
            calls.append(("get", url, headers, None))
            return _Response(
                {
                    "value": [
                        {"id": "sql-1", "displayName": "Database A"},
                    ]
                }
            )

        @staticmethod
        def post(url, headers=None, json=None):
            calls.append(("post", url, headers, json))
            return _Response(
                status_code=202,
                headers={
                    "x-ms-operation-id": "op-1",
                    "Location": (
                        "https://api.fabric.microsoft.com/v1/operations/op-1"
                    ),
                },
            )

    monkeypatch.setattr(storage, "credentials", _Credentials)
    monkeypatch.setattr(storage, "fabric", _Fabric)
    monkeypatch.setattr(storage, "requests", _Requests)

    result = refresh_sql_endpoint(
        "Workspace A",
        "Database A",
        recreate_tables=True,
        wait_for_completion=False,
    )

    assert isinstance(result, SQLMetadataRefresh)
    assert result.sql_endpoint_id == "sql-1"
    assert result.operation_id == "op-1"
    assert result.status_code == 202
    assert (
        calls[0][1]
        == "https://api.fabric.microsoft.com/v1/workspaces/ws-1/sqlEndpoints"
    )
    assert (
        calls[1][1]
        == (
            "https://api.fabric.microsoft.com/v1/workspaces/ws-1/"
            "sqlEndpoints/sql-1/refreshMetadata"
        )
    )
    assert calls[1][3] == {
        "recreateTables": True,
        "timeout": {"value": 15, "timeUnit": "Minutes"},
    }


def test_refresh_sql_endpoint_waits_for_completion(monkeypatch):
    class _Credentials:
        @staticmethod
        def getToken(resource):
            return "token"

    class _Fabric:
        @staticmethod
        def list_workspaces():
            return pd.DataFrame([{"Name": "Workspace A", "Id": "ws-1"}])

    class _Response:
        def __init__(self, payload=None, status_code=200, headers=None):
            self._payload = payload or {}
            self.status_code = status_code
            self.headers = headers or {}
            self.content = b"{}" if payload is not None else b""

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    calls = []
    operation_states = [
        {"status": "Running", "percentComplete": 25},
        {"status": "Succeeded", "percentComplete": 100},
    ]

    class _Requests:
        @staticmethod
        def get(url, headers=None):
            calls.append(("get", url))
            if url.endswith("/sqlEndpoints"):
                return _Response(
                    {
                        "value": [
                            {"id": "sql-1", "displayName": "Database A"},
                        ]
                    }
                )
            return _Response(
                operation_states.pop(0),
                headers={"Location": f"{url}/result", "Retry-After": "3"},
            )

        @staticmethod
        def post(url, headers=None, json=None):
            calls.append(("post", url))
            return _Response(
                status_code=202,
                headers={"x-ms-operation-id": "op-1", "Retry-After": "2"},
            )

    sleeps = []
    monkeypatch.setattr(storage, "credentials", _Credentials)
    monkeypatch.setattr(storage, "fabric", _Fabric)
    monkeypatch.setattr(storage, "requests", _Requests)
    monkeypatch.setattr(storage.time, "sleep", lambda seconds: sleeps.append(seconds))

    result = refresh_sql_endpoint(
        "Workspace A",
        "Database A",
        wait_for_completion=True,
    )

    assert result.operation_id == "op-1"
    assert result.location == (
        "https://api.fabric.microsoft.com/v1/operations/op-1/result"
    )
    assert result.data == {"status": "Succeeded", "percentComplete": 100}
    assert sleeps == [2.0, 3.0]
    assert calls[-1] == (
        "get",
        "https://api.fabric.microsoft.com/v1/operations/op-1",
    )


def test_refresh_sql_endpoint_raises_when_endpoint_missing(monkeypatch):
    class _Credentials:
        @staticmethod
        def getToken(resource):
            return "token"

    class _Fabric:
        @staticmethod
        def list_workspaces():
            return pd.DataFrame([{"Name": "Workspace A", "Id": "ws-1"}])

    class _Response:
        headers = {}
        status_code = 200
        content = b"{}"

        @staticmethod
        def raise_for_status():
            return None

        @staticmethod
        def json():
            return {"value": []}

    class _Requests:
        @staticmethod
        def get(url, headers=None):
            return _Response()

    monkeypatch.setattr(storage, "credentials", _Credentials)
    monkeypatch.setattr(storage, "fabric", _Fabric)
    monkeypatch.setattr(storage, "requests", _Requests)

    with pytest.raises(
        ValueError,
        match="SQL endpoint 'Database A' not found in workspace 'Workspace A'",
    ):
        refresh_sql_endpoint("Workspace A", "Database A")


def test_get_current_env_returns_matching_prefix(monkeypatch):
    class _Fabric:
        @staticmethod
        def resolve_workspace_name():
            return "preprod-sales"

    monkeypatch.setattr(storage, "fabric", _Fabric)

    assert get_current_env() == "preprod"


def test_get_current_env_raises_for_missing_workspace_name(monkeypatch):
    class _Fabric:
        @staticmethod
        def resolve_workspace_name():
            return ""

    monkeypatch.setattr(storage, "fabric", _Fabric)

    with pytest.raises(ValueError, match="workspace_name is None or empty"):
        get_current_env()


def test_get_current_env_raises_when_no_environment_matches(monkeypatch):
    class _Fabric:
        @staticmethod
        def resolve_workspace_name():
            return "sandbox-sales"

    monkeypatch.setattr(storage, "fabric", _Fabric)

    with pytest.raises(RuntimeError, match="Expected one of: dev, prod, preprod"):
        get_current_env()
