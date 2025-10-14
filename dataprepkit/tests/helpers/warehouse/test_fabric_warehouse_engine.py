import pytest
from unittest.mock import MagicMock, patch
import struct
import sqlalchemy as sa

from dataprepkit.helpers.connectors.warehouse import get_fabric_warehouse_engine


@pytest.mark.parametrize("endpoint, port", [
    ("myfabric.warehouse.microsoft.com", 1433),
    ("myfabric.warehouse.microsoft.com", 1444),
])
@patch("pyodbc.drivers", return_value=[
    "ODBC Driver 13 for SQL Server",
    "ODBC Driver 18 for SQL Server",
])
@patch("sqlalchemy.create_engine")
def test_get_fabric_warehouse_engine_success(mock_create_engine, mock_drivers, endpoint, port):
    token = "mocked_token".encode("UTF-16-LE")
    token_struct = struct.pack(f"<I{len(token)}s", len(token), token)

    mock_engine = MagicMock(spec=sa.engine.Engine)
    mock_create_engine.return_value = mock_engine

    mock_credentials = MagicMock()
    mock_credentials.getToken.return_value = "mocked_token"

    engine = get_fabric_warehouse_engine(endpoint, port, credentials=mock_credentials)

    assert engine == mock_engine
    mock_credentials.getToken.assert_called_once_with('https://database.windows.net/')
    connect_args = mock_create_engine.call_args[1]["connect_args"]
    assert 1256 in connect_args["attrs_before"]
    assert connect_args["attrs_before"][1256] == token_struct
    assert mock_create_engine.call_args[1]["pool_recycle"] == 3600
    assert mock_create_engine.call_args[1]["pool_pre_ping"] is True


def test_get_fabric_warehouse_engine_empty_endpoint_raises():
    with pytest.raises(ValueError, match="sql_endpoint is required and cannot be empty."):
        get_fabric_warehouse_engine("", credentials=MagicMock())


@patch("pyodbc.drivers", return_value=[])
def test_get_fabric_warehouse_engine_no_driver(mock_drivers):
    mock_credentials = MagicMock()
    with pytest.raises(RuntimeError, match="No suitable ODBC driver for SQL Server found."):
        get_fabric_warehouse_engine("some.endpoint", credentials=mock_credentials)


@patch("pyodbc.drivers", return_value=["ODBC Driver 18 for SQL Server"])
def test_get_fabric_warehouse_engine_token_failure(mock_drivers):
    mock_credentials = MagicMock()
    mock_credentials.getToken.side_effect = Exception("Token error")
    with pytest.raises(Exception, match="Token error"):
        get_fabric_warehouse_engine("some.endpoint", credentials=mock_credentials)
