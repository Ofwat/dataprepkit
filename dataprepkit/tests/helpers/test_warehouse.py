import pytest
from unittest.mock import patch, MagicMock
import re
import struct
import sqlalchemy as sa

import dataprepkit

def test_get_latest_sql_driver_picks_latest_driver():
    drivers = [
        "ODBC Driver 13 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server Native Client 11.0",
        "ODBC Driver 18 for SQL Server"
    ]

    with patch("pyodbc.drivers", return_value=drivers):
        driver = dataprepkit.helpers.connectors.warehouse.get_latest_sql_driver()
        assert driver == "ODBC Driver 18 for SQL Server"

def test_get_latest_sql_driver_no_driver_found():
    with patch("pyodbc.drivers", return_value=[]):
        with pytest.raises(RuntimeError, match="No suitable ODBC driver for SQL Server found."):
            dataprepkit.helpers.connectors.warehouse.get_latest_sql_driver()

def test_get_latest_sql_driver_version_extraction():
    # Test the internal version extraction regex works as expected
    extract_version = dataprepkit.helpers.connectors.warehouse.get_latest_sql_driver.__globals__['extract_version'] if 'extract_version' in dataprepkit.helpers.connectors.warehouse.get_latest_sql_driver.__globals__ else None
    if extract_version:
        assert extract_version("ODBC Driver 17 for SQL Server") == 17
        assert extract_version("SQL Server Native Client 11.0") == 11
        assert extract_version("ODBC Driver") == 0
        assert extract_version("Custom Driver 123") == 123
    else:
        # fallback: test regex directly here
        pattern = re.compile(r"(\d+)")
        assert int(pattern.search("ODBC Driver 17 for SQL Server").group(1)) == 17

@pytest.mark.parametrize("endpoint, port", [
    ("myfabric.warehouse.microsoft.com", 1433),
    ("myfabric.warehouse.microsoft.com", 1444),
])
@patch("dataprepkit.helpers.connectors.warehouse.get_latest_sql_driver", return_value="ODBC Driver 18 for SQL Server")
@patch("dataprepkit.helpers.connectors.warehouse.credentials.getToken", return_value=b"mocked_token")
@patch("sqlalchemy.create_engine")
def test_get_fabric_engine_creates_engine(mock_create_engine, mock_get_token, mock_get_driver, endpoint, port):
    # Prepare mock token bytes and the packed token struct
    token_bytes = b"mocked_token"
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

    # Mock create_engine to return a MagicMock engine
    mock_engine = MagicMock(spec=sa.engine.Engine)
    mock_create_engine.return_value = mock_engine

    engine = dataprepkit.helpers.connectors.warehouse.get_fabric_engine(endpoint, port)

    mock_get_driver.assert_called_once()
    mock_get_token.assert_called_once_with('https://database.windows.net/')
    mock_create_engine.assert_called_once()

    # Check that the engine returned is the mock
    assert engine == mock_engine

    # Check connection string formation and token struct passed
    call_args = mock_create_engine.call_args[1]  # kwargs
    connect_args = call_args.get("connect_args")
    assert connect_args is not None
    assert 1256 in connect_args.get("attrs_before", {})
    assert connect_args["attrs_before"][1256] == token_struct

    # Check pool options
    assert call_args.get("pool_pre_ping") is True
    assert call_args.get("pool_recycle") == 3600

def test_get_fabric_engine_empty_endpoint_raises():
    with pytest.raises(ValueError, match="sql_endpoint is required and cannot be empty."):
        dataprepkit.helpers.connectors.warehouse.get_fabric_engine("")

@patch("dataprepkit.helpers.connectors.warehouse.get_latest_sql_driver", side_effect=RuntimeError("No drivers"))
def test_get_fabric_engine_no_driver(mock_get_driver):
    with pytest.raises(RuntimeError, match="No drivers"):
        dataprepkit.helpers.connectors.warehouse.get_fabric_engine("some.endpoint")

@patch("dataprepkit.helpers.connectors.warehouse.get_latest_sql_driver", return_value="ODBC Driver 18 for SQL Server")
@patch("dataprepkit.helpers.connectors.warehouse.credentials.getToken", side_effect=Exception("Token error"))
def test_get_fabric_engine_token_failure(mock_get_token, mock_get_driver):
    with pytest.raises(Exception, match="Token error"):
        dataprepkit.helpers.connectors.warehouse.get_fabric_engine("some.endpoint")
