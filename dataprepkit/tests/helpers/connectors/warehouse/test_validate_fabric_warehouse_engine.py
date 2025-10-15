from unittest.mock import MagicMock, patch
import pytest
import sqlalchemy as sa

from dataprepkit.helpers.connectors.warehouse import (
    get_fabric_warehouse_engine,
    validate_fabric_warehouse_engine,
)


@patch("pyodbc.drivers", return_value=["ODBC Driver 18 for SQL Server"])
@patch("sqlalchemy.create_engine")
def test_connection_test_passes(mock_create_engine, _mock_drivers):
    """
    Test that `validate_fabric_warehouse_engine` returns True when the test query
    executes successfully and returns the expected result (1).
    
    This test mocks the SQLAlchemy engine and connection to simulate a successful
    connection and query execution.
    """
    mock_conn = MagicMock()
    mock_conn.execute.return_value.scalar.return_value = 1

    mock_engine = MagicMock(spec=sa.engine.Engine)
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    mock_create_engine.return_value = mock_engine

    mock_credentials = MagicMock()
    mock_credentials.getToken.return_value = "mocked_token"

    engine = get_fabric_warehouse_engine(
        "myfabric.warehouse.microsoft.com",
        credentials=mock_credentials
    )
    assert validate_fabric_warehouse_engine(engine) is True


@patch("pyodbc.drivers", return_value=["ODBC Driver 18 for SQL Server"])
@patch("sqlalchemy.create_engine")
def test_connection_test_fails_unexpected_result(mock_create_engine, _mock_drivers):
    """
    Test that `validate_fabric_warehouse_engine` raises a RuntimeError when the
    test query returns an unexpected result (anything other than 1).
    
    This test mocks the SQLAlchemy engine and connection to simulate a query
    returning an unexpected scalar value.
    """
    mock_conn = MagicMock()
    mock_conn.execute.return_value.scalar.return_value = 999

    mock_engine = MagicMock(spec=sa.engine.Engine)
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    mock_create_engine.return_value = mock_engine

    mock_credentials = MagicMock()
    mock_credentials.getToken.return_value = "mocked_token"

    engine = get_fabric_warehouse_engine(
        "myfabric.warehouse.microsoft.com",
        credentials=mock_credentials
    )

    with pytest.raises(RuntimeError, match="Unexpected result from test query."):
        validate_fabric_warehouse_engine(engine)
