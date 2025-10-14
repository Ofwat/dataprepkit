import pyodbc as _pyodbc
import struct as _struct
import sqlalchemy as _sa
import logging as _logging
import re as _re

_logger = _logging.getLogger(__name__)

# Explicitly define public API
__all__ = [
    "get_fabric_warehouse_engine",
    "validate_fabric_warehouse_engine"
]

# -----------------------------
# Handle Fabric-only dependency
# -----------------------------
try:
    from notebookutils import credentials as _default_credentials
except ImportError:
    class _MockCredentials:
        def getToken(self, resource: str) -> str:
            _logger.warning(f"Using mock credentials for resource: {resource}")
            return "FAKE_TOKEN_FOR_LOCAL_TESTING"
    _default_credentials = _MockCredentials()


def _get_latest_sql_driver() -> str:
    drivers = _pyodbc.drivers()
    sql_drivers = [d for d in drivers if "SQL Server" in d or "ODBC Driver" in d]
    if not sql_drivers:
        raise RuntimeError("No suitable ODBC driver for SQL Server found.")

    def extract_version(name: str) -> int:
        match = _re.search(r"(\d+)", name)
        return int(match.group(1)) if match else 0

    latest_driver = max(sql_drivers, key=extract_version)
    _logger.info(f"Using ODBC driver: {latest_driver}")
    return latest_driver


def get_fabric_warehouse_engine(sql_endpoint: str, port: int = 1433, credentials=_default_credentials) -> _sa.engine.Engine:
    """
    Create and return a SQLAlchemy engine connected to an Azure Fabric data warehouse.

    Args:
        sql_endpoint (str): The Fabric SQL endpoint to connect to.
        port (int, optional): The TCP port for the SQL server. Defaults to 1433.
        credentials (optional): An object with a getToken(resource) method. Defaults to Fabric's credentials.

    Returns:
        _sa.engine.Engine: A SQLAlchemy Engine instance connected to the Fabric warehouse.

    Raises:
        ValueError: If `sql_endpoint` is empty or None.
        RuntimeError: If no suitable ODBC driver is found.
        Exception: If token retrieval or engine creation fails.
    """
    if not sql_endpoint:
        raise ValueError("sql_endpoint is required and cannot be empty.")

    try:
        driver = _get_latest_sql_driver()
        server = f"{sql_endpoint},{port}"

        token = credentials.getToken("https://database.windows.net/").encode("UTF-16-LE")
        token_struct = _struct.pack(f"<I{len(token)}s", len(token), token)

        connection_string = f"DRIVER={{{driver}}};SERVER={server}"
        connection_url = _sa.engine.URL.create(
            "mssql+pyodbc",
            query={"odbc_connect": connection_string}
        )

        engine = _sa.create_engine(
            connection_url,
            connect_args={"attrs_before": {1256: token_struct}},
            pool_pre_ping=True,
            pool_recycle=3600
        )

        _logger.info("Successfully created Fabric SQL engine.")
        return engine

    except Exception as e:
        _logger.error(f"Failed to create Fabric engine: {e}", exc_info=True)
        raise


def validate_fabric_warehouse_engine(engine: _sa.engine.Engine) -> bool:
    """
    Test the connection to the Fabric warehouse engine by executing a simple query.

    Args:
        engine (_sa.engine.Engine): The SQLAlchemy engine connected to the Fabric warehouse.

    Returns:
        bool: True if the test query executes successfully and returns the expected result.

    Raises:
        Exception: If the query fails to execute or returns an unexpected result.
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(_sa.text("SELECT 1"))
            value = result.scalar()
            if value == 1:
                _logger.info("Fabric warehouse engine test passed.")
                return True
            else:
                raise RuntimeError("Unexpected result from test query.")
    except Exception as e:
        _logger.error(f"Test query failed: {e}", exc_info=True)
        raise
