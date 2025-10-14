"""
Fabric Warehouse SQLAlchemy Engine Helper

This module provides utility functions to create a SQLAlchemy engine
for connecting securely to an Azure Fabric data warehouse using
Azure Active Directory token-based authentication.

Functions:
- get_latest_sql_driver(): Detects and returns the latest installed
  Microsoft SQL Server ODBC driver available on the system.

- get_fabric_engine(sql_endpoint: str, port: int = 1433) -> sa.engine.Engine:
  Creates and returns a SQLAlchemy engine connected to the specified Fabric
  warehouse endpoint using token-based authentication, with built-in connection
  pooling and recycling to manage token expiration.

Usage:
    engine = get_fabric_engine("[your string].datawarehouse.fabric.microsoft.com")
    with engine.begin() as conn:
        result = conn.execute("SELECT TOP 10 * FROM your_table")
        for row in result:
            print(row)

Requirements:
- pyodbc
- sqlalchemy
- notebookutils (for token credentials, in Microsoft Fabric only)
- Proper ODBC driver(s) installed for SQL Server (e.g., ODBC Driver 18 for SQL Server)
"""

import pyodbc
import struct
import sqlalchemy as sa
import logging
import re

logger = logging.getLogger(__name__)

# -----------------------------
# Handle Fabric-only dependency
# -----------------------------
try:
    from notebookutils import credentials
except ImportError:
    # Mock credentials class for local testing outside Microsoft Fabric
    class _MockCredentials:
        def getToken(self, resource: str) -> str:
            logger.warning(f"Using mock credentials for resource: {resource}")
            # Return a bytes object encoded as UTF-16-LE to match Fabric's behavior
            return "FAKE_TOKEN_FOR_LOCAL_TESTING"

    credentials = _MockCredentials()


def get_latest_sql_driver() -> str:
    """
    Finds and returns the latest installed ODBC driver suitable for connecting
    to Microsoft SQL Server databases.

    Returns:
        str: The name of the latest SQL Server ODBC driver.

    Raises:
        RuntimeError: If no suitable SQL Server ODBC driver is found.
    """
    drivers = pyodbc.drivers()
    sql_drivers = [d for d in drivers if "SQL Server" in d or "ODBC Driver" in d]
    if not sql_drivers:
        raise RuntimeError("No suitable ODBC driver for SQL Server found.")

    def extract_version(name: str) -> int:
        match = re.search(r"(\d+)", name)
        return int(match.group(1)) if match else 0

    latest_driver = max(sql_drivers, key=extract_version)
    logger.info(f"Using ODBC driver: {latest_driver}")
    return latest_driver


def get_fabric_engine(sql_endpoint: str, port: int = 1433) -> sa.engine.Engine:
    """
    Creates and returns a SQLAlchemy engine connected to an Azure Fabric warehouse.

    Parameters:
        sql_endpoint (str): The Fabric SQL endpoint to connect to.
        port (int, optional): The TCP port for the SQL server. Defaults to 1433.

    Returns:
        sa.engine.Engine: A SQLAlchemy Engine instance connected to the Fabric warehouse.

    Raises:
        ValueError: If sql_endpoint is not provided.
        RuntimeError: If no suitable ODBC driver is found.
        Exception: If token retrieval or engine creation fails.
    """
    if not sql_endpoint:
        raise ValueError("sql_endpoint is required and cannot be empty.")

    try:
        driver = get_latest_sql_driver()
        server = f"{sql_endpoint},{port}"

        token = credentials.getToken("https://database.windows.net/").encode("UTF-16-LE")
        token_struct = struct.pack(f"<I{len(token)}s", len(token), token)

        connection_string = f"DRIVER={{{driver}}};SERVER={server}"
        connection_url = sa.engine.URL.create(
            "mssql+pyodbc",
            query={"odbc_connect": connection_string}
        )

        engine = sa.create_engine(
            connection_url,
            connect_args={"attrs_before": {1256: token_struct}},
            pool_pre_ping=True,
            pool_recycle=3600  # Recycle every hour to avoid token expiration issues
        )

        logger.info("Successfully created Fabric SQL engine.")
        return engine

    except Exception as e:
        logger.error(f"Failed to create Fabric engine: {e}", exc_info=True)
        raise
