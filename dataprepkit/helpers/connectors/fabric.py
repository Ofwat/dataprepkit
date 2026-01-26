import logging
import struct
from typing import Optional

import pyodbc
import sqlalchemy as sa
from notebookutils import credentials
from sqlalchemy import text

LOG = logging.getLogger(__name__)


def _get_driver(preferred: Optional[str] = "ODBC Driver 18 for SQL Server") -> str:
    drivers = [driver for driver in pyodbc.drivers() if "ODBC Driver" in driver]
    if not drivers:
        raise RuntimeError("No ODBC driver available.")
    if preferred and preferred in drivers:
        return preferred
    return sorted(drivers, reverse=True)[0]


def _build_connection_string(
    driver: str,
    host: str,
    database: str,
    port: int,
    encrypt: bool = True,
    trust_certificate: bool = False,
) -> str:
    parts = [
        f"Driver={{{driver}}}",
        f"Server={host},{port}",
    ]
    if database:
        parts.append(f"Database={database}")
    if encrypt:
        parts.append("Encrypt=yes")
    parts.append(f"TrustServerCertificate={'yes' if trust_certificate else 'no'}")
    return ";".join(parts)


def create_engine_for_fabric(
    endpoint: str,
    database: str,
    preferred_driver: Optional[str] = None,
) -> sa.engine.Engine:
    driver = _get_driver(preferred_driver) if preferred_driver else _get_driver()
    token = credentials.getToken("https://database.windows.net/").encode("UTF-16-LE")
    attrs_before = struct.pack(f"<I{len(token)}s", len(token), token)
    if ":" in endpoint:
        host, parsed_port = endpoint.split(":", 1)
        port = int(parsed_port)
    else:
        host = endpoint
        port = 1433
    conn_str = _build_connection_string(driver, host, database, port)
    url = sa.engine.URL.create("mssql+pyodbc", query={"odbc_connect": conn_str})
    return sa.create_engine(
        url,
        connect_args={"attrs_before": {1256: attrs_before}},
        pool_pre_ping=True,
        pool_recycle=3600,
    )


def validate(engine: sa.engine.Engine) -> bool:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1 AS value"))
        return result.scalar() == 1
