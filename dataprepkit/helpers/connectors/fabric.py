import logging
import struct
import time
from typing import Optional

import pyodbc
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
try:
    from notebookutils import credentials
except ImportError:  # pragma: no cover - optional dependency for Fabric environments
    class _MissingCredentials:
        @staticmethod
        def getToken(_):
            raise RuntimeError("Notebook credentials are unavailable")

    credentials = _MissingCredentials()

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
    port: int | None = None,
    max_retries: int = 3,
    initial_backoff_seconds: float = 1.0,
    backoff_multiplier: float = 2.0,
) -> sa.engine.Engine:
    driver = _get_driver(preferred_driver) if preferred_driver else _get_driver()
    if credentials is None:
        raise RuntimeError("notebookutils.credentials is unavailable in this environment")
    token = credentials.getToken("https://database.windows.net/")
    if isinstance(token, str):
        token = token.encode("UTF-16-LE")
    attrs_before = struct.pack(f"<I{len(token)}s", len(token), token)
    host = endpoint
    if "," in endpoint:
        host, port_part = endpoint.split(",", 1)
        if port_part.isdigit():
            port = int(port_part)
    port = port or 1433
    conn_str = _build_connection_string(driver, host, database, port)
    url = sa.engine.URL.create("mssql+pyodbc", query={"odbc_connect": conn_str})
    engine = sa.create_engine(
        url,
        connect_args={"attrs_before": {1256: attrs_before}},
        pool_pre_ping=True,
        pool_recycle=3600,
    )
    _validate_engine_with_retry(
        engine,
        max_retries=max_retries,
        initial_backoff_seconds=initial_backoff_seconds,
        backoff_multiplier=backoff_multiplier,
    )
    return engine


def _validate_engine_with_retry(
    engine: sa.engine.Engine,
    *,
    max_retries: int = 3,
    initial_backoff_seconds: float = 1.0,
    backoff_multiplier: float = 2.0,
) -> None:
    if max_retries < 1:
        raise ValueError("max_retries must be at least 1.")
    if initial_backoff_seconds < 0:
        raise ValueError("initial_backoff_seconds must be >= 0.")
    if backoff_multiplier < 1:
        raise ValueError("backoff_multiplier must be >= 1.")

    backoff = initial_backoff_seconds
    last_error: SQLAlchemyError | None = None
    for attempt in range(1, max_retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1 AS value"))
            return
        except SQLAlchemyError as exc:
            last_error = exc
            if attempt == max_retries:
                raise
            LOG.warning(
                "Fabric engine validation failed on attempt %d/%d; retrying in %.2fs",
                attempt,
                max_retries,
                backoff,
            )
            if backoff > 0:
                time.sleep(backoff)
            backoff *= backoff_multiplier
    if last_error is not None:
        raise last_error


def validate(engine: sa.engine.Engine) -> bool:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1 AS value"))
        return result.scalar() == 1
