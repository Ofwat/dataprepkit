"""Fabric SQLAlchemy connector utilities."""

from __future__ import annotations

import logging
import struct
from typing import Callable, Iterable, Optional

import pyodbc
import sqlalchemy as sa
from sqlalchemy import text

logger = logging.getLogger(__name__)

try:
    from notebookutils import credentials

    _default_token_provider: Iterable = (credentials,)
    _token_provider_name = "notebookutils.credentials"
except ImportError:  # pragma: no cover - Fabric-only runtime
    credentials = None  # type: ignore[assignment]
    _default_token_provider = ()
    _token_provider_name = "local-mock"


def _get_driver(preferred: Optional[str] = "ODBC Driver 18 for SQL Server") -> str:
    drivers = [driver for driver in pyodbc.drivers() if "ODBC Driver" in driver]
    if not drivers:
        raise RuntimeError("No ODBC driver available.")
    if preferred and preferred in drivers:
        return preferred
    return sorted(drivers, reverse=True)[0]


def _build_connection_string(
    driver: str,
    endpoint: str,
    database: Optional[str],
    port: int,
    encrypt: bool,
    trust_certificate: bool,
) -> str:
    parts = [
        f"Driver={{{driver}}}",
        f"Server={endpoint},{port}",
    ]
    if database:
        parts.append(f"Database={database}")
    if encrypt:
        parts.append("Encrypt=yes")
    parts.append(f"TrustServerCertificate={'yes' if trust_certificate else 'no'}")
    return ";".join(parts)


def _mock_token(resource: str) -> str:
    logger.warning("Mock token provided for %s", resource)
    return "".join(["MOCK", resource.replace(":", "")])


def _get_token(provider: Optional[Callable[[str], str]]) -> bytes:
    if provider:
        token_value = provider("https://database.windows.net/")
    else:
        try:
            token_provider = next(iter(_default_token_provider))
        except StopIteration:
            token_value = _mock_token("https://database.windows.net/")
        else:
            token_value = token_provider.getToken("https://database.windows.net/")
    logger.debug("Token retrieved via %s", _token_provider_name)
    return token_value.encode("UTF-16-LE")


def get_fabric_sql_engine(
    sql_endpoint: str,
    *,
    database: Optional[str] = None,
    preferred_driver: Optional[str] = None,
    port: int = 1433,
    encrypt: bool = True,
    trust_certificate: bool = False,
    token_provider: Optional[Callable[[str], str]] = None,
) -> sa.engine.Engine:
    """Return a SQLAlchemy engine authenticated with Fabric SQL."""

    if not sql_endpoint:
        raise ValueError("sql_endpoint is required")

    driver = _get_driver(preferred_driver)
    token = _get_token(token_provider)
    attrs_before = struct.pack(f"<I{len(token)}s", len(token), token)
    connection_string = _build_connection_string(driver, sql_endpoint, database, port, encrypt, trust_certificate)
    url = sa.engine.URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})

    return sa.create_engine(
        url,
        connect_args={"attrs_before": {1256: attrs_before}},
        pool_pre_ping=True,
        pool_recycle=3600,
    )


def validate_fabric_sql_engine(engine: sa.engine.Engine) -> bool:
    """Verify that the provided engine can actually run a trivial query."""

    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).scalar()
    return result == 1


def validate_fabric_warehouse_engine(engine: sa.engine.Engine) -> bool:
    """Historic alias kept for compatibility with legacy scripts."""

    return validate_fabric_sql_engine(engine)


def validate_fabric_warehouse_engine(engine: sa.engine.Engine) -> bool:
    """Historic alias kept for compatibility with legacy scripts."""
    return validate_fabric_sql_engine(engine)
