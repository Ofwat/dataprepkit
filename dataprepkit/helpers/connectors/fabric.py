"""Fabric SQLAlchemy connector utilities."""

from __future__ import annotations

import logging
import struct
from typing import Iterable, Optional, Protocol

import pyodbc
import sqlalchemy as sa
from sqlalchemy import text

logger = logging.getLogger(__name__)


class TokenProvider(Protocol):
    """Abstract access token provider."""

    def getToken(self, resource: str) -> str:  # pragma: no cover - Fabric only
        ...


try:
    from notebookutils import credentials

    _default_provider: Iterable = (credentials,)
except ImportError:  # pragma: no cover - Fabric-only runtime
    credentials = None  # type: ignore[assignment]
    _default_provider = ()


class _MockTokenProvider:
    """Fallback token provider used when Fabric utilities are unavailable."""

    def getToken(self, resource: str) -> str:  # pragma: no cover - local testing
        logger.warning("Mock token provided for %s", resource)
        return "".join(["MOCK", resource.replace(":", "")])


def _select_driver(preferred: Optional[str] = None) -> str:
    drivers = pyodbc.drivers()
    if preferred and preferred in drivers:
        return preferred
    candidates = [driver for driver in drivers if "ODBC Driver" in driver]
    if not candidates:
        raise RuntimeError("No ODBC driver matching 'ODBC Driver' found.")
    return sorted(candidates, reverse=True)[0]


def _format_connection_string(
    endpoint: str, port: int, driver: str, database: Optional[str]
) -> str:
    parts = [f"DRIVER={{{driver}}}", f"SERVER={endpoint},{port}"]
    if database:
        parts.append(f"DATABASE={database}")
    return ";".join(parts)


def get_fabric_sql_engine(
    sql_endpoint: str,
    *,
    port: int = 1433,
    database: Optional[str] = None,
    driver_name: Optional[str] = None,
    token_provider: Optional[TokenProvider] = None,
) -> sa.engine.Engine:
    """Return a SQLAlchemy engine authenticated with Fabric SQL."""

    if not sql_endpoint:
        raise ValueError("sql_endpoint is required")

    provider = token_provider or next(iter(_default_provider), _MockTokenProvider())
    token = provider.getToken("https://database.windows.net/").encode("UTF-16-LE")
    attrs_before = struct.pack(f"<I{len(token)}s", len(token), token)

    driver = _select_driver(driver_name)
    connection_string = _format_connection_string(sql_endpoint, port, driver, database)
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
