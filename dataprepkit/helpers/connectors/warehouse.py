"""Fabric SQLAlchemy connector utilities."""

from __future__ import annotations

import logging
import struct
from typing import Optional

import pyodbc
import sqlalchemy as sa

logger = logging.getLogger(__name__)

try:
    from notebookutils import credentials
except ImportError:  # pragma: no cover - Fabric only
    credentials = None  # type: ignore[assignment]


class MockCredentials:
    def getToken(self, resource: str) -> str:
        logger.warning("Using mock credentials for resource: %s", resource)
        return "FAKE_TOKEN"


def _select_driver() -> str:
    drivers = pyodbc.drivers()
    candidates = [d for d in drivers if "ODBC Driver" in d]
    if not candidates:
        raise RuntimeError("No suitable ODBC driver found.")
    return max(candidates)


def get_fabric_warehouse_engine(
    sql_endpoint: str,
    port: int = 1433,
    creds: Optional[object] = None,
) -> sa.engine.Engine:
    if not sql_endpoint:
        raise ValueError("sql_endpoint is required")
    provider = creds or credentials or MockCredentials()
    token = provider.getToken("https://database.windows.net/").encode("UTF-16-LE")
    attrs_before = struct.pack(f"<I{len(token)}s", len(token), token)

    driver = _select_driver()
    connection_string = f"DRIVER={{{driver}}};SERVER={sql_endpoint},{port};"
    connection_url = sa.engine.URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})

    return sa.create_engine(
        connection_url,
        connect_args={"attrs_before": {1256: attrs_before}},
        pool_pre_ping=True,
        pool_recycle=3600,
    )
