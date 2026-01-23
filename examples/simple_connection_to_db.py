import struct
import logging

import pyodbc
import sqlalchemy as sa
from notebookutils import credentials
from sqlalchemy import text

LOG = logging.getLogger(__name__)


def _get_driver(preferred="ODBC Driver 18 for SQL Server") -> str:
    drivers = [d for d in pyodbc.drivers() if "ODBC Driver" in d]
    if not drivers:
        raise RuntimeError("No ODBC driver available.")
    if preferred in drivers:
        return preferred
    return sorted(drivers, reverse=True)[0]


def _build_connection_string(driver, endpoint, database, port=1433, encrypt=True, trust_certificate=False):
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


def create_engine_for_fabric(endpoint, database, preferred_driver=None, port=1433):
    driver = _get_driver(preferred_driver) if preferred_driver else _get_driver()
    token = credentials.getToken("https://database.windows.net/").encode("UTF-16-LE")
    attrs_before = struct.pack(f"<I{len(token)}s", len(token), token)
    conn_str = _build_connection_string(driver, endpoint, database, port)
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


def main():
    endpoint = "byx2sqtktgzedbish3jdpk4dcm-qybek6cxp2yulbokgc3c6aie5u.database.fabric.microsoft.com"
    database = "mydb-8be33c12-255a-43ff-bead-2fbe027bf1ed"
    engine = create_engine_for_fabric(endpoint, database)
    if not validate(engine):
        raise RuntimeError("Fabric connection test failed.")
    print("Connection validated, ready to run SCD2 logic.")

main()