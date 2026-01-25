from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from dataprepkit.helpers.connectors.fabric import create_engine_for_fabric, validate
from dataprepkit.metadata_loader import register_metadata, run_dimension

def _resolve_base_dir() -> Path:
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:
        return Path.cwd()

FABRIC_ENDPOINT = "byx2sqtktgzedbish3jdpk4dcm-qybek6cxp2yulbokgc3c6aie5u.database.fabric.microsoft.com"
FABRIC_DATABASE = "mydb-8be33c12-255a-43ff-bead-2fbe027bf1ed"
FABRIC_TARGET_TABLE = "blah4"
FABRIC_METADATA_NAME = "fabric_demo_dimension"
FABRIC_FILEPATH = _resolve_base_dir() / "examples" / "dummy_dimension.csv"

def _register_metadata():
    register_metadata(
        FABRIC_METADATA_NAME,
        {
            "target_table": FABRIC_TARGET_TABLE,
            "natural_key_cols": ["natural_key"],
            "data_columns": {
                "data_column": {"type": "NVARCHAR(4000)"},
                "source_system": {"type": "NVARCHAR(4000)", "nullable": True},
            },
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": str(FABRIC_FILEPATH),
            "description": "Fabric metadata-driven SCD2 load",
        },
    )

def main():
    engine = create_engine_for_fabric(
        FABRIC_ENDPOINT,
        FABRIC_DATABASE,
        preferred_driver="ODBC Driver 18 for SQL Server",
    )
    _register_metadata()

    incoming = pd.DataFrame(
        [
            {"natural_key": "a1", "join_numeric_key": 1, "data_column": "a2", "source_system": "demo"},
            {"natural_key": "b1", "join_numeric_key": 2, "data_column": "b2", "source_system": "demo"},
        ]
    )

    run_dimension(engine, FABRIC_METADATA_NAME, override_df=incoming)

    with engine.connect() as conn:
        rows = conn.execute(text(f"SELECT COUNT(*) FROM {FABRIC_TARGET_TABLE} WHERE Current_Ind = 1")).scalar()
        print("Current row count:", rows)
        print(pd.read_sql_table(FABRIC_TARGET_TABLE, conn).head())

if __name__ == "__main__":
    main()
