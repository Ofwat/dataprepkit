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


def _build_engine():
    engine = create_engine_for_fabric(
        FABRIC_ENDPOINT,
        FABRIC_DATABASE,
        preferred_driver="ODBC Driver 18 for SQL Server",
    )
    if not validate(engine):
        raise RuntimeError("Fabric connection test failed.")
    return engine



def _register_metadata():
    register_metadata(
        FABRIC_METADATA_NAME,
        {
            "target_table": FABRIC_TARGET_TABLE,
            "natural_key_cols": ["natural_key"],
            "natural_key_specs": {
                "natural_key": {"type": "NVARCHAR(4000)", "nullable": False},
            },
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


def _load_insert_batch() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"natural_key": "a1", "data_column": "a2", "source_system": "demo"},
            {"natural_key": "b1", "data_column": "b2", "source_system": "demo"},
            {"natural_key": "d1", "data_column": "d2", "source_system": "demo"},
        ]
    )


def _load_update_batch() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"natural_key": "a1", "data_column": "a2", "source_system": "demo"},
            {"natural_key": "b1", "data_column": "updated", "source_system": "demo"},
        ]
    )


def _load_delete_batch() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"natural_key": "a1", "data_column": "a2", "source_system": "demo"},
        ]
    )


def _load_reinsert_batch() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"natural_key": "d1", "data_column": "d2", "source_system": "demo"},
            {"natural_key": "a1", "data_column": "a2", "source_system": "demo"},
            {"natural_key": "b1", "data_column": "updated", "source_system": "demo"},
        ]
    )


def _summarize(engine, label: str) -> None:
    with engine.connect() as conn:
        df = pd.read_sql_table(FABRIC_TARGET_TABLE, conn)
    print(f"\n--- {label} ---")
    print(df.sort_values(["natural_key", "surrogate_key"]))


def _run_batch(engine, label: str, incoming: pd.DataFrame) -> None:
    run_dimension(engine, FABRIC_METADATA_NAME, override_df=incoming)
    _summarize(engine, label)


def main():
    engine = _build_engine()
    _register_metadata()

    _run_batch(engine, "Insert phase", _load_insert_batch())
    _run_batch(engine, "Update phase", _load_update_batch())
    _run_batch(engine, "Delete phase", _load_delete_batch())
    _run_batch(engine, "Reinsert phase", _load_reinsert_batch())

    with engine.connect() as conn:
        rows = conn.execute(text(f"SELECT COUNT(*) FROM {FABRIC_TARGET_TABLE} WHERE Current_Ind = 1")).scalar()
        print("Current row count:", rows)
        print(pd.read_sql_table(FABRIC_TARGET_TABLE, conn).head())


if __name__ == "__main__":
    main()
