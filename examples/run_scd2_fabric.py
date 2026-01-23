"""Fabric SQL example that reuses the dataprepkit metadata driven SCD2 API."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text

from dataprepkit.helpers.connectors.fabric import (
    get_fabric_sql_engine,
    validate_fabric_sql_engine,
)
from dataprepkit.metadata_loader import register_metadata, run_dimension
from dataprepkit.storage import LakehouseMount, mount_lakehouse


FABRIC_ENDPOINT = "myfabric.warehouse.microsoft.com"
FABRIC_PORT = 1433
FABRIC_DATABASE = "mydb-8be33c12-255a-43ff-bead-2fbe027bf1ed"
FABRIC_TARGET_TABLE = "[dbo].[dimension]"
FABRIC_METADATA_NAME = "fabric_dimension"
FABRIC_WORKSPACE = "Ocean_Data_PROD"
FABRIC_LAKEHOUSE = "Dimension_Source_Data"
FABRIC_MOUNT_POINT = "/home/trusted-service-user/mounts/Source_Data"
DEFAULT_RAW_FILE = Path(__file__).resolve().parents[2] / "examples" / "dummy_dimension.csv"


def _load_insert_batch() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"natural_key": "a1", "data_column": "a2", "join_numeric_key": 1},
            {"natural_key": "b1", "data_column": "b2", "join_numeric_key": 2},
            {"natural_key": "d1", "data_column": "d2", "join_numeric_key": 4},
        ]
    )


def _load_update_batch() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"natural_key": "a1", "data_column": "a2"},
            {"natural_key": "b1", "data_column": "updated"},
        ]
    )


def _load_delete_batch() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"natural_key": "a1", "data_column": "a2"},
        ]
    )


def _load_reinsert_batch() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"natural_key": "d1", "data_column": "d2"},
            {"natural_key": "a1", "data_column": "a2"},
            {"natural_key": "b1", "data_column": "updated"},
        ]
    )


def _ensure_table(engine, table_name: str) -> None:
    create_sql = f"""
    IF OBJECT_ID('{table_name}', 'U') IS NULL
    BEGIN
        CREATE TABLE {table_name} (
            surrogate_key BIGINT IDENTITY(1,1) PRIMARY KEY,
            natural_key VARCHAR(255) NOT NULL,
            join_numeric_key BIGINT NOT NULL,
            data_column VARCHAR(4000),
            row_hash VARCHAR(64) NOT NULL,
            Insert_Date DATETIME2(3) NOT NULL,
            Update_Date DATETIME2(3),
            Current_Ind BIT NOT NULL,
            Deleted_Ind BIT NOT NULL
        );
    END
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))


def _summarize(engine, label: str) -> None:
    with engine.connect() as conn:
        df = pd.read_sql_table(FABRIC_TARGET_TABLE, conn)
    print(f"\n--- {label} ---")
    print(df.sort_values(["natural_key", "surrogate_key"]))


def _run_batch(engine, label: str, df: pd.DataFrame, metadata_name: str) -> None:
    run_dimension(
        engine,
        metadata_name,
        override_df=df,
    )
    _summarize(engine, label)


def _register_metadata_for_target(raw_file: Path) -> str:
    register_metadata(
        FABRIC_METADATA_NAME,
        {
            "target_table": FABRIC_TARGET_TABLE,
            "natural_key_cols": ["natural_key"],
            "data_columns": ["data_column"],
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": str(raw_file),
        },
    )
    return FABRIC_METADATA_NAME


def _create_engine() -> sa.engine.Engine:
    return get_fabric_sql_engine(
        FABRIC_ENDPOINT,
        database=FABRIC_DATABASE,
        port=FABRIC_PORT,
    )


def _validate_connection(engine: sa.engine.Engine) -> None:
    if not validate_fabric_sql_engine(engine):
        raise RuntimeError("Fabric connection test failed.")


def main() -> None:
    engine = _create_engine()
    _validate_connection(engine)

    target_table = FABRIC_TARGET_TABLE
    _ensure_table(engine, target_table)

    mount_info = mount_lakehouse(FABRIC_WORKSPACE, FABRIC_LAKEHOUSE, FABRIC_MOUNT_POINT)
    raw_file = Path(str(Path(mount_info.source_data_path) / "dimension.csv"))

    metadata_name = _register_metadata_for_target(raw_file)

    _run_batch(engine, "Insert phase", _load_insert_batch(), metadata_name)
    _run_batch(engine, "Update phase", _load_update_batch(), metadata_name)
    _run_batch(engine, "Delete phase", _load_delete_batch(), metadata_name)
    _run_batch(engine, "Reinsert phase", _load_reinsert_batch(), metadata_name)

    print("Fabric SCD2 load finished at", datetime.utcnow().isoformat())


if __name__ == "__main__":
    main()
