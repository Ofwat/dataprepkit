"""Fabric SQL example that reuses the dataprepkit metadata driven SCD2 API."""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from dataprepkit.helpers.connectors.warehouse import get_fabric_warehouse_engine
from dataprepkit.metadata_loader import register_metadata, run_dimension
from dataprepkit.storage import mount_lakehouse


ENDPOINT_ENV = "FABRIC_SQL_ENDPOINT"
PORT_ENV = "FABRIC_SQL_PORT"
TARGET_TABLE_ENV = "FABRIC_TARGET_TABLE"
METADATA_NAME_ENV = "FABRIC_METADATA_NAME"
RAW_FILE_ENV = "FABRIC_RAW_FILEPATH"
WORKSPACE_ENV = "FABRIC_WORKSPACE"
LAKEHOUSE_ENV = "FABRIC_LAKEHOUSE"
MOUNT_POINT_ENV = "FABRIC_MOUNT_POINT"
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
        df = pd.read_sql_table(os.environ[TARGET_TABLE_ENV], conn)
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
    name = os.environ.get(METADATA_NAME_ENV, "fabric_dimension")
    register_metadata(
        name,
        {
            "target_table": os.environ[TARGET_TABLE_ENV],
            "natural_key_cols": ["natural_key"],
            "data_columns": ["data_column"],
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": str(raw_file),
        },
    )
    return name


def main() -> None:
    engine = get_fabric_warehouse_engine(
        os.environ[ENDPOINT_ENV],
        port=int(os.environ.get(PORT_ENV, "1433")),
    )

    target_table = os.environ[TARGET_TABLE_ENV]
    _ensure_table(engine, target_table)

    workspace_name = os.environ[WORKSPACE_ENV]
    lakehouse_name = os.environ[LAKEHOUSE_ENV]
    mount_point = os.environ.get(MOUNT_POINT_ENV, "/home/trusted-service-user/mounts/Source_Data")
    _, mount_path = mount_lakehouse(workspace_name, lakehouse_name, mount_point)
    raw_file = Path(os.environ.get(RAW_FILE_ENV, str(mount_path / "dimension.csv")))

    metadata_name = _register_metadata_for_target(raw_file)

    _run_batch(engine, "Insert phase", _load_insert_batch(), metadata_name)
    _run_batch(engine, "Update phase", _load_update_batch(), metadata_name)
    _run_batch(engine, "Delete phase", _load_delete_batch(), metadata_name)
    _run_batch(engine, "Reinsert phase", _load_reinsert_batch(), metadata_name)

    print("Fabric SCD2 load finished at", datetime.utcnow().isoformat())


if __name__ == "__main__":
    main()
