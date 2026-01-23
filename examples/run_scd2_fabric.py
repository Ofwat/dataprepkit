"""Fabric SQL example that reuses the dataprepkit SCD2 API."""

from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from dataprepkit.helpers.connectors.warehouse import get_fabric_warehouse_engine
from dataprepkit.scd2 import apply_changes


ENDPOINT_ENV = "FABRIC_SQL_ENDPOINT"
PORT_ENV = "FABRIC_SQL_PORT"
TARGET_TABLE_ENV = "FABRIC_TARGET_TABLE"


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


def _run_batch(engine, label: str, df: pd.DataFrame) -> None:
    apply_changes(
        engine=engine,
        target_table=os.environ[TARGET_TABLE_ENV],
        incoming=df,
        natural_key_cols=["natural_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
    )
    _summarize(engine, label)


def main() -> None:
    engine = get_fabric_warehouse_engine(
        os.environ[ENDPOINT_ENV],
        port=int(os.environ.get(PORT_ENV, "1433")),
    )

    target_table = os.environ[TARGET_TABLE_ENV]
    _ensure_table(engine, target_table)

    _run_batch(engine, "Insert phase", _load_insert_batch())
    _run_batch(engine, "Update phase", _load_update_batch())
    _run_batch(engine, "Delete phase", _load_delete_batch())
    _run_batch(engine, "Reinsert phase", _load_reinsert_batch())

    print("Fabric SCD2 load finished at", datetime.utcnow().isoformat())


if __name__ == "__main__":
    main()
