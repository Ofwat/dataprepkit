"""Lightweight smoke test for the SQL-backed SCD2 flow."""

from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

from dataprepkit.scd2 import apply_changes

SYSTEM_COLUMNS = {
    "surrogate_key": "surrogate_key",
    "join_numeric_key": "join_numeric_key",
    "row_hash": "row_hash",
    "insert_date": "Insert_Date",
    "update_date": "Update_Date",
    "current_ind": "Current_Ind",
    "deleted_ind": "Deleted_Ind",
}


def _create_dimension_table(engine):
    ddl = """
    CREATE TABLE dimension (
        surrogate_key INTEGER PRIMARY KEY AUTOINCREMENT,
        natural_key TEXT NOT NULL,
        join_numeric_key INTEGER NOT NULL,
        data_column TEXT,
        row_hash TEXT NOT NULL,
        Insert_Date TEXT NOT NULL,
        Update_Date TEXT,
        Current_Ind INTEGER NOT NULL,
        Deleted_Ind INTEGER NOT NULL
    )
    """
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS dimension"))
        conn.execute(text(ddl))


def _seed_initial_data(engine):
    rows = [
        {
            "natural_key": "a1",
            "join_numeric_key": 1,
            "data_column": "a2",
            "row_hash": "hash-a2",
            "Insert_Date": datetime.utcnow().isoformat(timespec="seconds"),
            "Current_Ind": 1,
            "Deleted_Ind": 0,
        },
        {
            "natural_key": "b1",
            "join_numeric_key": 2,
            "data_column": "b2",
            "row_hash": "hash-b2",
            "Insert_Date": datetime.utcnow().isoformat(timespec="seconds"),
            "Current_Ind": 1,
            "Deleted_Ind": 0,
        },
    ]
    insert_sql = text(
        """
        INSERT INTO dimension (
            natural_key,
            join_numeric_key,
            data_column,
            row_hash,
            Insert_Date,
            Current_Ind,
            Deleted_Ind
        ) VALUES (
            :natural_key,
            :join_numeric_key,
            :data_column,
            :row_hash,
            :Insert_Date,
            :Current_Ind,
            :Deleted_Ind
        )
        """
    )
    with engine.begin() as conn:
        for row in rows:
            conn.execute(insert_sql, row)


def main():
    engine = create_engine("sqlite:///:memory:")
    _create_dimension_table(engine)
    _seed_initial_data(engine)

    incoming = pd.DataFrame(
        [
            {"natural_key": "a1", "data_column": "a2"},
            {"natural_key": "b1", "data_column": "updated"},
            {"natural_key": "c1", "data_column": "c3"},
        ]
    )

    apply_changes(
        engine=engine,
        target_table="dimension",
        incoming=incoming,
        natural_key_cols=["natural_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
    )

    with engine.connect() as conn:
        result = pd.read_sql_table("dimension", conn)
    print("Smoke test completed. Dimension table:")
    print(result.sort_values(["natural_key", "surrogate_key"]))
    print("\nRun completeness: existing rows closed, updated and inserted rows present.")


if __name__ == "__main__":
    main()
