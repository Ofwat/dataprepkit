"""
Example script that creates a dummy table, runs the SCD2 pipeline, and validates outcomes.

This script uses SQLite so you can test locally before switching to Fabric.
"""

from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

from dataprepkit.scd2 import apply_changes


def _create_schema(engine):
    create_sql = """
    CREATE TABLE IF NOT EXISTS dimension (
        surrogate_key INTEGER PRIMARY KEY AUTOINCREMENT,
        natural_key TEXT NOT NULL,
        join_numeric_key INTEGER NOT NULL,
        data_column TEXT,
        row_hash TEXT,
        Insert_Date TEXT NOT NULL,
        Update_Date TEXT,
        Current_Ind INTEGER NOT NULL,
        Deleted_Ind INTEGER NOT NULL
    )
    """
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS dimension"))
        conn.execute(text(create_sql))


def _seed_initial_data(engine):
    initial = pd.DataFrame(
        [
            {"natural_key": "a1", "join_numeric_key": 1, "data_column": "a2"},
            {"natural_key": "b1", "join_numeric_key": 2, "data_column": "b2"},
        ]
    )
    apply_changes(
        engine=engine,
        target_table="dimension",
        incoming=initial,
        natural_key_cols=["natural_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
    )


def _run_scd2(engine):
    incoming = pd.DataFrame(
        [
            {"natural_key": "a1", "data_column": "a2"},
            {"natural_key": "b1", "data_column": "updated"},
            {"natural_key": "c1", "data_column": "c2"},
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
    )


def _summarize(engine):
    with engine.connect() as conn:
        df = pd.read_sql_table("dimension", conn)
    print("Final table:")
    print(df.sort_values(["natural_key", "surrogate_key"]))


def _validate(engine):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM dimension WHERE Current_Ind = 1"))
        current_count = result.scalar()
    assert current_count == 3, "There should be exactly three current rows."


def main():
    engine = create_engine("sqlite:///:memory:")
    _create_schema(engine)
    _seed_initial_data(engine)
    _run_scd2(engine)
    _summarize(engine)
    _validate(engine)
    print("SCD2 demo completed successfully at", datetime.utcnow().isoformat())


if __name__ == "__main__":
    main()
