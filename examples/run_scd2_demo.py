"""SQLite demo that uses the metadata loader to drive the SCD2 API."""

from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

from dataprepkit.metadata_loader import register_metadata, run_dimension


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


def _register_demo_metadata() -> None:
    register_metadata(
        "demo_dimension",
        {
            "target_table": "dimension",
            "natural_key_cols": ["natural_key"],
            "data_columns": ["data_column"],
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": str(Path(__file__).resolve().parents[2] / "examples" / "dummy_dimension.csv"),
            "description": "In-memory demo powered via metadata and override DataFrames.",
        },
    )


def _seed_initial_data(engine):
    incoming = pd.DataFrame(
        [
            {"natural_key": "a1", "join_numeric_key": 1, "data_column": "a2"},
            {"natural_key": "b1", "join_numeric_key": 2, "data_column": "b2"},
        ]
    )
    run_dimension(engine, "demo_dimension", override_df=incoming)


def _run_next_batch(engine, label, rows):
    incoming = pd.DataFrame(rows)
    run_dimension(engine, "demo_dimension", override_df=incoming)
    _summarize(engine, label)


def _summarize(engine, label):
    with engine.connect() as conn:
        df = pd.read_sql_table("dimension", conn)
    print(f"\n--- {label} ---")
    print(df.sort_values(["natural_key", "surrogate_key"]))


def _validate(engine):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM dimension WHERE Current_Ind = 1"))
        current_count = result.scalar()
    assert current_count == 3, "There should be exactly three current rows."


def main():
    engine = create_engine("sqlite:///:memory:")
    _register_demo_metadata()
    _create_schema(engine)
    _seed_initial_data(engine)
    _run_next_batch(
        engine,
        "Update phase",
        [
            {"natural_key": "a1", "data_column": "a2"},
            {"natural_key": "b1", "data_column": "updated"},
            {"natural_key": "c1", "data_column": "c2"},
        ],
    )
    _validate(engine)
    print("SCD2 demo completed successfully at", datetime.utcnow().isoformat())


if __name__ == "__main__":
    main()
