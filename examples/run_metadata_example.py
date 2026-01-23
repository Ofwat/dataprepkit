"""
Example that exercises the metadata-driven orchestrator end-to-end.

Create target tables, register metadata, and call `run_dimension` to invoke
the new SQL-backed SCD2 logic, dependency joins, schema evolution, run policies,
and snapshot archiving.
"""

from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

from dataprepkit.metadata_loader import register_metadata, run_dimension


def _bootstrap_tables(engine):
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS dependency"))
        conn.execute(
            text(
                """
                CREATE TABLE dependency (
                    source_key TEXT PRIMARY KEY,
                    dep_value TEXT,
                    Current_Ind INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dependency (source_key, dep_value, Current_Ind)
                VALUES ('x', 'extra-context', 1)
                """
            )
        )

        conn.execute(text("DROP TABLE IF EXISTS dimension"))
        conn.execute(
            text(
                """
                CREATE TABLE dimension (
                    surrogate_key INTEGER PRIMARY KEY AUTOINCREMENT,
                    natural_key TEXT NOT NULL,
                    join_numeric_key INTEGER NOT NULL,
                    data_column TEXT,
                    dep_value TEXT,
                    row_hash TEXT NOT NULL,
                    Insert_Date TEXT NOT NULL,
                    Update_Date TEXT,
                    Current_Ind INTEGER NOT NULL,
                    Deleted_Ind INTEGER NOT NULL
                )
                """
            )
        )


def main():
    engine = create_engine("sqlite:///:memory:")
    _bootstrap_tables(engine)

    register_metadata(
        name="metadata_example",
        metadata={
            "target_table": "dimension",
            "natural_key_cols": ["natural_key"],
            "data_columns": {
                "data_column": {"type": "TEXT", "nullable": False},
                "dep_value": {"type": "TEXT"},
            },
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": str(Path(__file__).resolve().parents[1] / "examples" / "dummy_dimension.csv"),
            "schema_handling": {"mode": "evolve"},
            "dependencies": [
                {
                    "table": "dependency",
                    "on": [{"source": "natural_key", "target": "source_key"}],
                    "select": {"dep_value": "dep_value"},
                    "how": "left",
                }
            ],
            "processing_class": lambda df: df.assign(data_column=df.data_column.str.upper()),
            "run_policy": {"on_table_failure": "continue"},
            "archive_path": str(Path(__file__).resolve().parents[1] / "examples" / "archives"),
        },
    )

    incoming = pd.DataFrame(
        [
            {"natural_key": "x", "data_column": "a"},
            {"natural_key": "y", "data_column": "b"},
        ]
    )

    run_dimension(
        engine,
        "metadata_example",
        override_df=incoming,
    )

    with engine.connect() as conn:
        print(pd.read_sql_table("dimension", conn))


if __name__ == "__main__":
    main()
