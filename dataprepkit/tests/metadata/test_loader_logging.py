import logging

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from dataprepkit.metadata_loader import METADATA_REGISTRY, register_metadata, run_dimension


def test_run_dimension_logs_row_count(caplog):
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
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
            )
        )
    stub_df = pd.DataFrame([{"natural_key": "x", "data_column": "v"}])
    caplog.set_level(logging.INFO)

    run_dimension(engine, "dummy_dimension", override_df=stub_df)

    assert any(
        "Loaded dimension 'dummy_dimension'" in record.message for record in caplog.records
    )
    assert any("1 rows" in record.message or "1 row" in record.message for record in caplog.records)


def test_schema_drift_logs_safe_write_set(monkeypatch, caplog):
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE drift_table (
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
            )
        )

    metadata_name = "drift_table"
    register_metadata(
        metadata_name,
        {
            "target_table": "drift_table",
            "natural_key_cols": ["natural_key"],
            "data_columns": ["data_column", "extra_column"],
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": "unused.csv",
        },
    )

    captured = {}

    def fake_apply_changes(*args, **kwargs):
        captured["data_cols"] = kwargs["data_cols"]

    monkeypatch.setattr("dataprepkit.metadata_loader.apply_changes", fake_apply_changes)
    caplog.set_level(logging.WARNING)

    run_dimension(
        engine,
        metadata_name,
        override_df=pd.DataFrame([{"natural_key": "k", "data_column": "v"}]),
    )

    assert "missing columns" in caplog.text
    assert captured["data_cols"] == ["data_column"]
    METADATA_REGISTRY.pop(metadata_name, None)
