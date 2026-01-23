import logging
from datetime import datetime, timezone

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from dataprepkit.metadata_loader import (
    METADATA_REGISTRY,
    _post_scd2_validation,
    register_metadata,
    run_dimension,
)


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


def test_rename_collision_raises(caplog):
    engine = create_engine("sqlite:///:memory:")
    _create_dimension_table(engine)
    metadata_name = "rename_collision"
    register_metadata(
        metadata_name,
        {
            "target_table": "dimension",
            "natural_key_cols": ["natural_key"],
            "data_columns": ["data_column"],
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": "unused.csv",
            "column_renames": {"natural_key": "data_column"},
        },
    )

    caplog.set_level(logging.ERROR)
    with pytest.raises(ValueError):
        run_dimension(engine, metadata_name, override_df=pd.DataFrame([{"natural_key": "x", "data_column": "v"}]))

    assert "Column rename collision detected" in caplog.text
    METADATA_REGISTRY.pop(metadata_name, None)


def _create_dimension_table(engine):
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


def test_run_dimension_logs_execution_time(monkeypatch, caplog):
    engine = create_engine("sqlite:///:memory:")
    _create_dimension_table(engine)
    captured = {}

    def fake_apply_changes(*args, **kwargs):
        captured["execution_time"] = kwargs["execution_time"]

    monkeypatch.setattr("dataprepkit.metadata_loader.apply_changes", fake_apply_changes)
    caplog.set_level(logging.INFO)

    run_dimension(
        engine,
        "dummy_dimension",
        override_df=pd.DataFrame([{"natural_key": "x", "data_column": "v"}]),
    )

    assert "Execution timestamp" in caplog.text
    assert "SCD2 classification counts: not available" in caplog.text
    assert captured.get("execution_time") is not None


def _insert_row(engine, natural_key, join_numeric, current_ind=1, update_date=None):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO dimension (
                    natural_key,
                    join_numeric_key,
                    data_column,
                    row_hash,
                    Insert_Date,
                    Update_Date,
                    Current_Ind,
                    Deleted_Ind
                ) VALUES (
                    :natural_key,
                    :join_numeric,
                    'value',
                    'hash',
                    :insert_ts,
                    :update_ts,
                    :current_ind,
                    :deleted_ind
                )
                """
            ),
            {
                "natural_key": natural_key,
                "join_numeric": join_numeric,
                "insert_ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "update_ts": update_date,
                "current_ind": current_ind,
                "deleted_ind": 0 if current_ind else 1,
            },
        )


def test_post_scd2_validation_detects_duplicates():
    engine = create_engine("sqlite:///:memory:")
    _create_dimension_table(engine)
    _insert_row(engine, "dup", 1, current_ind=1)
    _insert_row(engine, "dup", 2, current_ind=1)
    with pytest.raises(RuntimeError, match="Multiple current rows"):
        _post_scd2_validation(engine, "dimension", ["natural_key"])


def test_post_scd2_validation_detects_current_with_update_date():
    engine = create_engine("sqlite:///:memory:")
    _create_dimension_table(engine)
    _insert_row(
        engine,
        "good",
        1,
        current_ind=1,
        update_date=datetime.now(timezone.utc).isoformat(),
    )
    with pytest.raises(RuntimeError, match="current row has Update_Date not NULL"):
        _post_scd2_validation(engine, "dimension", ["natural_key"])


def test_dependency_join_enriches_dataframe(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    _create_dimension_table(engine)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE dependency (
                    source_key TEXT NOT NULL,
                    dep_value TEXT NOT NULL,
                    Current_Ind INTEGER NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dependency (source_key, dep_value, Current_Ind)
                VALUES ('x', 'extra', 1)
                """
            )
        )

    metadata_name = "dep_dimension"
    register_metadata(
        metadata_name,
        {
            "target_table": "dimension",
            "natural_key_cols": ["natural_key"],
            "data_columns": ["data_column", "dep_value"],
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": "unused.csv",
            "dependencies": [
                {
                    "table": "dependency",
                    "on": [{"source": "natural_key", "target": "source_key"}],
                    "select": {"dep_value": "dep_value"},
                    "how": "left",
                }
            ],
        },
    )

    captured = {}

    def fake_apply_changes(*args, **kwargs):
        captured["incoming"] = kwargs["incoming"]

    monkeypatch.setattr("dataprepkit.metadata_loader.apply_changes", fake_apply_changes)

    run_dimension(
        engine,
        metadata_name,
        override_df=pd.DataFrame([{"natural_key": "x", "data_column": "base"}]),
    )

    assert captured["incoming"].iloc[0]["dep_value"] == "extra"
    METADATA_REGISTRY.pop(metadata_name, None)
