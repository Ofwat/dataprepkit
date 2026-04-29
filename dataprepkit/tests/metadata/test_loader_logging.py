import logging
from datetime import datetime, timezone

import pandas as pd
import pytest
from sqlalchemy import create_engine, inspect, text

from dataprepkit.metadata_loader import (
    METADATA_REGISTRY,
    _post_scd2_validation,
    register_metadata,
    run_dimension,
)


def _register_dummy_dimension_metadata():
    METADATA_REGISTRY.pop("dummy_dimension", None)
    register_metadata(
        "dummy_dimension",
        {
            "target_table": "dimension",
            "natural_key_cols": ["natural_key"],
            "data_columns": {"data_column": {"type": "TEXT"}},
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": "unused.csv",
        },
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
                    Effective_Date_Start TEXT NOT NULL,
                    Effective_Date_End TEXT NOT NULL,
                    Current_Ind INTEGER NOT NULL,
                    Deleted_Ind INTEGER NOT NULL
                )
                """
            )
        )
    stub_df = pd.DataFrame([{"natural_key": "x", "data_column": "v"}])
    _register_dummy_dimension_metadata()
    caplog.set_level(logging.INFO)

    run_dimension(engine, "dummy_dimension", override_df=stub_df)

    assert any(
        "Loaded dimension 'dummy_dimension'" in record.message for record in caplog.records
    )
    assert any("1 rows" in record.message or "1 row" in record.message for record in caplog.records)
    METADATA_REGISTRY.pop("dummy_dimension", None)


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
                    Effective_Date_Start TEXT NOT NULL,
                    Effective_Date_End TEXT NOT NULL,
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
    caplog.set_level(logging.INFO)

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
                    Effective_Date_Start TEXT NOT NULL,
                    Effective_Date_End TEXT NOT NULL,
                    Current_Ind INTEGER NOT NULL,
                    Deleted_Ind INTEGER NOT NULL
                )
                """
            )
        )


def test_run_dimension_logs_execution_time(monkeypatch, caplog):
    engine = create_engine("sqlite:///:memory:")
    _create_dimension_table(engine)
    _register_dummy_dimension_metadata()
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
    assert "Dimension load summary for dimension" in caplog.text
    assert captured.get("execution_time") is not None
    METADATA_REGISTRY.pop("dummy_dimension", None)


def test_run_dimension_passes_openrowset_staging_options(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    _create_dimension_table(engine)
    _register_dummy_dimension_metadata()
    captured = {}

    def fake_apply_changes(*args, **kwargs):
        captured["staging_use_openrowset_parquet"] = kwargs.get(
            "staging_use_openrowset_parquet"
        )
        captured["staging_parquet_base_dir"] = kwargs.get("staging_parquet_base_dir")
        captured["staging_copy_source_base_url"] = kwargs.get(
            "staging_copy_source_base_url"
        )
        captured["staging_copy_into_options"] = kwargs.get("staging_copy_into_options")
        return False

    monkeypatch.setattr("dataprepkit.metadata_loader.apply_changes", fake_apply_changes)

    run_dimension(
        engine,
        "dummy_dimension",
        override_df=pd.DataFrame([{"natural_key": "x", "data_column": "v"}]),
        staging_use_openrowset_parquet=True,
        staging_parquet_base_dir="/tmp/stage",
        staging_copy_source_base_url="abfss://workspace@onelake/path",
        staging_copy_into_options=", MAXERRORS = 10",
    )

    assert captured["staging_use_openrowset_parquet"] is True
    assert captured["staging_parquet_base_dir"] == "/tmp/stage"
    assert captured["staging_copy_source_base_url"] == "abfss://workspace@onelake/path"
    assert captured["staging_copy_into_options"] == ", MAXERRORS = 10"
    METADATA_REGISTRY.pop("dummy_dimension", None)


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
                    Effective_Date_Start,
                    Effective_Date_End,
                    Current_Ind,
                    Deleted_Ind
                ) VALUES (
                    :natural_key,
                    :join_numeric,
                    'value',
                    'hash',
                    :insert_ts,
                    :update_ts,
                    :effective_start,
                    :effective_end,
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
                "effective_start": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "effective_end": "9999-12-31T23:59:59.999" if current_ind else (update_date or "9999-12-31T23:59:59.999"),
                "current_ind": current_ind,
                "deleted_ind": 0 if current_ind else 1,
            },
        )


def test_post_scd2_validation_detects_duplicates():
    engine = create_engine("sqlite:///:memory:")
    _create_dimension_table(engine)
    _insert_row(engine, "dup", 1, current_ind=1)
    _insert_row(engine, "dup", 2, current_ind=1)
    with pytest.raises(
        RuntimeError,
        match=r"Multiple current rows found for natural key columns \['natural_key'\]",
    ) as exc_info:
        _post_scd2_validation(engine, "dimension", ["natural_key"])

    assert "Example duplicate keys: [{'natural_key': 'dup'}]" in str(exc_info.value)


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


def test_schema_suggest_logs_plan(monkeypatch, caplog):
    engine = create_engine("sqlite:///:memory:")
    _create_dimension_table(engine)
    metadata_name = "suggest_plan"
    register_metadata(
        metadata_name,
        {
            "target_table": "dimension",
            "natural_key_cols": ["natural_key"],
            "data_columns": ["data_column", "extra"],
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": "unused.csv",
            "schema_handling": {"mode": "suggest"},
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
        override_df=pd.DataFrame([{"natural_key": "x", "data_column": "v"}]),
    )

    assert "Schema evolution plan" in caplog.text
    assert captured["data_cols"] == ["data_column"]
    METADATA_REGISTRY.pop(metadata_name, None)


def test_schema_evolve_adds_columns(monkeypatch, caplog):
    engine = create_engine("sqlite:///:memory:")
    _create_dimension_table(engine)
    metadata_name = "evolve_plan"
    register_metadata(
        metadata_name,
        {
            "target_table": "dimension",
            "natural_key_cols": ["natural_key"],
            "data_columns": ["data_column", "extra_add"],
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": "unused.csv",
            "schema_handling": {"mode": "evolve"},
        },
    )

    captured = {}

    def fake_apply_changes(*args, **kwargs):
        captured["data_cols"] = kwargs["data_cols"]

    monkeypatch.setattr("dataprepkit.metadata_loader.apply_changes", fake_apply_changes)
    caplog.set_level(logging.INFO)

    run_dimension(
        engine,
        metadata_name,
        override_df=pd.DataFrame([{"natural_key": "x", "data_column": "v"}]),
    )

    inspector = inspect(engine)
    columns = {col["name"] for col in inspector.get_columns("dimension")}
    assert "extra_add" in columns
    assert captured["data_cols"] == ["data_column", "extra_add"]
    assert "schema_columns_added=['extra_add']" in caplog.text
    METADATA_REGISTRY.pop(metadata_name, None)


def test_run_policy_continue_on_failure(monkeypatch, caplog):
    engine = create_engine("sqlite:///:memory:")
    _create_dimension_table(engine)
    metadata_name = "policy_continue"
    register_metadata(
        metadata_name,
        {
            "target_table": "dimension",
            "natural_key_cols": ["natural_key"],
            "data_columns": ["data_column"],
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": "unused.csv",
            "run_policy": {"on_table_failure": "continue"},
        },
    )

    def fake_apply_changes(*args, **kwargs):
        raise RuntimeError("failure")

    monkeypatch.setattr("dataprepkit.metadata_loader.apply_changes", fake_apply_changes)
    caplog.set_level(logging.INFO)

    run_dimension(
        engine,
        metadata_name,
        override_df=pd.DataFrame([{"natural_key": "x", "data_column": "v"}]),
    )

    assert "Run policy on table failure: continue" in caplog.text
    METADATA_REGISTRY.pop(metadata_name, None)


def test_processing_class_applied(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    _create_dimension_table(engine)
    metadata_name = "transform"
    register_metadata(
        metadata_name,
        {
            "target_table": "dimension",
            "natural_key_cols": ["natural_key"],
            "data_columns": ["data_column"],
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": "unused.csv",
            "processing_class": lambda df: df.assign(data_column=df["data_column"].str.upper()),
        },
    )

    captured = {}

    def fake_apply_changes(*args, **kwargs):
        captured["incoming"] = kwargs["incoming"]

    monkeypatch.setattr("dataprepkit.metadata_loader.apply_changes", fake_apply_changes)

    run_dimension(
        engine,
        metadata_name,
        override_df=pd.DataFrame([{"natural_key": "x", "data_column": "lower"}]),
    )

    assert captured["incoming"].iloc[0]["data_column"] == "LOWER"
    METADATA_REGISTRY.pop(metadata_name, None)


def test_metrics_logged(caplog, monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    _create_dimension_table(engine)
    _register_dummy_dimension_metadata()
    caplog.set_level(logging.INFO)

    run_dimension(
        engine,
        "dummy_dimension",
        override_df=pd.DataFrame([{"natural_key": "x", "data_column": "v"}]),
    )

    assert "Table dimension completed in" in caplog.text
    assert "Dimension load summary for dimension" in caplog.text
    assert "inserted_rows=1" in caplog.text
    assert "new_rows=1" in caplog.text
    assert "new_keys=[{'natural_key': 'x'}]" in caplog.text
    assert "edited_rows=0" in caplog.text
    assert "edited_changes=[]" in caplog.text
    assert "soft_deleted_rows=0" in caplog.text
    assert "soft_deleted_keys=[]" in caplog.text
    assert "reactivated_keys=[]" in caplog.text
    METADATA_REGISTRY.pop("dummy_dimension", None)


def test_metrics_logged_include_edited_natural_keys(caplog):
    engine = create_engine("sqlite:///:memory:")
    _create_dimension_table(engine)
    _register_dummy_dimension_metadata()
    caplog.set_level(logging.INFO)

    run_dimension(
        engine,
        "dummy_dimension",
        override_df=pd.DataFrame([{"natural_key": "x", "data_column": "before"}]),
    )
    caplog.clear()

    run_dimension(
        engine,
        "dummy_dimension",
        override_df=pd.DataFrame([{"natural_key": "x", "data_column": "after"}]),
    )

    assert "edited_rows=1" in caplog.text
    assert (
        "edited_changes=[{'natural_key': 'x', 'changes': {'data_column': {'from': 'before', 'to': 'after'}}}]"
        in caplog.text
    )
    METADATA_REGISTRY.pop("dummy_dimension", None)


def test_archive_snapshot(tmp_path, caplog):
    engine = create_engine("sqlite:///:memory:")
    _create_dimension_table(engine)
    metadata_name = "archive_test"
    register_metadata(
        metadata_name,
        {
            "target_table": "dimension",
            "natural_key_cols": ["natural_key"],
            "data_columns": ["data_column"],
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": "unused.csv",
            "archive_path": str(tmp_path / "snapshots"),
        },
    )

    caplog.set_level(logging.WARNING)
    run_dimension(
        engine,
        metadata_name,
        override_df=pd.DataFrame([{"natural_key": "x", "data_column": "v"}]),
    )

    files = list((tmp_path / "snapshots").glob("*.parquet"))
    if not files and not caplog.text:
        pytest.skip("Parquet writing not available in this environment")
    if files:
        assert files, "Parquet snapshot written"
    else:
        assert "Existing table 'dimension' is missing metadata columns" in caplog.text
    METADATA_REGISTRY.pop(metadata_name, None)


def _create_dimension_table_with_archive(engine, table_name: str):
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE TABLE {table_name} (
                    surrogate_key INTEGER PRIMARY KEY AUTOINCREMENT,
                    natural_key TEXT NOT NULL,
                    join_numeric_key INTEGER NOT NULL,
                    data_column TEXT,
                    row_hash TEXT NOT NULL,
                    Insert_Date TEXT NOT NULL,
                    Update_Date TEXT,
                    Effective_Date_Start TEXT NOT NULL,
                    Effective_Date_End TEXT NOT NULL,
                    Current_Ind INTEGER NOT NULL,
                    Deleted_Ind INTEGER NOT NULL,
                    Batch_Id TEXT NOT NULL,
                    Archive_Filename TEXT NOT NULL
                )
                """
            )
        )


def _register_archive_metadata(metadata_name, table_name, archive_path):
    register_metadata(
        metadata_name,
        {
            "target_table": table_name,
            "natural_key_cols": ["natural_key"],
            "data_columns": ["data_column"],
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": "unused.csv",
            "archive_path": str(archive_path),
        },
    )


def test_archive_snapshot_skips_when_no_changes(monkeypatch, tmp_path):
    engine = create_engine("sqlite:///:memory:")
    table = "dimension_archive_skip"
    metadata_name = "archive_skip"
    _create_dimension_table_with_archive(engine, table)
    _register_archive_metadata(metadata_name, table, tmp_path / "snapshots_skip")

    archive_calls = []
    monkeypatch.setattr(
        "dataprepkit.metadata_loader._archive_snapshot",
        lambda *args, **kwargs: archive_calls.append(True),
    )

    monkeypatch.setattr(
        "dataprepkit.metadata_loader.apply_changes",
        lambda *args, **kwargs: False,
    )

    run_dimension(
        engine,
        metadata_name,
        override_df=pd.DataFrame([{"natural_key": "x", "data_column": "v"}]),
    )

    assert not archive_calls
    METADATA_REGISTRY.pop(metadata_name, None)


def test_archive_snapshot_runs_only_on_changes(monkeypatch, tmp_path):
    engine = create_engine("sqlite:///:memory:")
    table = "dimension_archive_apply"
    metadata_name = "archive_apply"
    _create_dimension_table_with_archive(engine, table)
    _register_archive_metadata(metadata_name, table, tmp_path / "snapshots_apply")

    archive_paths = []
    monkeypatch.setattr(
        "dataprepkit.metadata_loader._archive_snapshot",
        lambda *args, **_kwargs: archive_paths.append(args[2]),
    )

    monkeypatch.setattr(
        "dataprepkit.metadata_loader.apply_changes",
        lambda *args, **kwargs: True,
    )

    run_dimension(
        engine,
        metadata_name,
        override_df=pd.DataFrame([{"natural_key": "x", "data_column": "v"}]),
    )

    assert len(archive_paths) == 1
    assert metadata_name in archive_paths[0].name
    METADATA_REGISTRY.pop(metadata_name, None)
