import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from dataprepkit.metadata_loader import DimensionMetadata, get_metadata, run_dimension
from dataprepkit.helpers.staging import stage_dataframe
from dataprepkit.helpers.schema import ensure_schema_exists


def _create_scd2_table(engine):
    create_sql = """
    CREATE TABLE dimension (
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
        conn.execute(text(create_sql))


def test_run_dimension_uses_metadata(engine=None):
    engine = engine or create_engine("sqlite:///:memory:")
    _create_scd2_table(engine)
    df = pd.DataFrame(
        [
            {"natural_key": "a1", "data_column": "a2", "join_numeric_key": 1},
            {"natural_key": "b1", "data_column": "b2", "join_numeric_key": 2},
        ]
    )

    run_dimension(engine, "dummy_dimension", override_df=df)

    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM dimension")).scalar()

    assert result == 2


def test_get_metadata_unknown_key_raises():
    with pytest.raises(KeyError):
        get_metadata("nope")


def test_dimension_model_requires_columns():
    with pytest.raises(ValueError):
        DimensionMetadata(
            name="invalid",
            target_table="dimension",
            natural_key_cols=[],
            data_columns=["data_column"],
            surrogate_key="surrogate_key",
            join_numeric_key="join_numeric_key",
            filepath="dummy.csv",
        )


def test_schema_mismatch_raises(engine=None):
    engine = engine or create_engine("sqlite:///:memory:")
    # missing data_column to force mismatch
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE dimension (
                    surrogate_key INTEGER PRIMARY KEY AUTOINCREMENT,
                    natural_key TEXT NOT NULL,
                    join_numeric_key INTEGER NOT NULL,
                    row_hash TEXT NOT NULL,
                    Insert_Date TEXT NOT NULL,
                    Update_Date TEXT,
                    Current_Ind INTEGER NOT NULL,
                    Deleted_Ind INTEGER NOT NULL
                )
                """
            )
        )

    with pytest.raises(RuntimeError):
        run_dimension(engine, "dummy_dimension", override_df=pd.DataFrame([{"natural_key": "x", "data_column": "v"}]))


def test_stage_dataframe_creates_table():
    engine = create_engine("sqlite:///:memory:")
    df = pd.DataFrame({"col": [1, 2]})

    stage_dataframe(engine, "stage_table", df, if_exists="replace")

    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM stage_table")).scalar()
    assert result == 2


def test_stage_dataframe_append():
    engine = create_engine("sqlite:///:memory:")
    df = pd.DataFrame({"col": [1, 2]})
    stage_dataframe(engine, "stage_table", df, if_exists="replace")
    stage_dataframe(engine, "stage_table", df, if_exists="append")

    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM stage_table")).scalar()
    assert result == 4


def test_stage_dataframe_ensures_schema(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    df = pd.DataFrame({"col": [1]})
    calls = []

    def fake_ensure(engine_arg, schema_arg):
        calls.append((engine_arg, schema_arg))

    monkeypatch.setattr("dataprepkit.helpers.staging.ensure_schema_exists", fake_ensure)
    stage_dataframe(engine, "stage_table", df, schema="custom")

    assert calls == [(engine, "custom")]
