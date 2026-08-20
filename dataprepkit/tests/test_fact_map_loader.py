from sqlalchemy import create_engine, text

import shutil
import uuid
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import dataprepkit.fact_map_loader as fact_map_loader_module
from dataprepkit.fact_map_loader import load_fact_from_maps


def test_data_column_select_expression_casts_mssql_float_to_text():
    engine = SimpleNamespace(dialect=SimpleNamespace(name="mssql"))

    expression = fact_map_loader_module._data_column_select_expression(
        engine,
        column_name="Measure_Value",
        staging_type="FLOAT",
        target_type="VARCHAR(4000)",
    )

    assert expression == (
        "CONVERT(VARCHAR(4000), "
        "CAST(s.[Measure_Value] AS DECIMAL(38,18)))"
    )


def test_data_column_select_expression_keeps_non_float_types_unchanged():
    engine = SimpleNamespace(dialect=SimpleNamespace(name="mssql"))

    decimal_expression = fact_map_loader_module._data_column_select_expression(
        engine,
        column_name="Measure_Value",
        staging_type="DECIMAL(20,14)",
        target_type="VARCHAR(4000)",
    )
    numeric_target_expression = fact_map_loader_module._data_column_select_expression(
        engine,
        column_name="Measure_Value",
        staging_type="FLOAT",
        target_type="DECIMAL(38,18)",
    )

    assert decimal_expression == "s.[Measure_Value]"
    assert numeric_target_expression == "s.[Measure_Value]"


def test_load_fact_from_maps_builds_fact_table_from_staging_and_dimensions():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS facts"))
        conn.execute(
            text(
                """
                CREATE TABLE staging_fact (
                    Organisation_Cd TEXT,
                    Measure_Cd TEXT,
                    Submission_Period_Cd TEXT,
                    Value REAL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE dim_organisation (
                    Organisation_Cd TEXT,
                    Organisation_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE dim_measure (
                    Measure_Cd TEXT,
                    Measure_Instance_Id INTEGER,
                    Measure_Name TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE dim_interval (
                    Interval_Cd TEXT,
                    Interval_Instance_Id INTEGER,
                    Interval_Type TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO staging_fact (
                    Organisation_Cd,
                    Measure_Cd,
                    Submission_Period_Cd,
                    Value
                )
                VALUES
                    ('ORG1', 'MEASURE1', '2024M01', 10.5),
                    ('ORG2', 'MEASURE2', '2024M02', 11.5)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_organisation (
                    Organisation_Cd,
                    Organisation_Instance_Id
                )
                VALUES ('ORG1', 101), ('ORG2', 102)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_measure (
                    Measure_Cd,
                    Measure_Instance_Id,
                    Measure_Name
                )
                VALUES
                    ('MEASURE1', 201, 'Measure One'),
                    ('MEASURE2', 202, 'Measure Two')
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_interval (
                    Interval_Cd,
                    Interval_Instance_Id,
                    Interval_Type
                )
                VALUES
                    ('2024M01', 301, 'Month'),
                    ('2024M02', 302, 'Month')
                """
            )
        )

    lookup_map = {
        "Organisation_Cd": {
            "source": {
                "schema": "main",
                "table": "dim_organisation",
                "lookup_column": "Organisation_Cd",
                "value_column": "Organisation_Instance_Id",
            },
            "target": {
                "column": "Organisation_Instance_Id",
                "comment": "Foo",
            },
        },
        "Measure_Cd": {
            "source": {
                "schema": "main",
                "table": "dim_measure",
                "lookup_column": "Measure_Cd",
                "value_column": "Measure_Instance_Id",
            },
            "target": {
                "column": "Measure_Instance_Id",
                "comment": "Bar",
            },
        },
        "Submission_Period_Cd": {
            "source": {
                "schema": "main",
                "table": "dim_interval",
                "lookup_column": "Interval_Cd",
                "value_column": "Interval_Instance_Id",
            },
            "target": {
                "column": "Submission_Period_Interval_Instance_Id",
                "comment": "Baz",
            },
        },
    }
    data_columns = [
        {
            "column": "Value",
            "comment": "Actual inserted value",
        }
    ]
    additional_columns = [
        {
            "target": {
                "column": "Measure_Name",
                "comment": "Column brought from dim table",
            },
            "source": {
                "schema": "main",
                "table": "dim_measure",
                "column": "Measure_Name",
            },
            "surrogate_keys": {
                "fact": "Measure_Instance_Id",
                "dim": "Measure_Instance_Id",
            },
        },
        {
            "target": {
                "column": "Submission_Period_Interval_Type",
                "comment": "Column brought from dim table",
            },
            "source": {
                "schema": "main",
                "table": "dim_interval",
                "column": "Interval_Type",
            },
            "surrogate_keys": {
                "fact": "Submission_Period_Interval_Instance_Id",
                "dim": "Interval_Instance_Id",
            },
        },
    ]

    load_fact_from_maps(
        engine=engine,
        lookup_map=lookup_map,
        data_columns=data_columns,
        additional_columns=additional_columns,
        staging_table="staging_fact",
        staging_schema="main",
        fact_table="fact_result",
        fact_schema="facts",
    )

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    Organisation_Instance_Id,
                    Measure_Instance_Id,
                    Submission_Period_Interval_Instance_Id,
                    Value,
                    Measure_Name,
                    Submission_Period_Interval_Type
                FROM facts.fact_result
                ORDER BY Organisation_Instance_Id
                """
            )
        ).mappings().all()
        columns = conn.execute(
            text("PRAGMA facts.table_info(fact_result)")
        ).mappings().all()

    assert rows == [
        {
            "Organisation_Instance_Id": 101,
            "Measure_Instance_Id": 201,
            "Submission_Period_Interval_Instance_Id": 301,
            "Value": 10.5,
            "Measure_Name": "Measure One",
            "Submission_Period_Interval_Type": "Month",
        },
        {
            "Organisation_Instance_Id": 102,
            "Measure_Instance_Id": 202,
            "Submission_Period_Interval_Instance_Id": 302,
            "Value": 11.5,
            "Measure_Name": "Measure Two",
            "Submission_Period_Interval_Type": "Month",
        },
    ]
    assert {column["name"]: column["type"] for column in columns} == {
        "Organisation_Instance_Id": "INTEGER",
        "Measure_Instance_Id": "INTEGER",
        "Submission_Period_Interval_Instance_Id": "INTEGER",
        "Value": "REAL",
        "Measure_Name": "TEXT",
        "Submission_Period_Interval_Type": "TEXT",
    }
    assert {column["name"]: column["notnull"] for column in columns} == {
        "Organisation_Instance_Id": 1,
        "Measure_Instance_Id": 1,
        "Submission_Period_Interval_Instance_Id": 1,
        "Value": 0,
        "Measure_Name": 0,
        "Submission_Period_Interval_Type": 0,
    }


def test_load_fact_from_maps_matches_lookup_keys_case_sensitively():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE staging_fact (Measure_Cd TEXT COLLATE NOCASE)"))
        conn.execute(
            text(
                """
                CREATE TABLE dim_measure (
                    Measure_Cd TEXT COLLATE NOCASE,
                    Measure_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(text("INSERT INTO staging_fact (Measure_Cd) VALUES ('BIO')"))
        conn.execute(
            text(
                """
                INSERT INTO dim_measure (Measure_Cd, Measure_Instance_Id)
                VALUES ('Bio', 201), ('BIO', 202)
                """
            )
        )

    load_fact_from_maps(
        engine=engine,
        lookup_map={
            "Measure_Cd": {
                "source": {
                    "table": "dim_measure",
                    "lookup_column": "Measure_Cd",
                    "value_column": "Measure_Instance_Id",
                },
                "target": {"column": "Measure_Instance_Id"},
            }
        },
        data_columns=[],
        additional_columns=[],
        staging_table="staging_fact",
        staging_schema=None,
        fact_table="fact_result",
    )

    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT Measure_Instance_Id FROM fact_result")
        ).mappings().all()

    assert rows == [{"Measure_Instance_Id": 202}]


def test_load_fact_from_maps_rejects_projected_row_count_mismatch():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE staging_fact (Measure_Cd TEXT)"))
        conn.execute(
            text(
                """
                CREATE TABLE dim_measure (
                    Measure_Cd TEXT,
                    Measure_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(text("INSERT INTO staging_fact (Measure_Cd) VALUES ('BIO')"))
        conn.execute(
            text(
                """
                INSERT INTO dim_measure (Measure_Cd, Measure_Instance_Id)
                VALUES ('BIO', 201), ('BIO', 202)
                """
            )
        )

    with pytest.raises(RuntimeError, match="Projected fact row count"):
        load_fact_from_maps(
            engine=engine,
            lookup_map={
                "Measure_Cd": {
                    "source": {
                        "table": "dim_measure",
                        "lookup_column": "Measure_Cd",
                        "value_column": "Measure_Instance_Id",
                    },
                    "target": {"column": "Measure_Instance_Id"},
                }
            },
            data_columns=[],
            additional_columns=[],
            staging_table="staging_fact",
            staging_schema=None,
            fact_table="fact_result",
        )

    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT COUNT(1) AS row_count FROM fact_result")
        ).mappings().one()

    assert rows["row_count"] == 0


def test_load_fact_from_maps_honors_data_column_schema_overrides():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE staging_fact (Value TEXT, External_Row_Id TEXT)"))
        conn.execute(
            text(
                """
                INSERT INTO staging_fact (Value, External_Row_Id)
                VALUES ('10.5', 'row-1')
                """
            )
        )

    load_fact_from_maps(
        engine=engine,
        lookup_map={},
        data_columns=[
            {
                "column": "Value",
                "type": "NUMERIC(12, 2)",
                "nullable": False,
                "comment": "Actual inserted value",
            },
            {
                "column": "External_Row_Id",
                "type": "TEXT",
                "nullable": False,
                "unique": True,
            },
        ],
        additional_columns=[],
        staging_table="staging_fact",
        staging_schema=None,
        fact_table="fact_result",
    )

    with engine.connect() as conn:
        columns = conn.execute(text("PRAGMA table_info(fact_result)")).mappings().all()
        indexes = conn.execute(text("PRAGMA index_list(fact_result)")).mappings().all()

    column_info = {column["name"]: column for column in columns}
    assert column_info["Value"]["type"] == "NUMERIC(12, 2)"
    assert column_info["Value"]["notnull"] == 1
    assert column_info["External_Row_Id"]["type"] == "TEXT"
    assert column_info["External_Row_Id"]["notnull"] == 1
    assert any(
        index["name"] == "ux_fact_result_External_Row_Id" and index["unique"] == 1
        for index in indexes
    )


def test_load_fact_from_maps_enforces_unique_data_column():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE staging_fact (External_Row_Id TEXT)"))
        conn.execute(
            text(
                """
                INSERT INTO staging_fact (External_Row_Id)
                VALUES ('row-1'), ('row-1')
                """
            )
        )

    with pytest.raises(Exception):
        load_fact_from_maps(
            engine=engine,
            lookup_map={},
            data_columns=[
                {
                    "column": "External_Row_Id",
                    "type": "TEXT",
                    "unique": True,
                }
            ],
            additional_columns=[],
            staging_table="staging_fact",
            staging_schema=None,
            fact_table="fact_result",
        )


def test_load_fact_from_maps_append_mode_backfills_new_data_column():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE staging_fact (Value REAL, Quality_Flag TEXT)"))
        conn.execute(
            text(
                """
                INSERT INTO staging_fact (Value, Quality_Flag)
                VALUES (1.5, 'new'), (2.5, 'checked')
                """
            )
        )
        conn.execute(text("CREATE TABLE fact_result (Value REAL)"))
        conn.execute(text("INSERT INTO fact_result (Value) VALUES (1.5)"))

    load_fact_from_maps(
        engine=engine,
        lookup_map={},
        data_columns=[
            {"column": "Value"},
            {
                "column": "Quality_Flag",
                "type": "TEXT",
                "nullable": False,
                "backfill_existing_rows": "legacy",
            },
        ],
        additional_columns=[],
        staging_table="staging_fact",
        staging_schema=None,
        fact_table="fact_result",
        mode="append",
    )

    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT Value, Quality_Flag FROM fact_result ORDER BY Value")
        ).mappings().all()

    assert rows == [
        {"Value": 1.5, "Quality_Flag": "legacy"},
        {"Value": 1.5, "Quality_Flag": "new"},
        {"Value": 2.5, "Quality_Flag": "checked"},
    ]


def test_load_fact_from_maps_drops_and_recreates_existing_fact_table():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE staging_fact (Measure_Cd TEXT, Value REAL)"))
        conn.execute(
            text(
                """
                CREATE TABLE dim_measure (
                    Measure_Cd TEXT,
                    Measure_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO staging_fact (Measure_Cd, Value)
                VALUES ('MEASURE1', 1.5)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_measure (Measure_Cd, Measure_Instance_Id)
                VALUES ('MEASURE1', 200)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE fact_result (
                    old_col TEXT
                )
                """
            )
        )
        conn.execute(text("INSERT INTO fact_result (old_col) VALUES ('stale')"))

    load_fact_from_maps(
        engine=engine,
        lookup_map={
            "Measure_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_measure",
                    "lookup_column": "Measure_Cd",
                    "value_column": "Measure_Instance_Id",
                },
                "target": {
                    "column": "Measure_Instance_Id",
                    "comment": "Bar",
                },
            }
        },
        data_columns=[{"column": "Value", "comment": "Actual inserted value"}],
        additional_columns=[],
        staging_table="staging_fact",
        staging_schema="main",
        fact_table="fact_result",
        fact_schema="main",
    )

    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT Measure_Instance_Id, Value FROM fact_result")
        ).mappings().all()
        columns = conn.execute(text("PRAGMA table_info(fact_result)")).mappings().all()

    assert rows == [{"Measure_Instance_Id": 200, "Value": 1.5}]
    assert [column["name"] for column in columns] == ["Measure_Instance_Id", "Value"]


def test_load_fact_from_maps_creates_optional_fact_primary_key():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE staging_fact (Measure_Cd TEXT, Value REAL)"))
        conn.execute(
            text(
                """
                CREATE TABLE dim_measure (
                    Measure_Cd TEXT,
                    Measure_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO staging_fact (Measure_Cd, Value)
                VALUES ('MEASURE1', 1.5)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_measure (Measure_Cd, Measure_Instance_Id)
                VALUES ('MEASURE1', 200)
                """
            )
        )

    load_fact_from_maps(
        engine=engine,
        lookup_map={
            "Measure_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_measure",
                    "lookup_column": "Measure_Cd",
                    "value_column": "Measure_Instance_Id",
                },
                "target": {
                    "column": "Measure_Instance_Id",
                    "comment": "Bar",
                },
            }
        },
        data_columns=[{"column": "Value", "comment": "Actual inserted value"}],
        additional_columns=[],
        staging_table="staging_fact",
        staging_schema="main",
        fact_table="fact_result",
        fact_schema="main",
        fact_pk_column="fact_id",
    )

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT fact_id, Measure_Instance_Id, Value FROM fact_result")
        ).mappings().one()
        columns = conn.execute(text("PRAGMA table_info(fact_result)")).mappings().all()

    assert row == {"fact_id": 1, "Measure_Instance_Id": 200, "Value": 1.5}
    assert [column["name"] for column in columns if column["pk"]] == ["fact_id"]


def test_load_fact_from_maps_warns_for_missing_lookup_staging_column(capsys):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE staging_fact (Value REAL)"))
        conn.execute(
            text(
                """
                CREATE TABLE dim_measure (
                    Measure_Cd TEXT,
                    Measure_Instance_Id INTEGER
                )
                """
            )
        )

    load_fact_from_maps(
        engine=engine,
        lookup_map={
            "Measure_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_measure",
                    "lookup_column": "Measure_Cd",
                    "value_column": "Measure_Instance_Id",
                },
                "target": {
                    "column": "Measure_Instance_Id",
                    "comment": "Bar",
                },
            }
        },
        data_columns=[{"column": "Value", "comment": "Actual inserted value"}],
        additional_columns=[],
        staging_table="staging_fact",
        staging_schema="main",
        fact_table="fact_result",
        fact_schema="main",
    )

    captured = capsys.readouterr()
    assert "Warning: missing lookup staging columns" in captured.out
    assert "Measure_Cd" in captured.out

    with engine.connect() as conn:
        rows = conn.execute(text("SELECT Value FROM fact_result")).mappings().all()
        columns = conn.execute(text("PRAGMA table_info(fact_result)")).mappings().all()

    assert rows == []
    assert [column["name"] for column in columns] == ["Value"]


def test_load_fact_from_maps_uses_only_expected_lookup_columns():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE staging_fact (Measure_Cd TEXT, Value REAL)"))
        conn.execute(
            text(
                """
                CREATE TABLE dim_measure (
                    Measure_Cd TEXT,
                    Measure_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE dim_organisation (
                    Organisation_Cd TEXT,
                    Organisation_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO staging_fact (Measure_Cd, Value)
                VALUES ('MEASURE1', 1.5)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_measure (Measure_Cd, Measure_Instance_Id)
                VALUES ('MEASURE1', 200)
                """
            )
        )

    load_fact_from_maps(
        engine=engine,
        lookup_map={
            "Measure_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_measure",
                    "lookup_column": "Measure_Cd",
                    "value_column": "Measure_Instance_Id",
                },
                "target": {
                    "column": "Measure_Instance_Id",
                    "comment": "Bar",
                },
            },
            "Organisation_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_organisation",
                    "lookup_column": "Organisation_Cd",
                    "value_column": "Organisation_Instance_Id",
                },
                "target": {
                    "column": "Organisation_Instance_Id",
                    "comment": "Foo",
                },
            },
        },
        expected_lookup_columns=["Measure_Cd"],
        data_columns=[{"column": "Value", "comment": "Actual inserted value"}],
        additional_columns=[],
        staging_table="staging_fact",
        staging_schema="main",
        fact_table="fact_result",
        fact_schema="main",
    )

    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT Measure_Instance_Id, Value FROM fact_result")
        ).mappings().all()
        columns = conn.execute(text("PRAGMA table_info(fact_result)")).mappings().all()

    assert rows == [{"Measure_Instance_Id": 200, "Value": 1.5}]
    assert [column["name"] for column in columns] == ["Measure_Instance_Id", "Value"]


def test_load_fact_from_maps_uses_lookup_fallback_when_expected_column_missing():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE staging_fact (Value REAL)"))
        conn.execute(
            text(
                """
                CREATE TABLE dim_interval (
                    Interval_Cd TEXT,
                    Interval_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(text("INSERT INTO staging_fact (Value) VALUES (1.5)"))
        conn.execute(
            text(
                """
                INSERT INTO dim_interval (Interval_Cd, Interval_Instance_Id)
                VALUES ('UNKNOWN', 301)
                """
            )
        )

    load_fact_from_maps(
        engine=engine,
        lookup_map={
            "Submission_Period_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_interval",
                    "lookup_column": "Interval_Cd",
                    "value_column": "Interval_Instance_Id",
                },
                "target": {
                    "column": "Submission_Period_Interval_Instance_Id",
                    "comment": "Baz",
                },
                "fallbacks": {
                    "column_missing_in_staging": "UNKNOWN",
                    "backfill_existing_rows": "NOT_USED_YET",
                },
            }
        },
        expected_lookup_columns=["Submission_Period_Cd"],
        data_columns=[{"column": "Value", "comment": "Actual inserted value"}],
        additional_columns=[],
        staging_table="staging_fact",
        staging_schema="main",
        fact_table="fact_result",
        fact_schema="main",
    )

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT Submission_Period_Interval_Instance_Id, Value
                FROM fact_result
                """
            )
        ).mappings().all()

    assert rows == [
        {"Submission_Period_Interval_Instance_Id": 301, "Value": 1.5}
    ]


def test_load_fact_from_maps_raises_for_missing_expected_lookup_without_fallback():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE staging_fact (Value REAL)"))
        conn.execute(text("INSERT INTO staging_fact (Value) VALUES (1.5)"))

    with pytest.raises(
        ValueError,
        match="Missing required lookup staging columns in 'main.staging_fact': Submission_Period_Cd",
    ):
        load_fact_from_maps(
            engine=engine,
            lookup_map={
                "Submission_Period_Cd": {
                    "source": {
                        "schema": "main",
                        "table": "dim_interval",
                        "lookup_column": "Interval_Cd",
                        "value_column": "Interval_Instance_Id",
                    },
                    "target": {
                        "column": "Submission_Period_Interval_Instance_Id",
                        "comment": "Baz",
                    },
                }
            },
            expected_lookup_columns=["Submission_Period_Cd"],
            data_columns=[{"column": "Value", "comment": "Actual inserted value"}],
            additional_columns=[],
            staging_table="staging_fact",
            staging_schema="main",
            fact_table="fact_result",
            fact_schema="main",
        )


def test_load_fact_from_maps_raises_structured_error_for_missing_lookup_fallback():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE staging_fact (Value REAL)"))
        conn.execute(
            text(
                """
                CREATE TABLE dim_interval (
                    Interval_Cd TEXT,
                    Interval_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(text("INSERT INTO staging_fact (Value) VALUES (1.5)"))

    with pytest.raises(
        RuntimeError,
        match=(
            r"Missing dimension match in main.dim_interval "
            r"for lookup columns \['Submission_Period_Cd'\] -> dimension columns \['Interval_Cd'\].*"
            r"'Submission_Period_Cd': 'UNKNOWN'"
        ),
    ):
        load_fact_from_maps(
            engine=engine,
            lookup_map={
                "Submission_Period_Cd": {
                    "source": {
                        "schema": "main",
                        "table": "dim_interval",
                        "lookup_column": "Interval_Cd",
                        "value_column": "Interval_Instance_Id",
                    },
                    "target": {
                        "column": "Submission_Period_Interval_Instance_Id",
                        "comment": "Baz",
                    },
                    "fallbacks": {
                        "column_missing_in_staging": "UNKNOWN",
                        "backfill_existing_rows": "NOT_USED_YET",
                    },
                }
            },
            expected_lookup_columns=["Submission_Period_Cd"],
            data_columns=[{"column": "Value", "comment": "Actual inserted value"}],
            additional_columns=[],
            staging_table="staging_fact",
            staging_schema="main",
            fact_table="fact_result",
            fact_schema="main",
        )


def test_load_fact_from_maps_raises_structured_error_for_missing_dimension_lookup():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE staging_fact (
                    Measure_Cd TEXT,
                    Value REAL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE dim_measure (
                    Measure_Cd TEXT,
                    Measure_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO staging_fact (Measure_Cd, Value)
                VALUES ('UNKNOWN', 1.5)
                """
            )
        )

    with pytest.raises(
        RuntimeError,
        match=(
            r"Missing dimension match in main.dim_measure "
            r"for staging columns \['Measure_Cd'\] -> dimension columns \['Measure_Cd'\].*"
            r"'Measure_Cd': 'UNKNOWN'"
        ),
    ):
        load_fact_from_maps(
            engine=engine,
            lookup_map={
                "Measure_Cd": {
                    "source": {
                        "schema": "main",
                        "table": "dim_measure",
                        "lookup_column": "Measure_Cd",
                        "value_column": "Measure_Instance_Id",
                    },
                    "target": {
                        "column": "Measure_Instance_Id",
                        "comment": "Bar",
                    },
                }
            },
            data_columns=[{"column": "Value", "comment": "Actual inserted value"}],
            additional_columns=[],
            staging_table="staging_fact",
            staging_schema="main",
            fact_table="fact_result",
            fact_schema="main",
        )


def test_load_fact_from_maps_reports_all_missing_dimensions_in_one_error():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE staging_fact (
                    Currency_Pair_Cd TEXT,
                    Measure_Cd TEXT,
                    Value REAL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE dim_currency_pair (
                    Currency_Pair_Cd TEXT,
                    Currency_Pair_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE dim_measure (
                    Measure_Cd TEXT,
                    Measure_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO staging_fact (Currency_Pair_Cd, Measure_Cd, Value)
                VALUES ('CROSSTOT', 'FIN0075', 1.5)
                """
            )
        )

    with pytest.raises(RuntimeError) as exc_info:
        load_fact_from_maps(
            engine=engine,
            lookup_map={
                "Currency_Pair_Cd": {
                    "source": {
                        "schema": None,
                        "table": "dim_currency_pair",
                        "lookup_column": "Currency_Pair_Cd",
                        "value_column": "Currency_Pair_Instance_Id",
                    },
                    "target": {
                        "column": "Currency_Pair_Instance_Id",
                        "comment": "Currency pair instance",
                    },
                },
                "Measure_Cd": {
                    "source": {
                        "schema": None,
                        "table": "dim_measure",
                        "lookup_column": "Measure_Cd",
                        "value_column": "Measure_Instance_Id",
                    },
                    "target": {
                        "column": "Measure_Instance_Id",
                        "comment": "Measure instance",
                    },
                },
            },
            expected_lookup_columns=["Currency_Pair_Cd", "Measure_Cd"],
            data_columns=[{"column": "Value", "comment": "Actual inserted value"}],
            additional_columns=[],
            staging_table="staging_fact",
            staging_schema=None,
            fact_table="fact_result",
            fact_schema=None,
        )

    message = str(exc_info.value)
    assert message.count("Missing dimension match in") == 2
    assert "dim_currency_pair" in message
    assert "dim_measure" in message


def test_load_fact_from_maps_raises_for_missing_data_column():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE staging_fact (Measure_Cd TEXT)"))
        conn.execute(
            text(
                """
                CREATE TABLE dim_measure (
                    Measure_Cd TEXT,
                    Measure_Instance_Id INTEGER
                )
                """
            )
        )

    with pytest.raises(ValueError, match="Missing required data columns"):
        load_fact_from_maps(
            engine=engine,
            lookup_map={
                "Measure_Cd": {
                    "source": {
                        "schema": "main",
                        "table": "dim_measure",
                        "lookup_column": "Measure_Cd",
                        "value_column": "Measure_Instance_Id",
                    },
                    "target": {
                        "column": "Measure_Instance_Id",
                        "comment": "Bar",
                    },
                }
            },
            data_columns=[{"column": "Value", "comment": "Actual inserted value"}],
            additional_columns=[],
            staging_table="staging_fact",
            staging_schema="main",
            fact_table="fact_result",
            fact_schema="main",
        )


def test_load_fact_from_maps_warns_for_unused_staging_column(capsys):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE staging_fact (
                    Measure_Cd TEXT,
                    Value REAL,
                    Ignored_Column TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE dim_measure (
                    Measure_Cd TEXT,
                    Measure_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO staging_fact (Measure_Cd, Value, Ignored_Column)
                VALUES ('MEASURE1', 1.5, 'ignored')
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_measure (Measure_Cd, Measure_Instance_Id)
                VALUES ('MEASURE1', 200)
                """
            )
        )

    load_fact_from_maps(
        engine=engine,
        lookup_map={
            "Measure_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_measure",
                    "lookup_column": "Measure_Cd",
                    "value_column": "Measure_Instance_Id",
                },
                "target": {
                    "column": "Measure_Instance_Id",
                    "comment": "Bar",
                },
            }
        },
        data_columns=[{"column": "Value", "comment": "Actual inserted value"}],
        additional_columns=[],
        staging_table="staging_fact",
        staging_schema="main",
        fact_table="fact_result",
        fact_schema="main",
    )

    captured = capsys.readouterr()
    assert "Warning: unused staging columns" in captured.out
    assert "Ignored_Column" in captured.out


def test_load_fact_from_maps_filters_lookup_to_current_rows():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS facts"))
        conn.execute(
            text(
                """
                CREATE TABLE staging_fact (
                    Measure_Cd TEXT,
                    Value REAL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE dim_measure (
                    Measure_Cd TEXT,
                    Measure_Instance_Id INTEGER,
                    current_ind INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO staging_fact (Measure_Cd, Value)
                VALUES ('MEASURE1', 1.5)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_measure (Measure_Cd, Measure_Instance_Id, current_ind)
                VALUES ('MEASURE1', 100, 0), ('MEASURE1', 200, 1)
                """
            )
        )

    load_fact_from_maps(
        engine=engine,
        lookup_map={
            "Measure_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_measure",
                    "lookup_column": "Measure_Cd",
                    "value_column": "Measure_Instance_Id",
                },
                "target": {
                    "column": "Measure_Instance_Id",
                    "comment": "Bar",
                },
            }
        },
        data_columns=[{"column": "Value", "comment": "Actual inserted value"}],
        additional_columns=[],
        staging_table="staging_fact",
        staging_schema="main",
        fact_table="fact_result",
        fact_schema="facts",
    )

    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT Measure_Instance_Id, Value FROM facts.fact_result")
        ).mappings().all()

    assert rows == [{"Measure_Instance_Id": 200, "Value": 1.5}]


def test_load_fact_from_maps_filters_additional_columns_to_current_rows():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS facts"))
        conn.execute(
            text(
                """
                CREATE TABLE staging_fact (
                    Measure_Cd TEXT,
                    Value REAL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE dim_measure (
                    Measure_Cd TEXT,
                    Measure_Instance_Id INTEGER,
                    Measure_Name TEXT,
                    Current_Ind INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO staging_fact (Measure_Cd, Value)
                VALUES ('MEASURE1', 1.5)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_measure (
                    Measure_Cd,
                    Measure_Instance_Id,
                    Measure_Name,
                    Current_Ind
                )
                VALUES
                    ('MEASURE1', 200, 'Old Name', 0),
                    ('MEASURE1', 200, 'Current Name', 1)
                """
            )
        )

    load_fact_from_maps(
        engine=engine,
        lookup_map={
            "Measure_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_measure",
                    "lookup_column": "Measure_Cd",
                    "value_column": "Measure_Instance_Id",
                },
                "target": {
                    "column": "Measure_Instance_Id",
                    "comment": "Bar",
                },
            }
        },
        data_columns=[{"column": "Value", "comment": "Actual inserted value"}],
        additional_columns=[
            {
                "target": {
                    "column": "Measure_Name",
                    "comment": "Column brought from dim table",
                },
                "source": {
                    "schema": "main",
                    "table": "dim_measure",
                    "column": "Measure_Name",
                },
                "surrogate_keys": {
                    "fact": "Measure_Instance_Id",
                    "dim": "Measure_Instance_Id",
                },
            }
        ],
        staging_table="staging_fact",
        staging_schema="main",
        fact_table="fact_result",
        fact_schema="facts",
    )

    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT Measure_Instance_Id, Measure_Name FROM facts.fact_result")
        ).mappings().all()

    assert rows == [{"Measure_Instance_Id": 200, "Measure_Name": "Current Name"}]


def test_load_fact_from_maps_supports_metadata_columns_and_archive_filename(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    archive_dir = Path(".test_artifacts") / f"archive_{uuid.uuid4().hex}"
    archive_dir.mkdir(parents=True, exist_ok=True)

    try:
        with engine.begin() as conn:
            conn.execute(text("ATTACH DATABASE ':memory:' AS facts"))
            conn.execute(
                text(
                    """
                    CREATE TABLE staging_fact (
                        Measure_Cd TEXT,
                        Value REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE dim_measure (
                        Measure_Cd TEXT,
                        Measure_Instance_Id INTEGER
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO staging_fact (Measure_Cd, Value)
                    VALUES ('MEASURE1', 1.5)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO dim_measure (Measure_Cd, Measure_Instance_Id)
                    VALUES ('MEASURE1', 200)
                    """
                )
            )

        def fake_to_parquet(self, path, index=False):
            Path(path).write_bytes(b"parquet")

        monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet)

        load_fact_from_maps(
            engine=engine,
            lookup_map={
                "Measure_Cd": {
                    "source": {
                        "schema": "main",
                        "table": "dim_measure",
                        "lookup_column": "Measure_Cd",
                        "value_column": "Measure_Instance_Id",
                    },
                    "target": {
                        "column": "Measure_Instance_Id",
                        "comment": "Bar",
                    },
                }
            },
            data_columns=[{"column": "Value", "comment": "Actual inserted value"}],
            additional_columns=[],
            metadata_columns=[
                {
                    "target": {
                        "column": "Batch_Id",
                        "comment": "Pipeline batch identifier.",
                    },
                    "source": {
                        "kind": "parameter",
                        "name": "batch_id",
                    },
                },
                {
                    "target": {
                        "column": "Insert_Date",
                        "comment": "UTC insert timestamp.",
                    },
                    "source": {
                        "kind": "sql",
                        "expression": "CURRENT_TIMESTAMP",
                    },
                },
                {
                    "target": {
                        "column": "Archive_Filename",
                        "comment": "Archive file name for the snapshot.",
                    },
                    "source": {
                        "kind": "archive_filename",
                    },
                },
            ],
            runtime_values={"batch_id": "BATCH123"},
            staging_table="staging_fact",
            staging_schema="main",
            fact_table="fact_result",
            fact_schema="facts",
            archive_base_dir=str(archive_dir),
        )

        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT
                        Measure_Instance_Id,
                        Value,
                        Batch_Id,
                        Archive_Filename
                    FROM facts.fact_result
                    """
                )
            ).mappings().one()
            columns = conn.execute(
                text("PRAGMA facts.table_info(fact_result)")
            ).mappings().all()

        assert row["Measure_Instance_Id"] == 200
        assert row["Value"] == 1.5
        assert row["Batch_Id"] == "BATCH123"
        assert row["Archive_Filename"].startswith("staging_fact__")
        assert row["Archive_Filename"].endswith("__BATCHBATCH123.parquet")
        assert {column["name"]: column["notnull"] for column in columns}["Batch_Id"] == 1
        assert {column["name"]: column["notnull"] for column in columns}["Insert_Date"] == 1
        assert {column["name"]: column["notnull"] for column in columns}["Archive_Filename"] == 1
    finally:
        shutil.rmtree(archive_dir.parent, ignore_errors=True)


def test_load_fact_from_maps_skips_archive_when_archive_base_dir_is_none(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    archive_dir = Path(".test_artifacts") / f"archive_{uuid.uuid4().hex}"
    archive_dir.mkdir(parents=True, exist_ok=True)

    try:
        with engine.begin() as conn:
            conn.execute(text("ATTACH DATABASE ':memory:' AS facts"))
            conn.execute(
                text(
                    """
                    CREATE TABLE staging_fact (
                        Measure_Cd TEXT,
                        Value REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE dim_measure (
                        Measure_Cd TEXT,
                        Measure_Instance_Id INTEGER
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO staging_fact (Measure_Cd, Value)
                    VALUES ('MEASURE1', 1.5)
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO dim_measure (Measure_Cd, Measure_Instance_Id)
                    VALUES ('MEASURE1', 200)
                    """
                )
            )

        def fail_to_parquet(self, path, index=False):
            raise AssertionError("to_parquet should not be called when archive_base_dir is None")

        monkeypatch.setattr(pd.DataFrame, "to_parquet", fail_to_parquet)

        load_fact_from_maps(
            engine=engine,
            lookup_map={
                "Measure_Cd": {
                    "source": {
                        "schema": "main",
                        "table": "dim_measure",
                        "lookup_column": "Measure_Cd",
                        "value_column": "Measure_Instance_Id",
                    },
                    "target": {
                        "column": "Measure_Instance_Id",
                        "comment": "Bar",
                    },
                }
            },
            data_columns=[{"column": "Value", "comment": "Actual inserted value"}],
            additional_columns=[],
            metadata_columns=[
                {
                    "target": {
                        "column": "Archive_Filename",
                        "comment": "Archive file name for the snapshot.",
                    },
                    "source": {
                        "kind": "archive_filename",
                    },
                }
            ],
            runtime_values={"batch_id": "BATCH123"},
            staging_table="staging_fact",
            staging_schema="main",
            fact_table="fact_result",
            fact_schema="facts",
            archive_base_dir=None,
        )

        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT Measure_Instance_Id, Archive_Filename FROM facts.fact_result")
            ).mappings().one()
            columns = conn.execute(
                text("PRAGMA facts.table_info(fact_result)")
            ).mappings().all()

        assert row == {"Measure_Instance_Id": 200, "Archive_Filename": None}
        assert {column["name"]: column["notnull"] for column in columns}["Archive_Filename"] == 0
        assert list(archive_dir.iterdir()) == []
    finally:
        shutil.rmtree(archive_dir.parent, ignore_errors=True)


def test_load_fact_from_maps_append_mode_is_idempotent_for_same_batch_id():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS facts"))
        conn.execute(text("CREATE TABLE staging_fact (Measure_Cd TEXT, Value REAL)"))
        conn.execute(
            text(
                """
                CREATE TABLE dim_measure (
                    Measure_Cd TEXT,
                    Measure_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_measure (Measure_Cd, Measure_Instance_Id)
                VALUES ('MEASURE1', 200)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO staging_fact (Measure_Cd, Value)
                VALUES ('MEASURE1', 1.5)
                """
            )
        )

    kwargs = {
        "engine": engine,
        "lookup_map": {
            "Measure_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_measure",
                    "lookup_column": "Measure_Cd",
                    "value_column": "Measure_Instance_Id",
                },
                "target": {
                    "column": "Measure_Instance_Id",
                    "comment": "Bar",
                },
            }
        },
        "data_columns": [{"column": "Value", "comment": "Actual inserted value"}],
        "additional_columns": [],
        "metadata_columns": [
            {
                "target": {
                    "column": "Batch_Id",
                    "comment": "Pipeline batch identifier.",
                },
                "source": {
                    "kind": "parameter",
                    "name": "batch_id",
                },
            }
        ],
        "runtime_values": {"batch_id": "BATCH1"},
        "staging_table": "staging_fact",
        "staging_schema": "main",
        "fact_table": "fact_result",
        "fact_schema": "facts",
        "mode": "append",
    }

    load_fact_from_maps(**kwargs)
    load_fact_from_maps(**kwargs)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT Batch_Id, Measure_Instance_Id, Value
                FROM facts.fact_result
                ORDER BY Batch_Id
                """
            )
        ).mappings().all()

    assert rows == [
        {"Batch_Id": "BATCH1", "Measure_Instance_Id": 200, "Value": 1.5}
    ]


def test_load_fact_from_maps_append_mode_matches_metadata_columns_case_insensitively():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS facts"))
        conn.execute(text("CREATE TABLE staging_fact (Measure_Cd TEXT, Value REAL)"))
        conn.execute(
            text(
                """
                CREATE TABLE dim_measure (
                    Measure_Cd TEXT,
                    Measure_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE facts.fact_result (
                    Measure_Instance_Id INTEGER,
                    Value REAL,
                    batch_id TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_measure (Measure_Cd, Measure_Instance_Id)
                VALUES ('MEASURE1', 200)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO staging_fact (Measure_Cd, Value)
                VALUES ('MEASURE1', 1.5)
                """
            )
        )

    load_fact_from_maps(
        engine=engine,
        lookup_map={
            "Measure_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_measure",
                    "lookup_column": "Measure_Cd",
                    "value_column": "Measure_Instance_Id",
                },
                "target": {"column": "Measure_Instance_Id"},
            }
        },
        data_columns=[{"column": "Value"}],
        additional_columns=[],
        metadata_columns=[
            {
                "target": {"column": "Batch_Id"},
                "source": {"kind": "parameter", "name": "batch_id"},
            }
        ],
        runtime_values={"batch_id": "BATCH1"},
        staging_table="staging_fact",
        staging_schema="main",
        fact_table="fact_result",
        fact_schema="facts",
        mode="append",
    )

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT batch_id, Measure_Instance_Id, Value
                FROM facts.fact_result
                """
            )
        ).mappings().all()
        columns = conn.execute(text("PRAGMA facts.table_info(fact_result)")).fetchall()

    assert rows == [
        {"batch_id": "BATCH1", "Measure_Instance_Id": 200, "Value": 1.5}
    ]
    assert [column[1] for column in columns].count("batch_id") == 1


def test_load_fact_from_maps_append_mode_toggle_inserts_full_batch():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS facts"))
        conn.execute(text("CREATE TABLE staging_fact (Measure_Cd TEXT, Value REAL)"))
        conn.execute(
            text(
                """
                CREATE TABLE dim_measure (
                    Measure_Cd TEXT,
                    Measure_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_measure (Measure_Cd, Measure_Instance_Id)
                VALUES ('MEASURE1', 200)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO staging_fact (Measure_Cd, Value)
                VALUES ('MEASURE1', 1.5)
                """
            )
        )

    common_kwargs = {
        "engine": engine,
        "lookup_map": {
            "Measure_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_measure",
                    "lookup_column": "Measure_Cd",
                    "value_column": "Measure_Instance_Id",
                },
                "target": {
                    "column": "Measure_Instance_Id",
                    "comment": "Bar",
                },
            }
        },
        "data_columns": [{"column": "Value", "comment": "Actual inserted value"}],
        "additional_columns": [],
        "metadata_columns": [
            {
                "target": {
                    "column": "Batch_Id",
                    "comment": "Pipeline batch identifier.",
                },
                "source": {
                    "kind": "parameter",
                    "name": "batch_id",
                },
            }
        ],
        "staging_table": "staging_fact",
        "staging_schema": "main",
        "fact_table": "fact_result",
        "fact_schema": "facts",
        "mode": "append",
    }

    load_fact_from_maps(
        **common_kwargs,
        runtime_values={"batch_id": "BATCH1"},
        append_only_changed_rows=False,
    )
    with engine.begin() as conn:
        conn.execute(text("UPDATE staging_fact SET Value = 2.5"))
    load_fact_from_maps(
        **common_kwargs,
        runtime_values={"batch_id": "BATCH2"},
        append_only_changed_rows=False,
    )
    load_fact_from_maps(
        **common_kwargs,
        runtime_values={"batch_id": "BATCH3"},
        append_only_changed_rows=False,
    )

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT Batch_Id, Measure_Instance_Id, Value
                FROM facts.fact_result
                ORDER BY Batch_Id
                """
            )
        ).mappings().all()

    assert rows == [
        {"Batch_Id": "BATCH1", "Measure_Instance_Id": 200, "Value": 1.5},
        {"Batch_Id": "BATCH2", "Measure_Instance_Id": 200, "Value": 2.5},
    ]


def test_load_fact_from_maps_append_mode_inserts_changed_rows_across_batches():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS facts"))
        conn.execute(text("CREATE TABLE staging_fact (Measure_Cd TEXT, Value REAL)"))
        conn.execute(
            text(
                """
                CREATE TABLE dim_measure (
                    Measure_Cd TEXT,
                    Measure_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_measure (Measure_Cd, Measure_Instance_Id)
                VALUES ('MEASURE1', 200)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO staging_fact (Measure_Cd, Value)
                VALUES ('MEASURE1', 1.5)
                """
            )
        )

    common_kwargs = {
        "engine": engine,
        "lookup_map": {
            "Measure_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_measure",
                    "lookup_column": "Measure_Cd",
                    "value_column": "Measure_Instance_Id",
                },
                "target": {
                    "column": "Measure_Instance_Id",
                    "comment": "Bar",
                },
            }
        },
        "data_columns": [{"column": "Value", "comment": "Actual inserted value"}],
        "additional_columns": [],
        "metadata_columns": [
            {
                "target": {
                    "column": "Batch_Id",
                    "comment": "Pipeline batch identifier.",
                },
                "source": {
                    "kind": "parameter",
                    "name": "batch_id",
                },
            }
        ],
        "staging_table": "staging_fact",
        "staging_schema": "main",
        "fact_table": "fact_result",
        "fact_schema": "facts",
        "mode": "append",
    }

    load_fact_from_maps(**common_kwargs, runtime_values={"batch_id": "BATCH1"})

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM staging_fact"))
        conn.execute(
            text(
                """
                INSERT INTO staging_fact (Measure_Cd, Value)
                VALUES ('MEASURE1', 2.5)
                """
            )
        )

    load_fact_from_maps(**common_kwargs, runtime_values={"batch_id": "BATCH2"})

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT Batch_Id, Measure_Instance_Id, Value
                FROM facts.fact_result
                ORDER BY Batch_Id
                """
            )
        ).mappings().all()

    assert rows == [
        {"Batch_Id": "BATCH1", "Measure_Instance_Id": 200, "Value": 1.5},
        {"Batch_Id": "BATCH2", "Measure_Instance_Id": 200, "Value": 2.5},
    ]


def test_load_fact_from_maps_append_mode_inserts_only_changed_rows():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS facts"))
        conn.execute(text("CREATE TABLE staging_fact (Measure_Cd TEXT, Value REAL)"))
        conn.execute(
            text(
                """
                CREATE TABLE dim_measure (
                    Measure_Cd TEXT,
                    Measure_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_measure (Measure_Cd, Measure_Instance_Id)
                VALUES ('MEASURE1', 200), ('MEASURE2', 201)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO staging_fact (Measure_Cd, Value)
                VALUES ('MEASURE1', 1.5), ('MEASURE2', 2.5)
                """
            )
        )

    kwargs = {
        "engine": engine,
        "lookup_map": {
            "Measure_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_measure",
                    "lookup_column": "Measure_Cd",
                    "value_column": "Measure_Instance_Id",
                },
                "target": {"column": "Measure_Instance_Id"},
            }
        },
        "data_columns": [{"column": "Value"}],
        "additional_columns": [],
        "metadata_columns": [
            {
                "target": {"column": "Batch_Id"},
                "source": {"kind": "parameter", "name": "batch_id"},
            }
        ],
        "staging_table": "staging_fact",
        "staging_schema": "main",
        "fact_table": "fact_result",
        "fact_schema": "facts",
        "mode": "append",
    }

    load_fact_from_maps(**kwargs, runtime_values={"batch_id": "BATCH1"})
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE staging_fact SET Value = 3.5 WHERE Measure_Cd = 'MEASURE2'")
        )
    load_fact_from_maps(**kwargs, runtime_values={"batch_id": "BATCH2"})

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT Batch_Id, Measure_Instance_Id, Value
                FROM facts.fact_result
                ORDER BY Batch_Id
                """
            )
        ).mappings().all()

    assert rows == [
        {"Batch_Id": "BATCH1", "Measure_Instance_Id": 200, "Value": 1.5},
        {"Batch_Id": "BATCH1", "Measure_Instance_Id": 201, "Value": 2.5},
        {"Batch_Id": "BATCH2", "Measure_Instance_Id": 201, "Value": 3.5},
    ]


def test_load_fact_from_maps_append_mode_uses_distinct_fallbacks_for_existing_and_new_rows():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS facts"))
        conn.execute(text("CREATE TABLE staging_fact (Measure_Cd TEXT, Value REAL)"))
        conn.execute(
            text(
                """
                CREATE TABLE dim_measure (
                    Measure_Cd TEXT,
                    Measure_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE dim_region (
                    Region_Cd TEXT,
                    Region_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_measure (Measure_Cd, Measure_Instance_Id)
                VALUES ('MEASURE1', 200)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_region (Region_Cd, Region_Instance_Id)
                VALUES ('NA', 300), ('UNKNOWN', 999)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO staging_fact (Measure_Cd, Value)
                VALUES ('MEASURE1', 1.5)
                """
            )
        )

    base_kwargs = {
        "engine": engine,
        "data_columns": [{"column": "Value", "comment": "Actual inserted value"}],
        "additional_columns": [],
        "metadata_columns": [
            {
                "target": {
                    "column": "Batch_Id",
                    "comment": "Pipeline batch identifier.",
                },
                "source": {
                    "kind": "parameter",
                    "name": "batch_id",
                },
            }
        ],
        "staging_table": "staging_fact",
        "staging_schema": "main",
        "fact_table": "fact_result",
        "fact_schema": "facts",
        "mode": "append",
    }

    load_fact_from_maps(
        **base_kwargs,
        lookup_map={
            "Measure_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_measure",
                    "lookup_column": "Measure_Cd",
                    "value_column": "Measure_Instance_Id",
                },
                "target": {
                    "column": "Measure_Instance_Id",
                    "comment": "Bar",
                },
            }
        },
        runtime_values={"batch_id": "BATCH1"},
    )

    load_fact_from_maps(
        **base_kwargs,
        lookup_map={
            "Measure_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_measure",
                    "lookup_column": "Measure_Cd",
                    "value_column": "Measure_Instance_Id",
                },
                "target": {
                    "column": "Measure_Instance_Id",
                    "comment": "Bar",
                },
            },
            "Region_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_region",
                    "lookup_column": "Region_Cd",
                    "value_column": "Region_Instance_Id",
                },
                "target": {
                    "column": "Region_Instance_Id",
                    "comment": "Region surrogate key",
                },
                "fallbacks": {
                    "column_missing_in_staging": "NA",
                    "backfill_existing_rows": "UNKNOWN",
                },
            },
        },
        expected_lookup_columns=["Measure_Cd", "Region_Cd"],
        runtime_values={"batch_id": "BATCH2"},
    )

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT Batch_Id, Measure_Instance_Id, Region_Instance_Id, Value
                FROM facts.fact_result
                ORDER BY Batch_Id
                """
            )
        ).mappings().all()
        columns = conn.execute(
            text("PRAGMA facts.table_info(fact_result)")
        ).mappings().all()

    assert rows == [
        {
            "Batch_Id": "BATCH1",
            "Measure_Instance_Id": 200,
            "Region_Instance_Id": 999,
            "Value": 1.5,
        },
        {
            "Batch_Id": "BATCH2",
            "Measure_Instance_Id": 200,
            "Region_Instance_Id": 300,
            "Value": 1.5,
        },
    ]
    assert "Region_Instance_Id" in {column["name"] for column in columns}


def test_load_fact_from_maps_append_mode_backfills_only_newly_added_lookup_columns():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS facts"))
        conn.execute(
            text("CREATE TABLE staging_fact (Organisation_Cd TEXT, Measure_Cd TEXT, Value REAL)")
        )
        conn.execute(
            text(
                """
                CREATE TABLE dim_measure (
                    Measure_Cd TEXT,
                    Measure_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE dim_organisation (
                    Organisation_Cd TEXT,
                    Organisation_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE dim_region (
                    Region_Cd TEXT,
                    Region_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_measure (Measure_Cd, Measure_Instance_Id)
                VALUES ('MEASURE1', 200)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_organisation (Organisation_Cd, Organisation_Instance_Id)
                VALUES ('ORG1', 500)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_region (Region_Cd, Region_Instance_Id)
                VALUES ('NA', 300), ('UNKNOWN', 999)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO staging_fact (Organisation_Cd, Measure_Cd, Value)
                VALUES ('ORG1', 'MEASURE1', 1.5)
                """
            )
        )

    base_kwargs = {
        "engine": engine,
        "data_columns": [{"column": "Value", "comment": "Actual inserted value"}],
        "additional_columns": [],
        "metadata_columns": [
            {
                "target": {
                    "column": "Batch_Id",
                    "comment": "Pipeline batch identifier.",
                },
                "source": {
                    "kind": "parameter",
                    "name": "batch_id",
                },
            }
        ],
        "staging_table": "staging_fact",
        "staging_schema": "main",
        "fact_table": "fact_result",
        "fact_schema": "facts",
        "mode": "append",
    }

    load_fact_from_maps(
        **base_kwargs,
        lookup_map={
            "Organisation_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_organisation",
                    "lookup_column": "Organisation_Cd",
                    "value_column": "Organisation_Instance_Id",
                },
                "target": {
                    "column": "Organisation_Instance_Id",
                    "comment": "Foo",
                },
                "fallbacks": {
                    "backfill_existing_rows": "UNKNOWN",
                },
            },
            "Measure_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_measure",
                    "lookup_column": "Measure_Cd",
                    "value_column": "Measure_Instance_Id",
                },
                "target": {
                    "column": "Measure_Instance_Id",
                    "comment": "Bar",
                },
            },
        },
        expected_lookup_columns=["Organisation_Cd", "Measure_Cd"],
        runtime_values={"batch_id": "BATCH1"},
    )

    load_fact_from_maps(
        **base_kwargs,
        lookup_map={
            "Organisation_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_organisation",
                    "lookup_column": "Organisation_Cd",
                    "value_column": "Organisation_Instance_Id",
                },
                "target": {
                    "column": "Organisation_Instance_Id",
                    "comment": "Foo",
                },
                "fallbacks": {
                    "backfill_existing_rows": "UNKNOWN",
                },
            },
            "Measure_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_measure",
                    "lookup_column": "Measure_Cd",
                    "value_column": "Measure_Instance_Id",
                },
                "target": {
                    "column": "Measure_Instance_Id",
                    "comment": "Bar",
                },
            },
            "Region_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_region",
                    "lookup_column": "Region_Cd",
                    "value_column": "Region_Instance_Id",
                },
                "target": {
                    "column": "Region_Instance_Id",
                    "comment": "Region surrogate key",
                },
                "fallbacks": {
                    "column_missing_in_staging": "NA",
                    "backfill_existing_rows": "UNKNOWN",
                },
            },
        },
        expected_lookup_columns=["Organisation_Cd", "Measure_Cd", "Region_Cd"],
        runtime_values={"batch_id": "BATCH2"},
    )

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    Batch_Id,
                    Organisation_Instance_Id,
                    Measure_Instance_Id,
                    Region_Instance_Id,
                    Value
                FROM facts.fact_result
                ORDER BY Batch_Id
                """
            )
        ).mappings().all()

    assert rows == [
        {
            "Batch_Id": "BATCH1",
            "Organisation_Instance_Id": 500,
            "Measure_Instance_Id": 200,
            "Region_Instance_Id": 999,
            "Value": 1.5,
        },
        {
            "Batch_Id": "BATCH2",
            "Organisation_Instance_Id": 500,
            "Measure_Instance_Id": 200,
            "Region_Instance_Id": 300,
            "Value": 1.5,
        },
    ]


def test_load_fact_from_maps_append_mode_does_not_backfill_existing_lookup_columns():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS facts"))
        conn.execute(
            text("CREATE TABLE staging_fact (Organisation_Cd TEXT, Measure_Cd TEXT, Value REAL)")
        )
        conn.execute(
            text(
                """
                CREATE TABLE dim_measure (
                    Measure_Cd TEXT,
                    Measure_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE dim_organisation (
                    Organisation_Cd TEXT,
                    Organisation_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE dim_region (
                    Region_Cd TEXT,
                    Region_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_measure (Measure_Cd, Measure_Instance_Id)
                VALUES ('MEASURE1', 200)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_organisation (Organisation_Cd, Organisation_Instance_Id)
                VALUES ('ORG1', 500)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_region (Region_Cd, Region_Instance_Id)
                VALUES ('NA', 300), ('UNKNOWN', 999)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO staging_fact (Organisation_Cd, Measure_Cd, Value)
                VALUES ('ORG1', 'MEASURE1', 1.5)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE facts.fact_result (
                    Organisation_Instance_Id INTEGER,
                    Measure_Instance_Id INTEGER NOT NULL,
                    Batch_Id TEXT NOT NULL,
                    Value REAL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO facts.fact_result (
                    Organisation_Instance_Id,
                    Measure_Instance_Id,
                    Batch_Id,
                    Value
                )
                VALUES (NULL, 200, 'BATCH1', 1.5)
                """
            )
        )

    base_kwargs = {
        "engine": engine,
        "data_columns": [{"column": "Value", "comment": "Actual inserted value"}],
        "additional_columns": [],
        "metadata_columns": [
            {
                "target": {
                    "column": "Batch_Id",
                    "comment": "Pipeline batch identifier.",
                },
                "source": {
                    "kind": "parameter",
                    "name": "batch_id",
                },
            }
        ],
        "staging_table": "staging_fact",
        "staging_schema": "main",
        "fact_table": "fact_result",
        "fact_schema": "facts",
        "mode": "append",
    }

    base_lookup_map = {
        "Organisation_Cd": {
            "source": {
                "schema": "main",
                "table": "dim_organisation",
                "lookup_column": "Organisation_Cd",
                "value_column": "Organisation_Instance_Id",
            },
            "target": {
                "column": "Organisation_Instance_Id",
                "comment": "Foo",
            },
            "fallbacks": {
                "backfill_existing_rows": "UNKNOWN",
            },
        },
        "Measure_Cd": {
            "source": {
                "schema": "main",
                "table": "dim_measure",
                "lookup_column": "Measure_Cd",
                "value_column": "Measure_Instance_Id",
            },
            "target": {
                "column": "Measure_Instance_Id",
                "comment": "Bar",
            },
        },
    }

    load_fact_from_maps(
        **base_kwargs,
        lookup_map={
            **base_lookup_map,
            "Region_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_region",
                    "lookup_column": "Region_Cd",
                    "value_column": "Region_Instance_Id",
                },
                "target": {
                    "column": "Region_Instance_Id",
                    "comment": "Region surrogate key",
                },
                "fallbacks": {
                    "column_missing_in_staging": "NA",
                    "backfill_existing_rows": "UNKNOWN",
                },
            },
        },
        expected_lookup_columns=["Organisation_Cd", "Measure_Cd", "Region_Cd"],
        runtime_values={"batch_id": "BATCH2"},
    )

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    Batch_Id,
                    Organisation_Instance_Id,
                    Measure_Instance_Id,
                    Region_Instance_Id,
                    Value
                FROM facts.fact_result
                ORDER BY Batch_Id
                """
            )
        ).mappings().all()

    assert rows == [
        {
            "Batch_Id": "BATCH1",
            "Organisation_Instance_Id": None,
            "Measure_Instance_Id": 200,
            "Region_Instance_Id": 999,
            "Value": 1.5,
        },
        {
            "Batch_Id": "BATCH2",
            "Organisation_Instance_Id": 500,
            "Measure_Instance_Id": 200,
            "Region_Instance_Id": 300,
            "Value": 1.5,
        },
    ]


def test_load_fact_from_maps_append_mode_populates_retained_lookup_column():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with engine.begin() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS facts"))
        conn.execute(
            text("CREATE TABLE staging_fact (Measure_Cd TEXT, Value REAL)")
        )
        conn.execute(
            text(
                "CREATE TABLE dim_measure "
                "(Measure_Cd TEXT, Measure_Instance_Id INTEGER)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE dim_region "
                "(Region_Cd TEXT, Region_Instance_Id INTEGER)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO dim_measure (Measure_Cd, Measure_Instance_Id) "
                "VALUES ('MEASURE1', 200)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO dim_region (Region_Cd, Region_Instance_Id) "
                "VALUES ('NA', 300)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO staging_fact (Measure_Cd, Value) "
                "VALUES ('MEASURE1', 1.5)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE facts.fact_result ("
                "Measure_Instance_Id INTEGER, "
                "Region_Instance_Id INTEGER, "
                "Batch_Id TEXT NOT NULL, "
                "Value REAL)"
            )
        )

    load_fact_from_maps(
        engine=engine,
        lookup_map={
            "Measure_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_measure",
                    "lookup_column": "Measure_Cd",
                    "value_column": "Measure_Instance_Id",
                },
                "target": {
                    "column": "Measure_Instance_Id",
                    "comment": "Measure key",
                },
            },
            "Region_Cd": {
                "source": {
                    "schema": "main",
                    "table": "dim_region",
                    "lookup_column": "Region_Cd",
                    "value_column": "Region_Instance_Id",
                },
                "target": {
                    "column": "Region_Instance_Id",
                    "comment": "Region key",
                },
                "fallbacks": {"column_missing_in_staging": "NA"},
            },
        },
        data_columns=[{"column": "Value", "comment": "Value"}],
        additional_columns=[],
        metadata_columns=[
            {
                "target": {"column": "Batch_Id", "comment": "Batch"},
                "source": {"kind": "parameter", "name": "batch_id"},
            }
        ],
        runtime_values={"batch_id": "BATCH1"},
        expected_lookup_columns=["Measure_Cd"],
        staging_table="staging_fact",
        staging_schema="main",
        fact_table="fact_result",
        fact_schema="facts",
        mode="append",
    )

    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT Measure_Instance_Id, Region_Instance_Id, Batch_Id, Value "
                "FROM facts.fact_result"
            )
        ).mappings().one()

    assert row == {
        "Measure_Instance_Id": 200,
        "Region_Instance_Id": 300,
        "Batch_Id": "BATCH1",
        "Value": 1.5,
    }


def test_apply_comments_executes_for_mssql():
    class _FakeDialect:
        name = "mssql"

    class _FakeConn:
        def __init__(self):
            self.calls = []

        def execute(self, stmt, params=None):
            self.calls.append((str(stmt), params))

    class _FakeBegin:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self.conn

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeEngine:
        def __init__(self):
            self.dialect = _FakeDialect()
            self.conn = _FakeConn()

        def begin(self):
            return _FakeBegin(self.conn)

    engine = _FakeEngine()

    fact_map_loader_module._apply_comments(
        engine,
        schema="Facts",
        table="fact_result",
        table_comment="Fact description",
        column_comments={"Value": "Actual inserted value"},
    )

    assert len(engine.conn.calls) == 1
    sql, params = engine.conn.calls[0]
    assert "sp_updateextendedproperty" in sql
    assert "sp_addextendedproperty" in sql
    assert params["schema"] == "Facts"
    assert params["table"] == "fact_result"


def test_fact_pk_clause_uses_identity_for_mssql():
    class _FakeDialect:
        name = "mssql"

    class _FakeEngine:
        dialect = _FakeDialect()

    assert fact_map_loader_module._fact_pk_clause(_FakeEngine(), "fact_id") == (
        "[fact_id] INT IDENTITY(1,1) PRIMARY KEY"
    )
