from sqlalchemy import create_engine, text

import pytest

import dataprepkit.fact_map_loader as fact_map_loader_module
from dataprepkit.fact_map_loader import load_fact_from_maps


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
