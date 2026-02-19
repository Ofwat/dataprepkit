import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.mssql import DATETIME2
from sqlalchemy.exc import ProgrammingError

from dataprepkit.metadata_loader import (
    DimensionMetadata,
    METADATA_REGISTRY,
    get_metadata,
    register_metadata,
    run_dimension,
)
from dataprepkit.helpers.staging import stage_dataframe, union_tables_by_name_regex
from dataprepkit.helpers.staging import drop_tables_by_name_regex
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


def test_stage_dataframe_mssql_parses_bracketed_qualified_names(monkeypatch):
    class _FakeDialect:
        name = "mssql"

    class _FakeEngine:
        dialect = _FakeDialect()

    engine = _FakeEngine()
    df = pd.DataFrame({"col": [1]})
    calls = []
    captured = {}

    def fake_ensure(engine_arg, schema_arg):
        calls.append((engine_arg, schema_arg))

    def fake_to_sql(self, name, con, if_exists, index, schema):
        captured.update(
            {
                "name": name,
                "con": con,
                "if_exists": if_exists,
                "index": index,
                "schema": schema,
            }
        )

    monkeypatch.setattr("dataprepkit.helpers.staging.ensure_schema_exists", fake_ensure)
    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql)

    stage_dataframe(engine, "[my[schema]]].[ta]]ble[]", df)

    assert calls == [(engine, "my[schema]")]
    assert captured == {
        "name": "ta]ble[",
        "con": engine,
        "if_exists": "replace",
        "index": False,
        "schema": "my[schema]",
    }


def test_stage_dataframe_mssql_normalizes_bracketed_schema_argument(monkeypatch):
    class _FakeDialect:
        name = "mssql"

    class _FakeEngine:
        dialect = _FakeDialect()

    engine = _FakeEngine()
    df = pd.DataFrame({"col": [1]})
    calls = []
    captured = {}

    def fake_ensure(engine_arg, schema_arg):
        calls.append((engine_arg, schema_arg))

    def fake_to_sql(self, name, con, if_exists, index, schema):
        captured.update({"name": name, "schema": schema})

    monkeypatch.setattr("dataprepkit.helpers.staging.ensure_schema_exists", fake_ensure)
    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql)

    stage_dataframe(engine, "[ta]]ble[]", df, schema="[my[schema]]]")

    assert calls == [(engine, "my[schema]")]
    assert captured == {"name": "ta]ble[", "schema": "my[schema]"}


def test_stage_dataframe_mssql_preserves_literal_brackets_in_schema(monkeypatch):
    class _FakeDialect:
        name = "mssql"

    class _FakeEngine:
        dialect = _FakeDialect()

    engine = _FakeEngine()
    df = pd.DataFrame({"col": [1]})
    captured = {}
    calls = []

    def fake_ensure(engine_arg, schema_arg):
        calls.append((engine_arg, schema_arg))

    def fake_to_sql(self, name, con, if_exists, index, schema):
        captured.update({"name": name, "schema": schema})

    monkeypatch.setattr("dataprepkit.helpers.staging.ensure_schema_exists", fake_ensure)
    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql)

    stage_dataframe(
        engine,
        "afw_pcd_validation_stg",
        df,
        schema="Staging OFFICIAL - SENSITIVE [MARKET SENSITIVE]",
    )

    assert str(captured["schema"]) == "Staging OFFICIAL - SENSITIVE [MARKET SENSITIVE]"
    assert str(captured["name"]) == "afw_pcd_validation_stg"
    assert getattr(captured["schema"], "quote", None) is True
    assert getattr(captured["name"], "quote", None) is True
    assert calls == [(engine, "Staging OFFICIAL - SENSITIVE [MARKET SENSITIVE]")]


def test_stage_dataframe_mssql_datetime_columns_use_datetime2(monkeypatch):
    class _FakeDialect:
        name = "mssql"

    class _FakeEngine:
        dialect = _FakeDialect()

    engine = _FakeEngine()
    df = pd.DataFrame(
        {
            "id": [1],
            "start_ts": [pd.Timestamp("2026-01-01 10:00:00")],
            "end_ts": [pd.Timestamp("2026-01-01 11:00:00")],
        }
    )
    captured = {}

    def fake_to_sql(self, name, con, if_exists, index, schema, dtype=None):
        captured["dtype"] = dtype

    monkeypatch.setattr("dataprepkit.helpers.staging.ensure_schema_exists", lambda *_: None)
    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql)

    stage_dataframe(engine, "validation_summary", df)

    dtype = captured["dtype"]
    assert isinstance(dtype["start_ts"], DATETIME2)
    assert isinstance(dtype["end_ts"], DATETIME2)
    assert dtype["start_ts"].precision == 3
    assert dtype["end_ts"].precision == 3
    assert "id" not in dtype


def test_stage_dataframe_copy_into_requires_parquet_base_dir():
    class _FakeDialect:
        name = "mssql"

    class _FakeEngine:
        dialect = _FakeDialect()

    with pytest.raises(ValueError):
        stage_dataframe(
            _FakeEngine(),
            "stage_table",
            pd.DataFrame({"col": [1]}),
            use_copy_into_parquet=True,
        )


def test_stage_dataframe_copy_into_writes_parquet_and_executes_copy(monkeypatch, tmp_path):
    class _FakeDialect:
        name = "mssql"

    class _FakeConn:
        def __init__(self):
            self.calls = []

        def execute(self, statement, params=None):
            self.calls.append((str(statement), dict(params or {})))

    class _FakeBegin:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self.conn

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeEngine:
        dialect = _FakeDialect()

        def __init__(self):
            self.conn = _FakeConn()

        def begin(self):
            return _FakeBegin(self.conn)

    written = {}

    def fake_to_parquet(self, path, index=False):
        written["path"] = str(path)
        written["index"] = index

    class _FakeInspector:
        @staticmethod
        def has_table(_table, schema=None):
            return True

    monkeypatch.setattr("dataprepkit.helpers.staging.ensure_schema_exists", lambda *_: None)
    monkeypatch.setattr("dataprepkit.helpers.staging.inspect", lambda _: _FakeInspector())
    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet)
    engine = _FakeEngine()

    stage_dataframe(
        engine,
        "stage_table",
        pd.DataFrame({"col": [1]}),
        schema="dbo",
        use_copy_into_parquet=True,
        parquet_base_dir=str(tmp_path),
        if_exists="replace",
    )

    assert written["path"].endswith(".parquet")
    assert written["index"] is False
    assert "BATCHstage_" in written["path"]
    assert len(engine.conn.calls) == 2
    truncate_sql, truncate_params = engine.conn.calls[0]
    copy_sql, copy_params = engine.conn.calls[1]
    assert "TRUNCATE TABLE [dbo].[stage_table]" in truncate_sql
    assert truncate_params == {}
    assert "INSERT INTO [dbo].[stage_table] ([col])" in copy_sql
    assert "FROM OPENROWSET(" in copy_sql
    assert "FORMAT = 'PARQUET'" in copy_sql
    assert f"BULK '{str(tmp_path).rstrip('/')}/stage_table/" in copy_sql
    assert "/*.parquet'" in copy_sql
    assert copy_params == {}
    assert copy_sql.startswith("\n                    INSERT INTO [dbo].[stage_table]")


def test_stage_dataframe_copy_into_accepts_separate_source_base_url(monkeypatch, tmp_path):
    class _FakeDialect:
        name = "mssql"

    class _FakeConn:
        def __init__(self):
            self.calls = []

        def execute(self, statement, params=None):
            self.calls.append((str(statement), dict(params or {})))

    class _FakeBegin:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self.conn

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeEngine:
        dialect = _FakeDialect()

        def __init__(self):
            self.conn = _FakeConn()

        def begin(self):
            return _FakeBegin(self.conn)

    class _FakeInspector:
        @staticmethod
        def has_table(_table, schema=None):
            return True

    monkeypatch.setattr("dataprepkit.helpers.staging.ensure_schema_exists", lambda *_: None)
    monkeypatch.setattr("dataprepkit.helpers.staging.inspect", lambda _: _FakeInspector())
    monkeypatch.setattr(pd.DataFrame, "to_parquet", lambda self, path, index=False: None)
    engine = _FakeEngine()

    stage_dataframe(
        engine,
        "stage_table",
        pd.DataFrame({"col": [1]}),
        schema="dbo",
        use_copy_into_parquet=True,
        parquet_base_dir=str(tmp_path),
        copy_source_base_url="abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Files/tmp",
        if_exists="append",
    )

    sql, _ = engine.conn.calls[0]
    assert "BULK 'abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Files/tmp/stage_table/" in sql
    assert "/*.parquet'" in sql


def test_stage_dataframe_copy_into_splits_into_multiple_parquet_parts(monkeypatch, tmp_path):
    class _FakeDialect:
        name = "mssql"

    class _FakeConn:
        def __init__(self):
            self.calls = []

        def execute(self, statement, params=None):
            self.calls.append((str(statement), dict(params or {})))

    class _FakeBegin:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self.conn

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeEngine:
        dialect = _FakeDialect()

        def __init__(self):
            self.conn = _FakeConn()

        def begin(self):
            return _FakeBegin(self.conn)

    class _FakeInspector:
        @staticmethod
        def has_table(_table, schema=None):
            return True

    writes = []

    def fake_to_parquet(self, path, index=False):
        writes.append((str(path), len(self), index))

    monkeypatch.setattr("dataprepkit.helpers.staging.ensure_schema_exists", lambda *_: None)
    monkeypatch.setattr("dataprepkit.helpers.staging.inspect", lambda _: _FakeInspector())
    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet)
    engine = _FakeEngine()

    stage_dataframe(
        engine,
        "stage_table",
        pd.DataFrame({"col": [1, 2, 3]}),
        schema="dbo",
        use_copy_into_parquet=True,
        parquet_base_dir=str(tmp_path),
        if_exists="append",
        openrowset_max_rows_per_file=1,
    )

    assert len(writes) == 3
    assert all("part-" in path for path, _rows, _index in writes)
    assert all(rows == 1 for _path, rows, _index in writes)
    sql, _ = engine.conn.calls[0]
    assert "/*.parquet'" in sql


def test_stage_dataframe_copy_into_normalizes_mixed_object_columns(monkeypatch, tmp_path):
    class _FakeDialect:
        name = "mssql"

    class _FakeConn:
        def __init__(self):
            self.calls = []

        def execute(self, statement, params=None):
            self.calls.append((str(statement), dict(params or {})))

    class _FakeBegin:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self.conn

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeEngine:
        dialect = _FakeDialect()

        def __init__(self):
            self.conn = _FakeConn()

        def begin(self):
            return _FakeBegin(self.conn)

    class _FakeInspector:
        @staticmethod
        def has_table(_table, schema=None):
            return True

    captured = {}

    def fake_to_parquet(self, path, index=False):
        captured["values"] = self["Site_Cd"].tolist()

    monkeypatch.setattr("dataprepkit.helpers.staging.ensure_schema_exists", lambda *_: None)
    monkeypatch.setattr("dataprepkit.helpers.staging.inspect", lambda _: _FakeInspector())
    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet)
    engine = _FakeEngine()

    stage_dataframe(
        engine,
        "stage_table",
        pd.DataFrame({"Site_Cd": [b"A1", 10, None]}),
        schema="dbo",
        use_copy_into_parquet=True,
        parquet_base_dir=str(tmp_path),
        if_exists="append",
    )

    assert captured["values"] == ["A1", "10", None]


def test_stage_dataframe_copy_into_normalizes_datetime_columns(monkeypatch, tmp_path):
    class _FakeDialect:
        name = "mssql"

    class _FakeConn:
        def __init__(self):
            self.calls = []

        def execute(self, statement, params=None):
            self.calls.append((str(statement), dict(params or {})))

    class _FakeBegin:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self.conn

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeEngine:
        dialect = _FakeDialect()

        def __init__(self):
            self.conn = _FakeConn()

        def begin(self):
            return _FakeBegin(self.conn)

    class _FakeInspector:
        @staticmethod
        def has_table(_table, schema=None):
            return True

    captured = {}

    def fake_to_parquet(self, path, index=False):
        captured["values"] = self["Interval_Start_Date"].tolist()

    monkeypatch.setattr("dataprepkit.helpers.staging.ensure_schema_exists", lambda *_: None)
    monkeypatch.setattr("dataprepkit.helpers.staging.inspect", lambda _: _FakeInspector())
    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet)
    engine = _FakeEngine()

    stage_dataframe(
        engine,
        "stage_table",
        pd.DataFrame(
            {
                "Interval_Start_Date": [
                    pd.Timestamp("2026-01-01 10:30:00.123"),
                    pd.NaT,
                ]
            }
        ),
        schema="dbo",
        use_copy_into_parquet=True,
        parquet_base_dir=str(tmp_path),
        if_exists="append",
    )

    assert captured["values"][0].startswith("2026-01-01 10:30:00.")
    assert captured["values"][1] is None


def test_stage_dataframe_copy_into_creates_missing_table(monkeypatch, tmp_path):
    class _FakeDialect:
        name = "mssql"

    class _FakeConn:
        def __init__(self):
            self.calls = []

        def execute(self, statement, params=None):
            self.calls.append((str(statement), dict(params or {})))

    class _FakeBegin:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self.conn

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeEngine:
        dialect = _FakeDialect()

        def __init__(self):
            self.conn = _FakeConn()

        def begin(self):
            return _FakeBegin(self.conn)

    class _FakeInspector:
        @staticmethod
        def has_table(_table, schema=None):
            return False

    captured = {"create_calls": 0}
    monkeypatch.setattr("dataprepkit.helpers.staging.ensure_schema_exists", lambda *_: None)
    monkeypatch.setattr("dataprepkit.helpers.staging.inspect", lambda _: _FakeInspector())
    monkeypatch.setattr(pd.DataFrame, "to_parquet", lambda self, path, index=False: None)

    def fake_to_sql(self, name, con, if_exists, index, schema, dtype=None):
        captured["create_calls"] += 1
        captured["if_exists"] = if_exists

    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql)
    engine = _FakeEngine()

    stage_dataframe(
        engine,
        "stage_table",
        pd.DataFrame({"col": [1]}),
        schema="dbo",
        use_copy_into_parquet=True,
        parquet_base_dir=str(tmp_path),
        if_exists="replace",
    )

    assert captured["create_calls"] == 1
    assert captured["if_exists"] == "fail"
    assert len(engine.conn.calls) == 1
    assert "INSERT INTO [dbo].[stage_table] ([col])" in engine.conn.calls[0][0]


def test_stage_dataframe_copy_into_falls_back_to_delete_on_truncate_error(monkeypatch, tmp_path):
    class _FakeDialect:
        name = "mssql"

    class _FakeConn:
        def __init__(self):
            self.calls = []

        def execute(self, statement, params=None):
            sql = str(statement)
            self.calls.append((sql, dict(params or {})))
            if "TRUNCATE TABLE" in sql:
                raise ProgrammingError("TRUNCATE", {}, Exception("denied"))

    class _FakeBegin:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self.conn

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeEngine:
        dialect = _FakeDialect()

        def __init__(self):
            self.conn = _FakeConn()

        def begin(self):
            return _FakeBegin(self.conn)

    class _FakeInspector:
        @staticmethod
        def has_table(_table, schema=None):
            return True

    monkeypatch.setattr("dataprepkit.helpers.staging.ensure_schema_exists", lambda *_: None)
    monkeypatch.setattr("dataprepkit.helpers.staging.inspect", lambda _: _FakeInspector())
    monkeypatch.setattr(pd.DataFrame, "to_parquet", lambda self, path, index=False: None)
    engine = _FakeEngine()

    stage_dataframe(
        engine,
        "stage_table",
        pd.DataFrame({"col": [1]}),
        schema="dbo",
        use_copy_into_parquet=True,
        parquet_base_dir=str(tmp_path),
        if_exists="replace",
    )

    assert any("TRUNCATE TABLE [dbo].[stage_table]" in call[0] for call in engine.conn.calls)
    assert any("DELETE FROM [dbo].[stage_table]" in call[0] for call in engine.conn.calls)
    assert any("FROM OPENROWSET(" in call[0] for call in engine.conn.calls)
    assert any("INSERT INTO [dbo].[stage_table] ([col])" in call[0] for call in engine.conn.calls)


def test_union_tables_by_name_regex_unions_all_matches():
    engine = create_engine("sqlite:///:memory:")
    stage_dataframe(engine, "stg_sales_1", pd.DataFrame({"id": [1, 2]}))
    stage_dataframe(engine, "stg_sales_2", pd.DataFrame({"id": [3]}))

    union_tables_by_name_regex(engine, None, r"^stg_sales_\d+$", "stg_sales_all")

    with engine.connect() as conn:
        rows = conn.execute(text("SELECT id FROM stg_sales_all ORDER BY id")).fetchall()
    assert rows == [(1,), (2,), (3,)]


def test_union_tables_by_name_regex_overwrites_existing_output():
    engine = create_engine("sqlite:///:memory:")
    stage_dataframe(engine, "stg_src_1", pd.DataFrame({"id": [10]}))
    stage_dataframe(engine, "stg_src_2", pd.DataFrame({"id": [20]}))
    stage_dataframe(engine, "stg_output", pd.DataFrame({"id": [999]}))

    union_tables_by_name_regex(engine, None, r"^stg_src_\d+$", "stg_output")

    with engine.connect() as conn:
        rows = conn.execute(text("SELECT id FROM stg_output ORDER BY id")).fetchall()
    assert rows == [(10,), (20,)]


def test_union_tables_by_name_regex_raises_when_no_match():
    engine = create_engine("sqlite:///:memory:")
    stage_dataframe(engine, "stg_any", pd.DataFrame({"id": [1]}))

    with pytest.raises(ValueError, match="No tables matched regex"):
        union_tables_by_name_regex(engine, None, r"^missing_", "stg_output")


def test_drop_tables_by_name_regex_drops_matching_tables():
    engine = create_engine("sqlite:///:memory:")
    stage_dataframe(engine, "stg_drop_1", pd.DataFrame({"id": [1]}))
    stage_dataframe(engine, "stg_drop_2", pd.DataFrame({"id": [2]}))
    stage_dataframe(engine, "stg_keep", pd.DataFrame({"id": [3]}))

    dropped = drop_tables_by_name_regex(engine, None, r"^stg_drop_\d+$")

    assert dropped == ["stg_drop_1", "stg_drop_2"]
    with engine.connect() as conn:
        remaining = {
            row[0]
            for row in conn.execute(
                text("SELECT name FROM sqlite_master WHERE type = 'table'")
            ).fetchall()
        }
    assert "stg_keep" in remaining
    assert "stg_drop_1" not in remaining
    assert "stg_drop_2" not in remaining


def test_drop_tables_by_name_regex_mssql_quotes_literal_brackets(monkeypatch):
    class _FakeDialect:
        name = "mssql"

    class _FakeConn:
        def __init__(self):
            self.executed = []

        def execute(self, stmt):
            self.executed.append(str(stmt))

    class _FakeBegin:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self.conn

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeEngine:
        dialect = _FakeDialect()

        def __init__(self):
            self.conn = _FakeConn()

        def begin(self):
            return _FakeBegin(self.conn)

    class _FakeInspector:
        def get_table_names(self, schema=None):
            assert str(schema) == "Staging OFFICIAL - SENSITIVE [MARKET SENSITIVE]"
            return ["a_qd_stg", "b_qd_stg"]

    engine = _FakeEngine()
    inspector = _FakeInspector()

    monkeypatch.setattr("dataprepkit.helpers.staging.inspect", lambda _: inspector)
    monkeypatch.setattr("dataprepkit.helpers.staging.ensure_schema_exists", lambda *_: None)

    dropped = drop_tables_by_name_regex(
        engine=engine,
        schema="Staging OFFICIAL - SENSITIVE [MARKET SENSITIVE]",
        table_name_regex=r"^\w_qd_stg$",
    )

    assert dropped == ["a_qd_stg", "b_qd_stg"]
    assert (
        engine.conn.executed[0]
        == "DROP TABLE [Staging OFFICIAL - SENSITIVE [MARKET SENSITIVE]]].[a_qd_stg]"
    )
    assert (
        engine.conn.executed[1]
        == "DROP TABLE [Staging OFFICIAL - SENSITIVE [MARKET SENSITIVE]]].[b_qd_stg]"
    )


def test_union_tables_by_name_regex_mssql_quotes_schema_with_literal_brackets(monkeypatch):
    class _FakeDialect:
        name = "mssql"

    class _FakeEngine:
        dialect = _FakeDialect()

    class _FakeInspector:
        def __init__(self):
            self.schema_arg = None

        def get_table_names(self, schema=None):
            self.schema_arg = schema
            return ["afw_qd_validation_stg", "nhs_qd_validation_stg"]

    engine = _FakeEngine()
    inspector = _FakeInspector()
    read_calls = []
    output_call = {}

    def fake_inspect(engine_arg):
        assert engine_arg is engine
        return inspector

    def fake_read_sql_table(name, con, schema):
        read_calls.append((name, con, schema))
        return pd.DataFrame({"id": [1]})

    def fake_to_sql(self, name, con, if_exists, index, schema):
        output_call.update(
            {
                "name": name,
                "con": con,
                "if_exists": if_exists,
                "index": index,
                "schema": schema,
            }
        )

    monkeypatch.setattr("dataprepkit.helpers.staging.inspect", fake_inspect)
    monkeypatch.setattr("dataprepkit.helpers.staging.ensure_schema_exists", lambda *_: None)
    monkeypatch.setattr(pd, "read_sql_table", fake_read_sql_table)
    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql)

    union_tables_by_name_regex(
        engine=engine,
        schema="Staging OFFICIAL - SENSITIVE [MARKET SENSITIVE]",
        table_name_regex=r"^\w{3}_qd_validation_stg$",
        output_table_name="qd_stg",
    )

    assert str(inspector.schema_arg) == "Staging OFFICIAL - SENSITIVE [MARKET SENSITIVE]"
    assert getattr(inspector.schema_arg, "quote", None) is True
    assert len(read_calls) == 2
    assert output_call["if_exists"] == "replace"
    assert output_call["index"] is False
    assert str(output_call["schema"]) == "Staging OFFICIAL - SENSITIVE [MARKET SENSITIVE]"
    assert getattr(output_call["schema"], "quote", None) is True
    assert str(output_call["name"]) == "qd_stg"
    assert getattr(output_call["name"], "quote", None) is True


def test_dependency_null_value_does_not_rehash_other_rows():
    engine = create_engine("sqlite:///:memory:")
    metadata_name = "tbl_map_dmex_metric_null"
    METADATA_REGISTRY.pop(metadata_name, None)
    register_metadata(
        metadata_name,
        {
            "target_table": "tbl_map_dmex_metric_null",
            "natural_key_cols": ["Measure_Cd"],
            "data_columns": {
                "DMeX_Metric_Cd": {"type": "TEXT", "nullable": False},
                "DMeX_Measure_Type": {"type": "TEXT", "nullable": True},
                "DMeX_Metric_Type_Cd": {"type": "TEXT", "nullable": True},
                "DMex_Metric_Instance_Id": {"type": "BIGINT", "nullable": True},
            },
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": "dummy.csv",
            "schema_handling": {"mode": "evolve"},
            "dependencies": [
                {
                    "table": "dep_dim",
                    "on": [{"source": "DMeX_Metric_Cd", "target": "DMeX_Metric_Cd"}],
                    "select": {"DMex_Metric_Instance_Id": "DMex_Metric_Instance_Id"},
                    "how": "left",
                    "on_missing": "null",
                }
            ],
        },
    )

    stage_df = pd.DataFrame(
        [
            {
                "Measure_Cd": "m1",
                "DMeX_Metric_Cd": "A",
                "DMeX_Measure_Type": "type1",
                "DMeX_Metric_Type_Cd": "metric1",
            },
            {
                "Measure_Cd": "m2",
                "DMeX_Metric_Cd": "B",
                "DMeX_Measure_Type": "type2",
                "DMeX_Metric_Type_Cd": "metric2",
            },
        ]
    )

    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE dep_dim (
                        DMeX_Metric_Cd TEXT PRIMARY KEY,
                        DMex_Metric_Instance_Id INTEGER,
                        Current_Ind INTEGER NOT NULL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    "INSERT INTO dep_dim (DMeX_Metric_Cd, DMex_Metric_Instance_Id, Current_Ind) VALUES "
                    "('A', 10, 1), ('B', 20, 1)"
                )
            )

        run_dimension(engine, metadata_name, override_df=stage_df)
        with engine.connect() as conn:
            initial_hash = {
                row["Measure_Cd"]: row["row_hash"]
                for row in conn.execute(
                    text(
                        "SELECT Measure_Cd, row_hash FROM tbl_map_dmex_metric_null WHERE Current_Ind = 1"
                    )
                ).mappings()
            }

        with engine.begin() as conn:
            conn.execute(
                text("UPDATE dep_dim SET DMex_Metric_Instance_Id = NULL WHERE DMeX_Metric_Cd = 'A'")
            )

        run_dimension(engine, metadata_name, override_df=stage_df)
        with engine.connect() as conn:
            final_hash = {
                row["Measure_Cd"]: row["row_hash"]
                for row in conn.execute(
                    text(
                        "SELECT Measure_Cd, row_hash FROM tbl_map_dmex_metric_null WHERE Current_Ind = 1"
                    )
                ).mappings()
            }

        assert final_hash["m2"] == initial_hash["m2"]
    finally:
        METADATA_REGISTRY.pop(metadata_name, None)


def test_dependency_change_generates_new_history_for_affected_row():
    engine = create_engine("sqlite:///:memory:")
    metadata_name = "tbl_map_dmex_metric_change"
    METADATA_REGISTRY.pop(metadata_name, None)
    register_metadata(
        metadata_name,
        {
            "target_table": "tbl_map_dmex_metric_change",
            "natural_key_cols": ["Measure_Cd"],
            "data_columns": {
                "DMeX_Metric_Cd": {"type": "TEXT", "nullable": False},
                "DMeX_Measure_Type": {"type": "TEXT", "nullable": True},
                "DMeX_Metric_Type_Cd": {"type": "TEXT", "nullable": True},
                "DMex_Metric_Instance_Id": {"type": "BIGINT", "nullable": True},
            },
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": "dummy.csv",
            "schema_handling": {"mode": "evolve"},
            "dependencies": [
                {
                    "table": "dep_dim",
                    "on": [{"source": "DMeX_Metric_Cd", "target": "DMeX_Metric_Cd"}],
                    "select": {"DMex_Metric_Instance_Id": "DMex_Metric_Instance_Id"},
                    "how": "left",
                    "on_missing": "null",
                }
            ],
        },
    )

    stage_df = pd.DataFrame(
        [
            {
                "Measure_Cd": "m1",
                "DMeX_Metric_Cd": "A",
                "DMeX_Measure_Type": "type1",
                "DMeX_Metric_Type_Cd": "metric1",
            },
            {
                "Measure_Cd": "m2",
                "DMeX_Metric_Cd": "B",
                "DMeX_Measure_Type": "type2",
                "DMeX_Metric_Type_Cd": "metric2",
            },
        ]
    )

    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE dep_dim (
                        DMeX_Metric_Cd TEXT PRIMARY KEY,
                        DMex_Metric_Instance_Id INTEGER,
                        Current_Ind INTEGER NOT NULL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    "INSERT INTO dep_dim (DMeX_Metric_Cd, DMex_Metric_Instance_Id, Current_Ind) VALUES "
                    "('A', 10, 1), ('B', 20, 1)"
                )
            )

        run_dimension(engine, metadata_name, override_df=stage_df)
        with engine.connect() as conn:
            initial_hash = {
                row["Measure_Cd"]: row["row_hash"]
                for row in conn.execute(
                    text(
                        "SELECT Measure_Cd, row_hash FROM tbl_map_dmex_metric_change WHERE Current_Ind = 1"
                    )
                ).mappings()
            }

        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE dep_dim SET DMex_Metric_Instance_Id = 11 WHERE DMeX_Metric_Cd = 'A'"
                )
            )

        run_dimension(engine, metadata_name, override_df=stage_df)
        with engine.connect() as conn:
            final_hash = {
                row["Measure_Cd"]: row["row_hash"]
                for row in conn.execute(
                    text(
                        "SELECT Measure_Cd, row_hash FROM tbl_map_dmex_metric_change WHERE Current_Ind = 1"
                    )
                ).mappings()
            }
            row_counts = {
                key: sum(
                    1
                    for row in conn.execute(
                        text(
                            "SELECT Current_Ind FROM tbl_map_dmex_metric_change WHERE Measure_Cd = :key"
                        ),
                        {"key": key},
                    )
                )
                for key in ["m1", "m2"]
            }

        assert final_hash["m2"] == initial_hash["m2"]
        assert final_hash["m1"] != initial_hash["m1"]
        assert row_counts["m1"] == 2
        assert row_counts["m2"] == 1
    finally:
        METADATA_REGISTRY.pop(metadata_name, None)


def test_repeated_dependency_updates_append_history():
    engine = create_engine("sqlite:///:memory:")
    metadata_name = "tbl_map_dmex_metric_repeated"
    METADATA_REGISTRY.pop(metadata_name, None)
    register_metadata(
        metadata_name,
        {
            "target_table": "tbl_map_dmex_metric_repeated",
            "natural_key_cols": ["Measure_Cd"],
            "data_columns": {
                "DMeX_Metric_Cd": {"type": "TEXT", "nullable": False},
                "DMeX_Measure_Type": {"type": "TEXT", "nullable": True},
                "DMeX_Metric_Type_Cd": {"type": "TEXT", "nullable": True},
                "DMex_Metric_Instance_Id": {"type": "BIGINT", "nullable": True},
            },
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": "dummy.csv",
            "schema_handling": {"mode": "evolve"},
            "dependencies": [
                {
                    "table": "dep_dim",
                    "on": [{"source": "DMeX_Metric_Cd", "target": "DMeX_Metric_Cd"}],
                    "select": {"DMex_Metric_Instance_Id": "DMex_Metric_Instance_Id"},
                    "how": "left",
                    "on_missing": "null",
                }
            ],
        },
    )

    stage_df = pd.DataFrame(
        [
            {
                "Measure_Cd": "m1",
                "DMeX_Metric_Cd": "A",
                "DMeX_Measure_Type": "type1",
                "DMeX_Metric_Type_Cd": "metric1",
            },
            {
                "Measure_Cd": "m2",
                "DMeX_Metric_Cd": "B",
                "DMeX_Measure_Type": "type2",
                "DMeX_Metric_Type_Cd": "metric2",
            },
        ]
    )

    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE dep_dim (
                        DMeX_Metric_Cd TEXT PRIMARY KEY,
                        DMex_Metric_Instance_Id INTEGER,
                        Current_Ind INTEGER NOT NULL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    "INSERT INTO dep_dim (DMeX_Metric_Cd, DMex_Metric_Instance_Id, Current_Ind) VALUES "
                    "('A', 10, 1), ('B', 20, 1)"
                )
            )

        run_dimension(engine, metadata_name, override_df=stage_df)

        for value in range(11, 16):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE dep_dim SET DMex_Metric_Instance_Id = :value WHERE DMeX_Metric_Cd = 'A'"
                    ),
                    {"value": value},
                )
            run_dimension(engine, metadata_name, override_df=stage_df)

        with engine.connect() as conn:
            m1_rows = list(
                conn.execute(
                    text(
                        "SELECT Current_Ind, DMex_Metric_Instance_Id FROM tbl_map_dmex_metric_repeated WHERE Measure_Cd = 'm1'"
                    )
                )
            )
            m2_rows = list(
                conn.execute(
                    text(
                        "SELECT Current_Ind FROM tbl_map_dmex_metric_repeated WHERE Measure_Cd = 'm2'"
                    )
                )
            )
            final_m1 = conn.execute(
                text(
                    "SELECT DMex_Metric_Instance_Id FROM tbl_map_dmex_metric_repeated WHERE Measure_Cd = 'm1' AND Current_Ind = 1"
                )
            ).scalar_one()
            all_m1_values = {row[1] for row in m1_rows}

        assert len(m1_rows) == 6
        assert sum(1 for row in m1_rows if row[0] == 1) == 1
        assert sum(1 for row in m1_rows if row[0] == 0) == 5
        assert final_m1 == 15
        assert all_m1_values == {10, 11, 12, 13, 14, 15}
        assert len(m2_rows) == 1
    finally:
        METADATA_REGISTRY.pop(metadata_name, None)


def test_dependency_and_stage_updates_accumulate_history():
    engine = create_engine("sqlite:///:memory:")
    metadata_name = "tbl_map_dmex_metric_combo"
    METADATA_REGISTRY.pop(metadata_name, None)
    register_metadata(
        metadata_name,
        {
            "target_table": "tbl_map_dmex_metric_combo",
            "natural_key_cols": ["Measure_Cd"],
            "data_columns": {
                "DMeX_Metric_Cd": {"type": "TEXT", "nullable": False},
                "DMeX_Measure_Type": {"type": "TEXT", "nullable": True},
                "DMeX_Metric_Type_Cd": {"type": "TEXT", "nullable": True},
                "DMex_Metric_Instance_Id": {"type": "BIGINT", "nullable": True},
            },
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": "dummy.csv",
            "schema_handling": {"mode": "evolve"},
            "dependencies": [
                {
                    "table": "dep_dim",
                    "on": [{"source": "DMeX_Metric_Cd", "target": "DMeX_Metric_Cd"}],
                    "select": {"DMex_Metric_Instance_Id": "DMex_Metric_Instance_Id"},
                    "how": "left",
                    "on_missing": "null",
                }
            ],
        },
    )

    stage_df = pd.DataFrame(
        [
            {
                "Measure_Cd": "m1",
                "DMeX_Metric_Cd": "A",
                "DMeX_Measure_Type": "stage-0",
                "DMeX_Metric_Type_Cd": "metric1",
            },
            {
                "Measure_Cd": "m2",
                "DMeX_Metric_Cd": "B",
                "DMeX_Measure_Type": "type2",
                "DMeX_Metric_Type_Cd": "metric2",
            },
        ]
    )

    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE dep_dim (
                        DMeX_Metric_Cd TEXT PRIMARY KEY,
                        DMex_Metric_Instance_Id INTEGER,
                        Current_Ind INTEGER NOT NULL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    "INSERT INTO dep_dim (DMeX_Metric_Cd, DMex_Metric_Instance_Id, Current_Ind) VALUES "
                    "('A', 10, 1), ('B', 20, 1)"
                )
            )

        run_dimension(engine, metadata_name, override_df=stage_df)

        edits = [
            (11, "stage-1", "metric-A"),
            (12, "stage-2", "metric-B"),
            (13, "stage-3", "metric-C"),
            (14, "stage-4", "metric-D"),
            (15, "stage-5", "metric-E"),
        ]
        for value, measure_type, metric_type in edits:
            stage_df.loc[stage_df.Measure_Cd == "m1", "DMeX_Measure_Type"] = measure_type
            stage_df.loc[stage_df.Measure_Cd == "m1", "DMeX_Metric_Type_Cd"] = metric_type
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE dep_dim SET DMex_Metric_Instance_Id = :value WHERE DMeX_Metric_Cd = 'A'"
                    ),
                    {"value": value},
                )
            run_dimension(engine, metadata_name, override_df=stage_df)

        with engine.connect() as conn:
            rows = list(
                conn.execute(
                    text(
                        "SELECT Measure_Cd, DMex_Metric_Instance_Id, DMeX_Measure_Type, DMeX_Metric_Type_Cd, Current_Ind "
                        "FROM tbl_map_dmex_metric_combo "
                        "WHERE Measure_Cd = 'm1'"
                    )
                )
            )
            history_values = {row[2] for row in rows}
            metric_types = {row[3] for row in rows}
            current_row = next(row for row in rows if row[4] == 1)

        assert len(rows) == 6
        assert history_values == {"stage-0", "stage-1", "stage-2", "stage-3", "stage-4", "stage-5"}
        assert current_row[1:] == (15, "stage-5", "metric-E", 1)
        assert metric_types == {"metric1", "metric-A", "metric-B", "metric-C", "metric-D", "metric-E"}
    finally:
        METADATA_REGISTRY.pop(metadata_name, None)
