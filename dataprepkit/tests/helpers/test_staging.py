import hashlib
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, inspect, text

from dataprepkit.helpers.staging import (
    HashMismatchError,
    StageFileSpec,
    clone_table,
    assert_columns_have_single_distinct_row,
    assert_columns_not_null,
    sync_mssql_tables,
    verify_stage_file_hashes,
)


def _md5_file(path: Path) -> str:
    h = hashlib.md5()
    h.update(path.read_bytes())
    return h.hexdigest()


def _create_hash_staging(engine, rows):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE staging_fact (
                    Organisation_Cd TEXT,
                    Filename TEXT,
                    file_hash_md5 TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO staging_fact (
                    Organisation_Cd,
                    Filename,
                    file_hash_md5
                )
                VALUES (:org, :filename, :hash)
                """
            ),
            rows,
        )


def test_verify_stage_file_hashes_success(tmp_path):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    file_path = tmp_path / "REGION1" / "data.csv"
    file_path.parent.mkdir()
    file_path.write_bytes(b"payload")
    _create_hash_staging(
        engine,
        [{"org": "REGION1", "filename": "data.csv", "hash": _md5_file(file_path)}],
    )

    verify_stage_file_hashes(
        engine,
        "staging_fact",
        StageFileSpec("Organisation_Cd", "Filename", "file_hash_md5"),
        schema="main",
        base_path=str(tmp_path),
    )


def test_verify_stage_file_hashes_raises_for_mismatch(tmp_path):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    file_path = tmp_path / "REGION1" / "data.csv"
    file_path.parent.mkdir()
    file_path.write_bytes(b"payload")
    _create_hash_staging(
        engine,
        [{"org": "REGION1", "filename": "data.csv", "hash": "deadbeef"}],
    )

    with pytest.raises(HashMismatchError):
        verify_stage_file_hashes(
            engine,
            "staging_fact",
            StageFileSpec("Organisation_Cd", "Filename", "file_hash_md5"),
            schema="main",
            base_path=str(tmp_path),
        )


def test_staging_validation_helpers_pass():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE staging_fact (a TEXT, b TEXT)"))
        conn.execute(
            text(
                "INSERT INTO staging_fact (a, b) VALUES "
                "('x', '1'), ('x', '1')"
            )
        )

    assert_columns_not_null(
        engine,
        table_name="staging_fact",
        schema="main",
        columns=["a", "b"],
    )
    assert_columns_have_single_distinct_row(
        engine,
        table_name="staging_fact",
        schema="main",
        columns=["a", "b"],
    )


def test_staging_validation_helpers_raise():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE staging_fact (a TEXT, b TEXT)"))
        conn.execute(
            text(
                "INSERT INTO staging_fact (a, b) VALUES "
                "('x', '1'), ('y', NULL)"
            )
        )

    with pytest.raises(RuntimeError, match="Null values found"):
        assert_columns_not_null(
            engine,
            table_name="staging_fact",
            schema="main",
            columns=["a", "b"],
        )
    with pytest.raises(RuntimeError, match="Expected a single distinct row"):
        assert_columns_have_single_distinct_row(
            engine,
            table_name="staging_fact",
            schema="main",
            columns=["a"],
        )


def test_clone_table_recreates_schema_and_copies_rows():
    source_engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    target_engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with source_engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE source_table (
                    id INTEGER PRIMARY KEY,
                    code TEXT NOT NULL,
                    value TEXT,
                    CONSTRAINT uq_source_table_code UNIQUE (code)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO source_table (id, code, value)
                VALUES (1, 'A', 'alpha'), (2, 'B', 'beta')
                """
            )
        )

    with target_engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE source_table (
                    id INTEGER PRIMARY KEY,
                    code TEXT,
                    value TEXT,
                    obsolete TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO source_table (id, code, value, obsolete)
                VALUES (99, 'Z', 'old', 'legacy')
                """
            )
        )

    clone_table(source_engine, target_engine, "main", "source_table")

    target_inspector = inspect(target_engine)
    assert [column["name"] for column in target_inspector.get_columns("source_table")] == [
        "id",
        "code",
        "value",
    ]
    assert target_inspector.get_pk_constraint("source_table")["constrained_columns"] == [
        "id"
    ]
    assert any(
        unique_constraint["column_names"] == ["code"]
        for unique_constraint in target_inspector.get_unique_constraints("source_table")
    )

    with target_engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id, code, value FROM source_table ORDER BY id")
        ).fetchall()

    assert rows == [(1, "A", "alpha"), (2, "B", "beta")]


def test_clone_table_uses_parquet_branch_when_requested(monkeypatch, tmp_path):
    class _FakeRow:
        def __init__(self, mapping):
            self._mapping = mapping

    class _FakeResult:
        def __init__(self, rows):
            self._rows = [_FakeRow(row) for row in rows]

        def __iter__(self):
            return iter(self._rows)

        def fetchall(self):
            return self._rows

    class _FakeSourceConn:
        def __init__(self, rows):
            self._rows = rows

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, statement, params=None):
            text = str(statement)
            if "sys.indexes" in text:
                return _FakeResult([])
            if "sys.columns" in text:
                return _FakeResult(
                    [
                        {"column_name": "Insert_Date", "column_scale": 3},
                    ]
                )
            assert "SELECT" in text
            return _FakeResult(self._rows)

    class _FakeTxnConn:
        def __init__(self, statements):
            self.statements = statements

        def execute(self, statement):
            self.statements.append(str(statement))

    class _FakeTxn:
        def __init__(self, statements):
            self._conn = _FakeTxnConn(statements)

        def __enter__(self):
            return self._conn

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeDialect:
        name = "mssql"

    class _FakeEngine:
        def __init__(self, rows=None):
            self.dialect = _FakeDialect()
            self._rows = rows or []
            self.statements = []

        def connect(self):
            return _FakeSourceConn(self._rows)

        def begin(self):
            return _FakeTxn(self.statements)

    class _FakeInspector:
        def __init__(self, columns=None, has_table=False):
            self._columns = columns or []
            self._has_table = has_table

        def get_columns(self, table, schema=None):
            return self._columns

        def get_pk_constraint(self, table, schema=None):
            return {"constrained_columns": ["Measure_Instance_Id"], "name": None}

        def has_table(self, table, schema=None):
            return self._has_table

    source_engine = _FakeEngine(
        rows=[
            {
                "Measure_Instance_Id": 1,
                "Measure_Id": 10,
                "Measure_Cd": "A",
                "Insert_Date": "2024-01-01 00:00:00.000",
            },
            {
                "Measure_Instance_Id": 2,
                "Measure_Id": None,
                "Measure_Cd": "B",
                "Insert_Date": "2024-01-02 00:00:00.000",
            },
        ]
    )
    target_engine = _FakeEngine()

    source_columns = [
        {"name": "Measure_Instance_Id", "type": "INT", "nullable": False},
        {"name": "Measure_Id", "type": "INT", "nullable": True},
        {"name": "Measure_Cd", "type": "NVARCHAR(100)", "nullable": True},
        {"name": "Insert_Date", "type": "DATETIME2(3)", "nullable": False},
    ]
    source_inspector = _FakeInspector(columns=source_columns)
    target_inspector = _FakeInspector(has_table=True)

    def _fake_inspect(engine):
        return source_inspector if engine is source_engine else target_inspector

    captured = {}

    def _fake_stage_dataframe(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        temp_dir = Path(kwargs["parquet_base_dir"]) / args[1] / "part-00000"
        temp_dir.mkdir(parents=True, exist_ok=True)
        (temp_dir / "part-00000.parquet").write_text("payload")

    monkeypatch.setattr("dataprepkit.helpers.staging.inspect", _fake_inspect)
    monkeypatch.setattr("dataprepkit.helpers.staging.stage_dataframe", _fake_stage_dataframe)

    clone_table(
        source_engine,
        target_engine,
        "main",
        "source_table",
        staging_use_openrowset_parquet=True,
        staging_parquet_base_dir=str(tmp_path),
        staging_copy_source_base_url="https://example.test",
    )

    assert captured["args"][1] == "source_table__raw"
    assert all(str(dtype).startswith("string") for dtype in captured["args"][2].dtypes)
    measure_id_values = captured["args"][2]["Measure_Id"].tolist()
    assert measure_id_values[0] == "10"
    assert measure_id_values[1] is pd.NA
    assert captured["kwargs"]["schema"] == "main"
    assert captured["kwargs"]["use_copy_into_parquet"] is True
    assert captured["kwargs"]["parquet_base_dir"] == str(tmp_path)
    assert captured["kwargs"]["copy_source_base_url"] == "https://example.test"
    assert any("DATETIME2(3)" in statement for statement in target_engine.statements)
    assert any(
        "INSERT INTO [main].[source_table]" in statement
        for statement in target_engine.statements
    )
    assert any(
        "FROM [main].[source_table__raw] src" in statement
        for statement in target_engine.statements
    )
    assert any(
        "DROP TABLE IF EXISTS [main].[source_table__raw]" in statement
        for statement in target_engine.statements
    )
    assert not (tmp_path / "source_table__raw").exists()


def test_sync_mssql_tables_clones_only_accepted_statuses(monkeypatch):
    source_engine = object()
    target_engine = object()

    source_dates = pd.DataFrame(
        [
            {
                "schema_name": "Dimensions",
                "table_name": "dim_a",
                "max_insert_date": pd.Timestamp("2024-01-01"),
                "max_update_date": pd.Timestamp("2024-01-01"),
            },
            {
                "schema_name": "Dimensions",
                "table_name": "dim_b",
                "max_insert_date": pd.Timestamp("2024-01-02"),
                "max_update_date": pd.Timestamp("2024-01-02"),
            },
            {
                "schema_name": "Dimensions",
                "table_name": "dim_c",
                "max_insert_date": pd.Timestamp("2024-01-03"),
                "max_update_date": pd.Timestamp("2024-01-03"),
            },
        ]
    )
    target_dates = pd.DataFrame(
        [
            {
                "schema_name": "Dimensions",
                "table_name": "dim_a",
                "max_insert_date": pd.Timestamp("2024-01-01"),
                "max_update_date": pd.Timestamp("2024-01-01"),
            },
            {
                "schema_name": "Dimensions",
                "table_name": "dim_b",
                "max_insert_date": pd.Timestamp("2024-01-01"),
                "max_update_date": pd.Timestamp("2024-01-01"),
            },
        ]
    )

    captured = []

    def _fake_get_schema_max_dates(engine, schema):
        assert schema == "Dimensions"
        if engine is source_engine:
            return source_dates
        if engine is target_engine:
            return target_dates
        raise AssertionError("Unexpected engine")

    def _fake_clone_table(*args, **kwargs):
        captured.append((args, kwargs))

    monkeypatch.setattr(
        "dataprepkit.helpers.staging._get_schema_max_dates",
        _fake_get_schema_max_dates,
    )
    monkeypatch.setattr("dataprepkit.helpers.staging.clone_table", _fake_clone_table)

    diffs = sync_mssql_tables(
        source_engine,
        target_engine,
        "Dimensions",
        staging_use_openrowset_parquet=True,
        staging_parquet_base_dir=r"C:\tmp",
        staging_copy_source_base_url="https://example.test",
        accepted_statuses=["missing_in_target", "data_mismatch"],
    )

    assert list(diffs["table_name"]) == ["dim_b", "dim_c"]
    assert [call[1]["table_name"] for call in captured] == ["dim_b", "dim_c"]
    assert all(call[1]["staging_use_openrowset_parquet"] is True for call in captured)
    assert all(call[1]["staging_parquet_base_dir"] == r"C:\tmp" for call in captured)
    assert all(
        call[1]["staging_copy_source_base_url"] == "https://example.test"
        for call in captured
    )
