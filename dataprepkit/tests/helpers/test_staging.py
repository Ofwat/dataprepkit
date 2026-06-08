import hashlib
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, text

from dataprepkit.helpers.staging import (
    HashMismatchError,
    StageFileSpec,
    clone_table,
    assert_columns_have_single_distinct_row,
    assert_columns_not_null,
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
