import hashlib
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from dataprepkit.helpers.staging import (
    HashMismatchError,
    StageFileSpec,
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
