from pathlib import Path

import hashlib
import textwrap

import pytest
from sqlalchemy import create_engine, text

from dataprepkit.fact_loader import (
    HashMismatchError,
    MissingStageFileError,
    StageFileSpec,
    verify_stage_file_hashes,
)


def _create_file(path: Path, *, content: bytes) -> None:
    path.write_bytes(content)


def _create_engine_with_table(engine):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE staging (
                    Organisation_Cd TEXT,
                    Filename TEXT,
                    file_hash_md5 TEXT
                )
                """
            )
        )


@pytest.fixture
def sample_engine(tmp_path):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    _create_engine_with_table(engine)
    yield engine
    engine.dispose()


def _insert_record(engine, org, filename, hash_value):
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO staging (Organisation_Cd, Filename, file_hash_md5) VALUES (:org, :filename, :hash)"
            ),
            {"org": org, "filename": filename, "hash": hash_value},
        )


def _md5_file(path: Path) -> str:
    h = hashlib.md5()
    h.update(path.read_bytes())
    return h.hexdigest()


def test_verify_stage_file_hashes_success(tmp_path, sample_engine):
    file_path = tmp_path / "data.csv"
    _create_file(file_path, content=b"payload")
    expected = _md5_file(file_path)
    _insert_record(sample_engine, "REGION1", str(file_path), expected)

    spec = StageFileSpec("Organisation_Cd", "Filename", "file_hash_md5")
    verify_stage_file_hashes(
        sample_engine,
        "staging",
        spec,
        base_path=str(tmp_path),
    )


def test_verify_stage_file_hashes_mismatch(tmp_path, sample_engine):
    file_path = tmp_path / "data.csv"
    _create_file(file_path, content=b"payload")
    wrong_hash = "deadbeef"
    _insert_record(sample_engine, "REGION1", str(file_path), wrong_hash)

    spec = StageFileSpec("Organisation_Cd", "Filename", "file_hash_md5")
    with pytest.raises(HashMismatchError):
        verify_stage_file_hashes(
            sample_engine,
            "staging",
            spec,
            base_path=str(tmp_path),
        )


def test_missing_target_file(tmp_path, sample_engine):
    _insert_record(sample_engine, "REGION1", "unmatched.csv", "abc123")
    spec = StageFileSpec("Organisation_Cd", "Filename", "file_hash_md5")
    with pytest.raises(MissingStageFileError):
        verify_stage_file_hashes(
            sample_engine,
            "staging",
            spec,
            base_path=str(tmp_path),
        )
