from pathlib import Path

import hashlib
import textwrap

import pytest
from sqlalchemy import create_engine, text

from dataprepkit.fact_loader import (
    DimensionJoinSpec,
    ExtraColumnSpec,
    FactBatchMetadata,
    FactConfig,
    HashMismatchError,
    MissingStageFileError,
    StageFileSpec,
    ingest_fact,
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


def _create_fact_tables(engine):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS staging (
                    batch_id TEXT,
                    Organisation_Cd TEXT,
                    measure_value REAL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS fact (
                    batch_id TEXT,
                    company_sk INTEGER,
                    measure_value REAL,
                    Company_Instance_Id INTEGER,
                    Company_Id INTEGER,
                    Region_Id INTEGER,
                    Insert_Date TEXT
                )
                """
            )
        )
        conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS Dimensions_tbl_d_company (
                        surrogate_key INTEGER PRIMARY KEY,
                        join_numeric_key INTEGER,
                        Organisation_Cd TEXT,
                        Company_Instance_Id INTEGER,
                        Company_Id INTEGER,
                        Company_Type_Cd TEXT,
                        current_ind INTEGER
                    )
                    """
                )
            )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS Dimensions_tbl_d_company_type (
                    surrogate_key INTEGER PRIMARY KEY,
                    Company_Type_Cd TEXT,
                    Company_Type_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS Dimensions_tbl_d_region (
                    Region_Id INTEGER PRIMARY KEY,
                    Company_Id INTEGER,
                    current_ind INTEGER
                )
                """
            )
        )


@pytest.fixture
def fact_engine(tmp_path):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    _create_fact_tables(engine)
    yield engine
    engine.dispose()


def test_ingest_fact_populates_company_ids(fact_engine):
    with fact_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO staging (batch_id, Organisation_Cd, measure_value) VALUES ('B1','REGION1', 1.5)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO Dimensions_tbl_d_company (surrogate_key, join_numeric_key, Organisation_Cd, Company_Instance_Id, Company_Id) VALUES (1, 10, 'REGION1', 100, 200)"
            )
        )
    config = FactConfig(
        batch=FactBatchMetadata(
            fact_table="fact",
            batch_id="B1",
            validations={},
        ),
        extra_columns=[
            ExtraColumnSpec(
                dim_table="Dimensions_tbl_d_company",
                staging_columns=["Organisation_Cd"],
                dim_columns=["Organisation_Cd"],
                column="Company_Id",
                dim_column="Company_Id",
                require_not_null=True,
            )
        ],
        dimensions=[
            DimensionJoinSpec(
                dim_table="Dimensions_tbl_d_company",
                staging_columns=["Organisation_Cd"],
                dim_columns=["Organisation_Cd"],
                add_columns={"Company_Instance_Id": "Company_Instance_Id"},
                require_not_null=["Company_Instance_Id"],
            )
        ],
        fact_columns=[
            "company_sk",
            "measure_value",
            "Company_Instance_Id",
            "Company_Id",
        ],
        source_table="staging",
        temp_table="tmp_fact",
        temp_columns={
            "batch_id": None,
            "Organisation_Cd": None,
            "measure_value": None,
        },
    )
    ingest_fact(fact_engine, config, batch_id="B1")
    with fact_engine.connect() as conn:
        result = conn.execute(text("SELECT batch_id, Company_Instance_Id, Company_Id FROM fact")).fetchone()
    assert result == ("B1", 100, 200)


def test_ingest_fact_missing_dimension(fact_engine):
    with fact_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO staging (batch_id, Organisation_Cd, measure_value) VALUES ('B2','UNKNOWN', 2.5)"
            )
        )
    config = FactConfig(
        batch=FactBatchMetadata(
            fact_table="fact",
            batch_id="B2",
            validations={},
        ),
        extra_columns=[
            ExtraColumnSpec(
                dim_table="Dimensions_tbl_d_company",
                staging_columns=["Organisation_Cd"],
                dim_columns=["Organisation_Cd"],
                column="Company_Id",
                dim_column="Company_Id",
                require_not_null=True,
            )
        ],
        dimensions=[
            DimensionJoinSpec(
                dim_table="Dimensions_tbl_d_company",
                staging_columns=["Organisation_Cd"],
                dim_columns=["Organisation_Cd"],
                add_columns={"Company_Instance_Id": "Company_Instance_Id"},
                require_not_null=["Company_Instance_Id"],
            )
        ],
        fact_columns=[
            "company_sk",
            "measure_value",
            "Company_Instance_Id",
            "Company_Id",
        ],
        source_table="staging",
        temp_table="tmp_fact",
        temp_columns={
            "batch_id": None,
            "Organisation_Cd": None,
            "measure_value": None,
        },
    )
    with pytest.raises(RuntimeError):
        ingest_fact(fact_engine, config, batch_id="B2")


def test_ingest_fact_creates_fact_table_when_missing(fact_engine):
    with fact_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS fact"))
        conn.execute(
            text(
                "INSERT INTO staging (batch_id, Organisation_Cd, measure_value) VALUES ('B3','REGION3', 3.5)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO Dimensions_tbl_d_company (surrogate_key, join_numeric_key, Organisation_Cd, Company_Instance_Id, Company_Id) VALUES (9, 90, 'REGION3', 900, 901)"
            )
        )
    config = FactConfig(
        batch=FactBatchMetadata(
            fact_table="fact",
            batch_id="B3",
            validations={},
        ),
        dimensions=[
            DimensionJoinSpec(
                dim_table="Dimensions_tbl_d_company",
                staging_columns=["Organisation_Cd"],
                dim_columns=["Organisation_Cd"],
                add_columns={"Company_Instance_Id": "Company_Instance_Id"},
                require_not_null=["Company_Instance_Id"],
            )
        ],
        fact_columns=["company_sk", "measure_value", "Company_Instance_Id"],
        source_table="staging",
        temp_table="tmp_fact",
        temp_columns={
            "batch_id": None,
            "Organisation_Cd": None,
            "measure_value": None,
        },
    )
    ingest_fact(fact_engine, config, batch_id="B3")
    with fact_engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(1) FROM fact")).scalar()
    assert count == 1


def test_ingest_fact_allows_non_current_rows_when_disabled(fact_engine):
    with fact_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO staging (batch_id, Organisation_Cd, measure_value) VALUES ('B4','REGION4', 4.5)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO Dimensions_tbl_d_company (surrogate_key, join_numeric_key, Organisation_Cd, Company_Instance_Id, Company_Id, current_ind) VALUES (2, 20, 'REGION4', 400, 500, 0)"
            )
        )
    config = FactConfig(
        batch=FactBatchMetadata(
            fact_table="fact",
            batch_id="B4",
            validations={},
        ),
        dimensions=[
            DimensionJoinSpec(
                dim_table="Dimensions_tbl_d_company",
                staging_columns=["Organisation_Cd"],
                dim_columns=["Organisation_Cd"],
                surrogate_column="company_sk",
                filter_target_current=False,
                add_columns={"Company_Instance_Id": "Company_Instance_Id"},
                require_not_null=["company_sk"],
            )
        ],
        fact_columns=["company_sk", "measure_value", "Company_Instance_Id"],
        source_table="staging",
        temp_table="tmp_fact",
        temp_columns={
            "batch_id": None,
            "Organisation_Cd": None,
            "measure_value": None,
        },
    )
    ingest_fact(fact_engine, config, batch_id="B4")
    with fact_engine.connect() as conn:
        result = conn.execute(text("SELECT company_sk, Company_Instance_Id FROM fact WHERE batch_id='B4'")).fetchone()
    assert result == (2, 400)


def test_ingest_fact_internal_columns_used_for_chain(fact_engine):
    with fact_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO staging (batch_id, Organisation_Cd, measure_value) VALUES ('B5','REGION5', 5.0)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO Dimensions_tbl_d_company (surrogate_key, join_numeric_key, Organisation_Cd, Company_Instance_Id, Company_Id) VALUES (3, 30, 'REGION5', 300, 400)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO Dimensions_tbl_d_region (Region_Id, Company_Id, current_ind) VALUES (55, 400, 1)"
            )
        )
    config = FactConfig(
        batch=FactBatchMetadata(
            fact_table="fact",
            batch_id="B5",
            validations={},
        ),
        extra_columns=[
            ExtraColumnSpec(
                dim_table="Dimensions_tbl_d_company",
                staging_columns=["Organisation_Cd"],
                dim_columns=["Organisation_Cd"],
                column="Company_Id",
                dim_column="Company_Id",
                require_not_null=True,
            )
        ],
        dimensions=[
            DimensionJoinSpec(
                dim_table="Dimensions_tbl_d_company",
                staging_columns=["Organisation_Cd"],
                dim_columns=["Organisation_Cd"],
                surrogate_column="company_sk",
                add_columns={"Company_Instance_Id": "Company_Instance_Id"},
                require_not_null=["company_sk"],
                join_chain=[
                    DimensionJoinSpec(
                        dim_table="Dimensions_tbl_d_region",
                        staging_columns=["Company_Id"],
                        dim_columns=["Company_Id"],
                        add_columns={"Region_Id": "Region_Id"},
                        require_not_null=["Region_Id"],
                    )
                ],
            )
        ],
        fact_columns=["company_sk", "measure_value", "Company_Instance_Id", "Region_Id"],
        source_table="staging",
        temp_table="tmp_fact",
        temp_columns={
            "batch_id": None,
            "Organisation_Cd": None,
            "measure_value": None,
        },
    )
    ingest_fact(fact_engine, config, batch_id="B5")
    with fact_engine.connect() as conn:
        result = conn.execute(text("SELECT company_sk, Region_Id FROM fact WHERE batch_id='B5'")).fetchone()
    assert result == (3, 55)


