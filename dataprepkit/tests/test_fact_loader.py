from pathlib import Path

import hashlib
import textwrap

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool

import dataprepkit.fact_loader as fact_loader_module
from dataprepkit.fact_loader import (
    DimensionJoinSpec,
    ExtraColumnSpec,
    FactBatchMetadata,
    FactConfig,
    HashMismatchError,
    MissingStageFileError,
    StageFileSpec,
    TableRef,
    _fact_pk_clause,
    _null_safe_row_match_predicate,
    _quote_identifier,
    assert_columns_have_single_distinct_row,
    assert_columns_not_null,
    assert_columns_unique,
    ingest_fact,
    verify_stage_file_hashes,
)


def _create_file(path: Path, *, content: bytes) -> None:
    path.write_bytes(content)


def test_fact_pk_clause_defaults_to_int_for_mssql():
    class _FakeDialect:
        name = "mssql"

    class _FakeEngine:
        dialect = _FakeDialect()

    assert _fact_pk_clause(_FakeEngine(), "fact_id") == "fact_id INT IDENTITY(1,1) PRIMARY KEY"


def test_quote_identifier_normalizes_bracketed_name_for_mssql():
    class _FakeDialect:
        name = "mssql"

    class _FakeEngine:
        dialect = _FakeDialect()

    assert _quote_identifier(_FakeEngine(), "[Model version]") == "[Model version]"


def test_null_safe_row_match_predicate_uses_normalized_bracketed_names_for_mssql():
    class _FakeDialect:
        name = "mssql"

    class _FakeEngine:
        dialect = _FakeDialect()

    predicate = _null_safe_row_match_predicate(
        _FakeEngine(),
        "e",
        "s",
        ["[Model version]"],
    )

    assert predicate == (
        "((e.[Model version] = s.[Model version]) OR "
        "(e.[Model version] IS NULL AND s.[Model version] IS NULL))"
    )


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


def test_verify_stage_file_hashes_accepts_explicit_schema_and_table(
    tmp_path, sample_engine, monkeypatch
):
    file_path = tmp_path / "data.csv"
    _create_file(file_path, content=b"payload")
    expected = _md5_file(file_path)
    captured = {}

    def fake_list_stage_files(engine, table_name, spec, filters=None):
        captured["table_name"] = table_name
        yield ("REGION1", "data.csv", expected, {})

    monkeypatch.setattr(
        "dataprepkit.fact_loader._list_stage_files", fake_list_stage_files
    )

    spec = StageFileSpec("Organisation_Cd", "Filename", "file_hash_md5")
    verify_stage_file_hashes(
        sample_engine,
        None,
        spec,
        schema="Staging",
        table="qd_stg",
        path_resolver=lambda *_: str(file_path),
    )

    assert captured["table_name"] == TableRef(table="qd_stg", schema="Staging")


def test_verify_stage_file_hashes_rejects_conflicting_schema(sample_engine):
    spec = StageFileSpec("Organisation_Cd", "Filename", "file_hash_md5")
    with pytest.raises(ValueError, match="Conflicting schema values"):
        verify_stage_file_hashes(
            sample_engine,
            "SchemaA.qd_stg",
            spec,
            schema="SchemaB",
            path_resolver=lambda *_: __file__,
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
    with pytest.raises(
        RuntimeError,
        match=(
            r"Missing dimension match in Dimensions_tbl_d_company "
            r"for staging columns \['Organisation_Cd'\] -> dimension columns \['Organisation_Cd'\].*"
            r"'Organisation_Cd': 'UNKNOWN'"
        ),
    ):
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
        columns = conn.execute(text("PRAGMA table_info(fact)")).fetchall()
    assert count == 1
    pk_columns = [row[1] for row in columns if row[5] == 1]
    assert pk_columns == []
    assert "fact_id" not in [row[1] for row in columns]
    not_null_flags = {row[1]: row[3] for row in columns}
    assert not_null_flags["batch_id"] == 1
    assert not_null_flags["Insert_Date"] == 1


def test_ingest_fact_uses_metadata_pk_column_name_when_creating_fact_table(fact_engine):
    with fact_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS fact"))
        conn.execute(
            text(
                "INSERT INTO staging (batch_id, Organisation_Cd, measure_value) VALUES ('B3P','REGION3', 3.5)"
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
            batch_id="B3P",
            validations={},
            fact_pk_column_name="qd_fact_id",
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
    ingest_fact(fact_engine, config, batch_id="B3P")
    with fact_engine.connect() as conn:
        columns = conn.execute(text("PRAGMA table_info(fact)")).fetchall()
    pk_columns = [row[1] for row in columns if row[5] == 1]
    assert pk_columns == ["qd_fact_id"]


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


def test_ingest_fact_maps_named_surrogate_when_dim_has_no_surrogate_key():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE staging (
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
                CREATE TABLE fact (
                    batch_id TEXT,
                    Organisation_Instance_Id INTEGER,
                    measure_value REAL,
                    Insert_Date TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE Dimensions_tbl_d_company (
                    Organisation_Cd TEXT,
                    Organisation_Instance_Id INTEGER,
                    current_ind INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                "INSERT INTO staging (batch_id, Organisation_Cd, measure_value) VALUES ('B6','ORG6', 6.0)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO Dimensions_tbl_d_company (Organisation_Cd, Organisation_Instance_Id, current_ind) VALUES ('ORG6', 600, 1)"
            )
        )

    config = FactConfig(
        batch=FactBatchMetadata(fact_table="fact", validations={}),
        dimensions=[
            DimensionJoinSpec(
                dim_table="Dimensions_tbl_d_company",
                staging_columns=["Organisation_Cd"],
                dim_columns=["Organisation_Cd"],
                surrogate_column="Organisation_Instance_Id",
                add_columns={"Organisation_Instance_Id": "Organisation_Instance_Id"},
                require_not_null=["Organisation_Instance_Id"],
            )
        ],
        fact_columns=["Organisation_Instance_Id", "measure_value"],
        source_table="staging",
        temp_table="tmp_fact",
        temp_columns={
            "batch_id": None,
            "Organisation_Cd": None,
            "measure_value": None,
        },
    )

    ingest_fact(engine, config, batch_id="B6")
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT Organisation_Instance_Id FROM fact WHERE batch_id='B6'")
        ).scalar_one()
    assert result == 600


def test_ingest_fact_is_idempotent_for_same_batch_id(fact_engine):
    with fact_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO staging (batch_id, Organisation_Cd, measure_value) VALUES ('B9','REGION9', 9.5)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO Dimensions_tbl_d_company (surrogate_key, join_numeric_key, Organisation_Cd, Company_Instance_Id, Company_Id) VALUES (19, 190, 'REGION9', 1900, 1901)"
            )
        )
    config = FactConfig(
        batch=FactBatchMetadata(
            fact_table="fact",
            batch_id="B9",
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

    ingest_fact(fact_engine, config, batch_id="B9")
    ingest_fact(fact_engine, config, batch_id="B9")

    with fact_engine.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(1) FROM fact WHERE batch_id='B9'")
        ).scalar()
    assert count == 1


def test_ingest_fact_same_batch_id_preserves_existing_pk_and_values(fact_engine):
    with fact_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS fact"))
        conn.execute(
            text("DELETE FROM staging WHERE batch_id = 'B9P'")
        )
        conn.execute(
            text(
                "INSERT INTO staging (batch_id, Organisation_Cd, measure_value) VALUES ('B9P','REGION9P', 9.5)"
            )
        )
    config = FactConfig(
        batch=FactBatchMetadata(
            fact_table="fact",
            batch_id="B9P",
            validations={},
        ),
        dimensions=[],
        fact_columns=["measure_value"],
        source_table="staging",
        temp_table="tmp_fact",
        temp_columns={
            "batch_id": {"type": "TEXT", "nullable": False},
            "Organisation_Cd": {"type": "TEXT", "nullable": True},
            "measure_value": {"type": "REAL", "nullable": True},
        },
    )

    ingest_fact(fact_engine, config, batch_id="B9P")
    with fact_engine.connect() as conn:
        first = conn.execute(
            text("SELECT measure_value FROM fact WHERE batch_id='B9P'")
        ).one()

    with fact_engine.begin() as conn:
        conn.execute(text("DELETE FROM staging WHERE batch_id = 'B9P'"))
        conn.execute(
            text(
                "INSERT INTO staging (batch_id, Organisation_Cd, measure_value) VALUES ('B9P','REGION9P', 99.9)"
            )
        )

    ingest_fact(fact_engine, config, batch_id="B9P")
    with fact_engine.connect() as conn:
        second = conn.execute(
            text("SELECT measure_value FROM fact WHERE batch_id='B9P'")
        ).one()

    assert second == first


def test_ingest_fact_skips_duplicate_rows_across_different_batches(fact_engine):
    with fact_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS fact"))
        conn.execute(text("DELETE FROM staging WHERE batch_id IN ('B14', 'B15')"))
        conn.execute(
            text(
                "INSERT INTO staging (batch_id, Organisation_Cd, measure_value) VALUES ('B14','REGION14', 14.5)"
            )
        )
    config = FactConfig(
        batch=FactBatchMetadata(fact_table="fact", validations={}),
        dimensions=[],
        fact_columns=["measure_value"],
        source_table="staging",
        temp_table="tmp_fact",
        temp_columns={
            "batch_id": {"type": "TEXT", "nullable": False},
            "Organisation_Cd": {"type": "TEXT", "nullable": True},
            "measure_value": {"type": "REAL", "nullable": True},
        },
    )

    ingest_fact(fact_engine, config, batch_id="B14")
    with fact_engine.begin() as conn:
        conn.execute(text("DELETE FROM staging WHERE batch_id = 'B14'"))
        conn.execute(
            text(
                "INSERT INTO staging (batch_id, Organisation_Cd, measure_value) VALUES ('B15','REGION14', 14.5)"
            )
        )
    ingest_fact(fact_engine, config, batch_id="B15")

    with fact_engine.connect() as conn:
        rows = conn.execute(
            text("SELECT batch_id, measure_value FROM fact ORDER BY batch_id")
        ).fetchall()
    assert rows == [("B14", 14.5)]


def test_ingest_fact_inserts_changed_rows_across_different_batches(fact_engine):
    with fact_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS fact"))
        conn.execute(text("DELETE FROM staging WHERE batch_id IN ('B16', 'B17')"))
        conn.execute(
            text(
                "INSERT INTO staging (batch_id, Organisation_Cd, measure_value) VALUES ('B16','REGION16', 16.5)"
            )
        )
    config = FactConfig(
        batch=FactBatchMetadata(fact_table="fact", validations={}),
        dimensions=[],
        fact_columns=["measure_value"],
        source_table="staging",
        temp_table="tmp_fact",
        temp_columns={
            "batch_id": {"type": "TEXT", "nullable": False},
            "Organisation_Cd": {"type": "TEXT", "nullable": True},
            "measure_value": {"type": "REAL", "nullable": True},
        },
    )

    ingest_fact(fact_engine, config, batch_id="B16")
    with fact_engine.begin() as conn:
        conn.execute(text("DELETE FROM staging WHERE batch_id = 'B16'"))
        conn.execute(
            text(
                "INSERT INTO staging (batch_id, Organisation_Cd, measure_value) VALUES ('B17','REGION16', 17.5)"
            )
        )
    ingest_fact(fact_engine, config, batch_id="B17")

    with fact_engine.connect() as conn:
        rows = conn.execute(
            text("SELECT batch_id, measure_value FROM fact ORDER BY batch_id")
        ).fetchall()
    assert rows == [("B16", 16.5), ("B17", 17.5)]


def test_ingest_fact_archives_input_table_and_updates_metadata(
    fact_engine, tmp_path, monkeypatch
):
    with fact_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS fact"))
        conn.execute(text("DELETE FROM staging WHERE batch_id = 'B13'"))
        conn.execute(
            text(
                "INSERT INTO staging (batch_id, Organisation_Cd, measure_value) VALUES ('B13','REGION13', 13.5)"
            )
        )
    batch_metadata = FactBatchMetadata(
        fact_table="fact",
        batch_id="B13",
        validations={},
        input_archive_base_dir=str(tmp_path / "archive"),
    )
    config = FactConfig(
        batch=batch_metadata,
        dimensions=[],
        fact_columns=["measure_value"],
        source_table="staging",
        temp_table="tmp_fact",
        temp_columns={
            "batch_id": {"type": "TEXT", "nullable": False},
            "Organisation_Cd": {"type": "TEXT", "nullable": True},
            "measure_value": {"type": "REAL", "nullable": True},
        },
    )

    def fake_to_parquet(self, path, index=False):
        Path(path).write_bytes(b"parquet")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet)

    ingest_fact(fact_engine, config, batch_id="B13")

    assert batch_metadata.input_archive_file_path is not None
    assert batch_metadata.input_archive_filename is not None
    archived_path = Path(batch_metadata.input_archive_file_path)
    assert archived_path.exists()
    assert archived_path.suffix == ".parquet"
    assert "staging__" in batch_metadata.input_archive_filename
    with fact_engine.begin() as conn:
        archive_value = conn.execute(
            text(
                "SELECT Archive_Filename FROM fact WHERE batch_id = 'B13'"
            )
        ).scalar_one()
    assert archive_value == batch_metadata.input_archive_filename


def test_ingest_fact_temp_columns_support_explicit_nullability(fact_engine):
    with fact_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO staging (batch_id, Organisation_Cd, measure_value) VALUES ('B10','REGION10', 10.5)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO Dimensions_tbl_d_company (surrogate_key, join_numeric_key, Organisation_Cd, Company_Instance_Id, Company_Id) VALUES (10, 100, 'REGION10', 1000, 1001)"
            )
        )
    config = FactConfig(
        batch=FactBatchMetadata(
            fact_table="fact",
            batch_id="B10",
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
            "batch_id": {"type": "TEXT", "nullable": False},
            "Organisation_Cd": {"type": "TEXT", "nullable": False},
            "measure_value": {"type": "REAL", "nullable": True},
        },
    )

    ingest_fact(fact_engine, config, batch_id="B10")

    with fact_engine.connect() as conn:
        columns = conn.execute(text("PRAGMA table_info(tmp_fact)")).fetchall()
    not_null_flags = {row[1]: row[3] for row in columns}
    assert not_null_flags["batch_id"] == 1
    assert not_null_flags["Organisation_Cd"] == 1
    assert not_null_flags["measure_value"] == 0


def test_ingest_fact_propagates_temp_nullability_to_new_fact_table(fact_engine):
    with fact_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS fact"))
        conn.execute(
            text(
                "INSERT INTO staging (batch_id, Organisation_Cd, measure_value) VALUES ('B11','REGION11', 11.5)"
            )
        )
    config = FactConfig(
        batch=FactBatchMetadata(
            fact_table="fact",
            batch_id="B11",
            validations={},
        ),
        dimensions=[],
        fact_columns=["measure_value"],
        source_table="staging",
        temp_table="tmp_fact",
        temp_columns={
            "batch_id": {"type": "TEXT", "nullable": False},
            "Organisation_Cd": {"type": "TEXT", "nullable": False},
            "measure_value": {"type": "REAL", "nullable": False},
        },
    )

    ingest_fact(fact_engine, config, batch_id="B11")

    with fact_engine.connect() as conn:
        columns = conn.execute(text("PRAGMA table_info(fact)")).fetchall()
    not_null_flags = {row[1]: row[3] for row in columns}
    assert not_null_flags["measure_value"] == 1


def test_ingest_fact_fact_columns_support_explicit_nullability(fact_engine):
    with fact_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS fact"))
        conn.execute(
            text(
                "INSERT INTO staging (batch_id, Organisation_Cd, measure_value) VALUES ('B12','REGION12', 12.5)"
            )
        )
    config = FactConfig(
        batch=FactBatchMetadata(
            fact_table="fact",
            batch_id="B12",
            validations={},
        ),
        dimensions=[],
        fact_columns=[
            {"name": "measure_value", "nullable": False},
            {"name": "Organisation_Cd", "nullable": False},
        ],
        source_table="staging",
        temp_table="tmp_fact",
        temp_columns={
            "batch_id": {"type": "TEXT", "nullable": False},
            "Organisation_Cd": {"type": "TEXT", "nullable": True},
            "measure_value": {"type": "REAL", "nullable": True},
        },
    )

    ingest_fact(fact_engine, config, batch_id="B12")

    with fact_engine.connect() as conn:
        columns = conn.execute(text("PRAGMA table_info(fact)")).fetchall()
    not_null_flags = {row[1]: row[3] for row in columns}
    assert not_null_flags["measure_value"] == 1
    assert not_null_flags["Organisation_Cd"] == 1


def test_ingest_fact_respects_current_ind_filter_with_mixed_case_column():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE staging (
                    batch_id TEXT,
                    Measure_Cd TEXT,
                    measure_value REAL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE fact (
                    batch_id TEXT,
                    Measure_Instance_Id INTEGER,
                    measure_value REAL,
                    Insert_Date TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE Dimensions_tbl_map_measure (
                    Legacy_BonCode TEXT,
                    Measure_Instance_Id INTEGER,
                    Current_Ind INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                "INSERT INTO staging (batch_id, Measure_Cd, measure_value) VALUES ('B7','M1', 1.0)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO Dimensions_tbl_map_measure (Legacy_BonCode, Measure_Instance_Id, Current_Ind) VALUES ('M1', NULL, 0)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO Dimensions_tbl_map_measure (Legacy_BonCode, Measure_Instance_Id, Current_Ind) VALUES ('M1', 101, 1)"
            )
        )

    config = FactConfig(
        batch=FactBatchMetadata(fact_table="fact", validations={}),
        dimensions=[
            DimensionJoinSpec(
                dim_table="Dimensions_tbl_map_measure",
                staging_columns=["Measure_Cd"],
                dim_columns=["Legacy_BonCode"],
                filter_target_current=True,
                add_columns={"Measure_Instance_Id": "Measure_Instance_Id"},
                require_not_null=["Measure_Instance_Id"],
            )
        ],
        fact_columns=["Measure_Instance_Id", "measure_value"],
        source_table="staging",
        temp_table="tmp_fact",
        temp_columns={
            "batch_id": None,
            "Measure_Cd": None,
            "measure_value": None,
        },
    )

    ingest_fact(engine, config, batch_id="B7")
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT Measure_Instance_Id FROM fact WHERE batch_id='B7'")
        ).scalar_one()
    assert result == 101


def test_ingest_fact_supports_explicit_table_refs(fact_engine):
    with fact_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO staging (batch_id, Organisation_Cd, measure_value) VALUES ('B8','REGION8', 8.5)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO Dimensions_tbl_d_company "
                "(surrogate_key, join_numeric_key, Organisation_Cd, Company_Instance_Id, Company_Id) "
                "VALUES (8, 80, 'REGION8', 800, 801)"
            )
        )
    config = FactConfig(
        batch=FactBatchMetadata(
            fact_table=TableRef(table="fact", schema=None),
            batch_id="B8",
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
        source_table=TableRef(table="staging", schema=None),
        temp_table=TableRef(table="tmp_fact", schema=None),
        temp_columns={
            "batch_id": None,
            "Organisation_Cd": None,
            "measure_value": None,
        },
    )
    ingest_fact(fact_engine, config, batch_id="B8")
    with fact_engine.connect() as conn:
        result = conn.execute(
            text("SELECT batch_id, Company_Instance_Id FROM fact WHERE batch_id='B8'")
        ).fetchone()
    assert result == ("B8", 800)


def test_ingest_fact_with_connection_scoped_temp_table_fails_across_connections(
    tmp_path,
):
    db_path = tmp_path / "temp_scope.db"
    engine = create_engine(
        f"sqlite+pysqlite:///{db_path}",
        future=True,
        poolclass=NullPool,
    )

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE staging (
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
                CREATE TABLE fact (
                    batch_id TEXT,
                    Company_Instance_Id INTEGER,
                    measure_value REAL,
                    Insert_Date TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE Dimensions_tbl_d_company (
                    Organisation_Cd TEXT,
                    Company_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                "INSERT INTO staging (batch_id, Organisation_Cd, measure_value) "
                "VALUES ('B_TEMP', 'REGION1', 1.5)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO Dimensions_tbl_d_company "
                "(Organisation_Cd, Company_Instance_Id) VALUES ('REGION1', 100)"
            )
        )

    config = FactConfig(
        batch=FactBatchMetadata(fact_table="fact", validations={}),
        dimensions=[
            DimensionJoinSpec(
                dim_table="Dimensions_tbl_d_company",
                staging_columns=["Organisation_Cd"],
                dim_columns=["Organisation_Cd"],
                add_columns={"Company_Instance_Id": "Company_Instance_Id"},
                require_not_null=["Company_Instance_Id"],
            )
        ],
        fact_columns=["Company_Instance_Id", "measure_value"],
        source_table="staging",
        temp_table=TableRef(schema="temp", table="tmp_fact"),
        temp_columns={
            "batch_id": None,
            "Organisation_Cd": None,
            "measure_value": None,
        },
    )

    with pytest.raises(OperationalError, match="no such table: temp.tmp_fact"):
        ingest_fact(engine, config, batch_id="B_TEMP")


def test_ingest_fact_handles_existing_fact_table_when_inspection_misses_it(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE staging (
                    batch_id TEXT,
                    measure_value REAL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE fact (
                    batch_id TEXT,
                    measure_value REAL,
                    Insert_Date TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                "INSERT INTO staging (batch_id, measure_value) VALUES ('B9', 9.0)"
            )
        )

    original_get_existing_columns = fact_loader_module._get_existing_columns
    calls = {"fact": 0}

    def fake_get_existing_columns(engine_arg, table_name):
        table_ref = (
            table_name
            if isinstance(table_name, TableRef)
            else fact_loader_module._parse_table_ref(table_name)
        )
        if table_ref.table == "fact":
            calls["fact"] += 1
            if calls["fact"] == 1:
                return set()
        return original_get_existing_columns(engine_arg, table_name)

    monkeypatch.setattr(
        "dataprepkit.fact_loader._get_existing_columns", fake_get_existing_columns
    )

    config = FactConfig(
        batch=FactBatchMetadata(fact_table="fact", validations={}),
        dimensions=[],
        fact_columns=["measure_value"],
        source_table="staging",
        temp_table="tmp_fact",
        temp_columns={"batch_id": "TEXT", "measure_value": "REAL"},
    )

    ingest_fact(engine, config, batch_id="B9")
    with engine.connect() as conn:
        value = conn.execute(
            text("SELECT measure_value FROM fact WHERE batch_id='B9'")
        ).scalar_one()
    assert value == 9.0


def test_ensure_fact_table_ignores_duplicate_column_from_stale_inspection(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE fact (
                    Organisation_Instance_Id INTEGER
                )
                """
            )
        )

    original_get_existing_columns = fact_loader_module._get_existing_columns
    calls = {"fact": 0}

    def fake_get_existing_columns(engine_arg, table_name):
        table_ref = (
            table_name
            if isinstance(table_name, TableRef)
            else fact_loader_module._parse_table_ref(table_name)
        )
        if table_ref.table == "fact":
            calls["fact"] += 1
            if calls["fact"] == 1:
                return set()
        return original_get_existing_columns(engine_arg, table_name)

    monkeypatch.setattr(
        "dataprepkit.fact_loader._get_existing_columns", fake_get_existing_columns
    )

    fact_loader_module._ensure_fact_table(
        engine,
        "fact",
        {"Organisation_Instance_Id": "BIGINT"},
    )


def test_assert_columns_unique_passes_for_unique_combinations():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE staging (
                    a TEXT,
                    b TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                "INSERT INTO staging (a, b) VALUES ('x', '1'), ('x', '2'), ('y', '1')"
            )
        )

    assert_columns_unique(engine, table_name="staging", schema="main", columns=["a", "b"])


def test_assert_columns_unique_raises_for_duplicate_combinations():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE staging (
                    a TEXT,
                    b TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                "INSERT INTO staging (a, b) VALUES ('x', '1'), ('x', '1')"
            )
        )

    with pytest.raises(RuntimeError, match="Duplicate values found"):
        assert_columns_unique(
            engine, table_name="staging", schema="main", columns=["a", "b"]
        )


def test_assert_columns_not_null_passes_when_no_nulls():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE staging (
                    a TEXT,
                    b TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                "INSERT INTO staging (a, b) VALUES ('x', '1'), ('y', '2')"
            )
        )

    assert_columns_not_null(engine, table_name="staging", schema="main", columns=["a", "b"])


def test_assert_columns_not_null_raises_when_nulls_present():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE staging (
                    a TEXT,
                    b TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                "INSERT INTO staging (a, b) VALUES ('x', NULL), ('y', '2')"
            )
        )

    with pytest.raises(RuntimeError, match="Null values found"):
        assert_columns_not_null(
            engine, table_name="staging", schema="main", columns=["a", "b"]
        )


def test_assert_columns_have_single_distinct_row_passes():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE staging (
                    batch_id TEXT,
                    process_cd TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                "INSERT INTO staging (batch_id, process_cd) VALUES ('B1', 'P1'), ('B1', 'P1')"
            )
        )

    assert_columns_have_single_distinct_row(
        engine,
        table_name="staging",
        schema="main",
        columns=["batch_id", "process_cd"],
    )


def test_assert_columns_have_single_distinct_row_raises_when_multiple_distinct():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE staging (
                    batch_id TEXT,
                    process_cd TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                "INSERT INTO staging (batch_id, process_cd) VALUES ('B1', 'P1'), ('B2', 'P1')"
            )
        )

    with pytest.raises(RuntimeError, match="Expected a single distinct row"):
        assert_columns_have_single_distinct_row(
            engine,
            table_name="staging",
            schema="main",
            columns=["batch_id", "process_cd"],
        )


def test_assert_columns_have_single_distinct_row_raises_when_empty():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE staging (
                    batch_id TEXT,
                    process_cd TEXT
                )
                """
            )
        )

    with pytest.raises(RuntimeError, match="Expected a single distinct row"):
        assert_columns_have_single_distinct_row(
            engine,
            table_name="staging",
            schema="main",
            columns=["batch_id", "process_cd"],
        )


def test_apply_fact_table_comments_executes_for_mssql():
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

    fact_loader_module._apply_fact_table_comments(
        engine,
        TableRef(schema="Facts", table="qd_fact"),
        table_comment="Fact description",
        column_comments={"Batch_Id": "Batch identifier"},
    )

    assert len(engine.conn.calls) == 1
    sql, params = engine.conn.calls[0]
    assert "sp_updateextendedproperty" in sql
    assert "sp_addextendedproperty" in sql
    assert params["schema"] == "Facts"
    assert params["table"] == "qd_fact"


def test_apply_fact_table_comments_skips_for_non_mssql():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    # Should be a no-op for sqlite and not raise.
    fact_loader_module._apply_fact_table_comments(
        engine,
        "fact",
        table_comment="ignored",
        column_comments={"Batch_Id": "ignored"},
    )


