"""Example showing how to verify staging file hashes and build a temp fact table."""

import glob
from pathlib import Path

from dataprepkit.fact_loader import (
    DimensionJoinSpec,
    FactBatchMetadata,
    FactConfig,
    MissingStageFileError,
    StageFileSpec,
    ingest_fact,
    verify_stage_file_hashes,
)
from dataprepkit.helpers.connectors.fabric import create_engine_for_fabric, validate
from dataprepkit.storage import get_sql_db_endpoint

sql_db_conn_details = get_sql_db_endpoint(
    "Ocean_Dimension_Ingestion_PROD",
    "mydb",
)

engine = create_engine_for_fabric(
    sql_db_conn_details.server_fqdn,
    sql_db_conn_details.database_name,
)
assert validate(engine)

spec = StageFileSpec(
    organisation_column="Organisation_Cd",
    filename_column="Filename",
    hash_column="file_hash_md5",
    path_columns=["Process_Cd", "Submission_Period_Cd"],
)

BASE_PATH = Path("/mounts/data/Files/data collections")

def find_stage_file(org: str, filename: str, meta: dict[str, str]) -> str:
    segments = [
        f"Status=files_error_fix",
        f"Process_Cd={meta['Process_Cd']}",
        f"Submission_Period_Cd={meta['Submission_Period_Cd']}",
        f"Organisation_Cd={org}",
    ]
    directory = BASE_PATH.joinpath(*segments)
    pattern = directory / f"{filename}*"
    matches = glob.glob(str(pattern))
    if not matches:
        raise MissingStageFileError(f"no file for {filename} under {directory}")
    return matches[0]

verify_stage_file_hashes(
    engine,
    "Staging.qd_stg",
    spec,
    path_resolver=find_stage_file,
)

# After verification, build and load the temporary fact table.
fact_config = FactConfig(
    batch=FactBatchMetadata(
        fact_table="Dimensions.fact",
        batch_id="BATCH123",
        audit_columns={
            # staging_col : fact_col
            "batch_id": "batch_id"
        },
        validations={},
    ),
    dimensions=[
        DimensionJoinSpec(
            dim_table="Dimensions.tbl_d_company",
            staging_columns=["Organisation_Cd"],
            dim_columns=["Organisation_Cd"],
            surrogate_column="company_sk",
            add_columns={
                "Company_Instance_Id": "Company_Instance_Id",
                "Company_Id": "Company_Id",
            },
            require_not_null=["company_sk"],
        ),
    ],
    fact_columns=[
        "batch_id",
        "company_sk",
        "measure_value",
        "Company_Instance_Id",
        "Company_Id",
    ],
    # Input staging table holding the raw snapshot; this is passed directly.
    source_table="Staging.qd_stg",
    # Temporary fact table where surrogate lookups happen before the final insert.
    temp_table="Staging.qd_tmp_fact",
    temp_columns={
        "batch_id": "NVARCHAR(4000)",
        "Organisation_Cd": "NVARCHAR(4000)",
        "measure_value": "FLOAT",
        "Company_Instance_Id": "BIGINT",
        "Company_Id": "BIGINT",
    },
)

ingest_fact(engine, fact_config)
