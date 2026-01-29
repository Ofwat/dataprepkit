"""Example showing how to verify staging file hashes before fact loads."""

import glob
from pathlib import Path

from dataprepkit.fact_loader import (
    MissingStageFileError,
    StageFileSpec,
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
