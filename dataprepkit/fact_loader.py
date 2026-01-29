from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import hashlib

import pandas as pd
from sqlalchemy import Engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError

from dataprepkit.helpers.schema import ensure_schema_exists
from dataprepkit.helpers.staging import stage_dataframe


class HashMismatchError(RuntimeError):
    pass


@dataclass
class DimensionJoinSpec:
    dim_table: str
    staging_columns: Sequence[str]
    dim_columns: Sequence[str]
    extra_columns: Sequence[str] = field(default_factory=list[str])


@dataclass
class FactBatchMetadata:
    fact_table: str
    staging_table: str
    batch_id: str
    audit_columns: Dict[str, str]
    validations: Dict[str, str]  # {filename_col: hash_col}


@dataclass
class FactConfig:
    batch: FactBatchMetadata
    dimensions: Sequence[DimensionJoinSpec]
    fact_columns: Sequence[str]
    staging_reader: Callable[[], pd.DataFrame]


@dataclass
class StageFileSpec:
    organisation_column: str
    filename_column: str
    hash_column: str


def compute_md5(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()


def _split_table_name(name: str) -> tuple[str | None, str]:
    if "." in name:
        schema, tbl = name.split(".", 1)
        return schema.strip("[]\""), tbl.strip("[]\"")
    return None, name


def discover_stage_file_spec(
    engine: Engine, table_name: str, spec: StageFileSpec
) -> StageFileSpec:
    schema, table = _split_table_name(table_name)
    inspector = inspect(engine)
    columns = {col["name"] for col in inspector.get_columns(table, schema=schema)}
    for attr in ("organisation_column", "filename_column", "hash_column"):
        col = getattr(spec, attr)
        if col not in columns:
            raise ValueError(f"Missing column {col} in staging table {table_name}")
    return spec


def list_stage_files(
    engine: Engine,
    table_name: str,
    spec: StageFileSpec,
    filters: Optional[Dict[str, str]] = None,
) -> Iterable[Tuple[str, str, Optional[str]]]:
    select_cols = [spec.organisation_column, spec.filename_column, spec.hash_column]
    query = f"SELECT {', '.join(select_cols)} FROM {table_name}"
    params = {}
    if filters:
        where_clause = " AND ".join(f"{col} = :{col}" for col in filters)
        query += f" WHERE {where_clause}"
        params.update(filters)
    with engine.connect() as conn:
        for row in conn.execute(text(query), params):
            yield row


def verify_stage_file_hashes(
    engine: Engine,
    table_name: str,
    spec: StageFileSpec,
    *,
    path_resolver: Optional[Callable[[str, str], str]] = None,
    base_path: Optional[str] = None,
) -> None:
    """
    Ensure each staging row file hash matches the checksum of the referenced file.

    Parameters:
        engine: SQLAlchemy engine bound to the staging database.
        table_name: Fully qualified staging table name (schema.table).
        spec: Columns describing organisation, filename, and hash.
        path_resolver: Callable mapping (organisation, filename) -> path to actual file.
    """
    resolver = path_resolver
    if resolver is None and base_path is not None:
        resolver = lambda org, filename: str(Path(base_path) / org / filename)
    if resolver is None:
        raise ValueError("Either path_resolver or base_path must be provided.")
    for organisation, filename, expected_hash in list_stage_files(
        engine, table_name, spec
    ):
        if expected_hash is None:
            raise HashMismatchError(
                f"Missing expected hash for {filename} (org={organisation})"
            )
        actual_path = resolver(organisation, filename)
        actual_hash = compute_md5(actual_path)
        if actual_hash != expected_hash:
            raise HashMismatchError(
                f"{filename} hash mismatch (expected {expected_hash}, got {actual_hash})"
            )


def validate_hash(engine: Engine, metadata: FactBatchMetadata) -> None:
    # Placeholder: expect validations contains 'file_path' -> 'expected_hash'
    query = text(
        "SELECT Filename, file_hash_md5 FROM fact_batch_files WHERE batch_id = :batch"
    )
    with engine.connect() as conn:
        result = conn.execute(query, {"batch": metadata.batch_id}).fetchone()
    if not result:
        raise RuntimeError("missing batch metadata")
    actual_hash = compute_md5(result[0])
    expected_hash = result[1]
    if actual_hash != expected_hash:
        raise HashMismatchError("hash mismatch")


def ingest_fact(engine: Engine, config: FactConfig, *, mode: str = "replace") -> None:
    # 1. Validate
    validate_hash(engine, config.batch)

    # 2. Stage
    df = config.staging_reader()
    ensure_schema_exists(engine, config.batch.staging_table.split(".")[0])
    stage_dataframe(engine, config.batch.staging_table, df, if_exists="replace")

    # 3. Resolve surrogates
    for dimension in config.dimensions:
        join_clause = " AND ".join(
            f"fs.{s} = d.{dcol}"
            for s, dcol in zip(dimension.staging_columns, dimension.dim_columns)
        )
        update_stmt = text(
            f"""
            UPDATE {config.batch.staging_table} fs
            SET {dimension.dim_table}_sk = d.surrogate_key
            FROM {config.batch.staging_table} fs
            JOIN {dimension.dim_table} d
              ON {join_clause}
            WHERE d.current_ind = 1
            """
        )
        engine.execute(update_stmt)

    # 4. Insert facts
    try:
        with engine.begin() as conn:
            insert_cols = ", ".join(config.fact_columns)
            select_cols = ", ".join(config.fact_columns)
            insert_sql = text(
                f"""
                INSERT INTO {config.batch.fact_table} ({insert_cols})
                SELECT {select_cols} FROM {config.batch.staging_table}
                """
            )
            conn.execute(insert_sql)
    except SQLAlchemyError as exc:
        raise RuntimeError("fact insert failed") from exc
