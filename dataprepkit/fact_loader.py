from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Sequence

import hashlib

import pandas as pd
from sqlalchemy import Engine, text
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


def compute_md5(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()


def validate_hash(engine: Engine, metadata: FactBatchMetadata) -> None:
    # Placeholder: expect validations contains 'file_path' -> 'expected_hash'
    query = text(
        "SELECT Filename, file_hash_md5 FROM fact_batch_files WHERE batch_id = :batch"
    )
    result = engine.execute(query, {"batch": metadata.batch_id}).fetchone()
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
