from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import hashlib

from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError

from dataprepkit.helpers.schema import ensure_schema_exists


class HashMismatchError(RuntimeError):
    pass


class MissingStageFileError(RuntimeError):
    pass


@dataclass
class DimensionJoinSpec:
    dim_table: str
    staging_columns: Sequence[str]
    dim_columns: Sequence[str]
    extra_columns: Sequence[str] = field(default_factory=list[str])
    add_columns: Mapping[str, str] = field(default_factory=dict[str, str])
    require_not_null: Sequence[str] = field(default_factory=list[str])


@dataclass
class FactBatchMetadata:
    fact_table: str
    batch_id: str
    audit_columns: Dict[str, str]
    validations: Dict[str, str]  # {filename_col: hash_col}


@dataclass
class FactConfig:
    batch: FactBatchMetadata
    dimensions: Sequence[DimensionJoinSpec]
    fact_columns: Sequence[str]
    source_table: str
    temp_table: str
    temp_columns: Mapping[str, Optional[str]]


@dataclass
class StageFileSpec:
    organisation_column: str
    filename_column: str
    hash_column: str
    path_columns: Sequence[str] = field(default_factory=list[str])


def _compute_md5(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()


def _list_stage_files(
    engine: Engine,
    table_name: str,
    spec: StageFileSpec,
    filters: Optional[Dict[str, str]] = None,
) -> Iterable[Tuple[str, str, Optional[str], Mapping[str, str]]]:
    select_cols = [
        spec.organisation_column,
        spec.filename_column,
        spec.hash_column,
        *spec.path_columns,
    ]
    query = f"SELECT DISTINCT {', '.join(select_cols)} FROM {table_name}"
    params = {}
    if filters:
        where_clause = " AND ".join(f"{col} = :{col}" for col in filters)
        query += f" WHERE {where_clause}"
        params.update(filters)
    with engine.connect() as conn:
        for row in conn.execute(text(query), params).mappings():
            extra = {col: row[col] for col in spec.path_columns}
            yield (
                row[spec.organisation_column],
                row[spec.filename_column],
                row.get(spec.hash_column),
                extra,
            )


def _render_column_defs(columns: Mapping[str, Optional[str]]) -> str:
    return ", ".join(f"{name} {dtype}" for name, dtype in columns.items() if dtype)


def _ensure_temp_table(
    engine: Engine, table_name: str, columns: Mapping[str, Optional[str]]
) -> None:
    if not any(dtype for dtype in columns.values()):
        return
    schema = table_name.split(".")[0] if "." in table_name else None
    if schema:
        ensure_schema_exists(engine, schema)
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        create_sql = f"CREATE TABLE {table_name} ({_render_column_defs(columns)})"
        conn.execute(text(create_sql))


def verify_stage_file_hashes(
    engine: Engine,
    table_name: str,
    spec: StageFileSpec,
    *,
    path_resolver: Optional[
        Callable[[str, str, Mapping[str, str]], str]
    ] = None,
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
        def _base_resolver(svc, fname, extra=None):
            full = Path(base_path) / svc / fname
            if not full.exists():
                raise FileNotFoundError(f"{full} not found")
            return str(full)

        resolver = _base_resolver
    if resolver is None:
        raise ValueError("Either path_resolver or base_path must be provided.")
    for organisation, filename, expected_hash, row_meta in _list_stage_files(
        engine, table_name, spec
    ):
        if expected_hash is None:
            raise HashMismatchError(
                f"Missing expected hash for {filename} (org={organisation})"
            )
        try:
            actual_path = resolver(organisation, filename, row_meta)
        except FileNotFoundError as exc:
            raise MissingStageFileError(str(exc)) from exc
        actual_hash = _compute_md5(actual_path)
        if actual_hash != expected_hash:
            raise HashMismatchError(
                f"{filename} hash mismatch (expected {expected_hash}, got {actual_hash})"
            )


def ingest_fact(engine: Engine, config: FactConfig, *, mode: str = "replace") -> None:
    _ensure_temp_table(engine, config.temp_table, config.temp_columns)
    cols_list = list(config.temp_columns.keys())
    if cols_list:
        cols = ", ".join(cols_list)
        insert_temp = text(
            f"""
            INSERT INTO {config.temp_table} ({cols})
            SELECT {cols} FROM {config.source_table}
            """
        )
    else:
        insert_temp = text(
            f"""
            INSERT INTO {config.temp_table}
            SELECT * FROM {config.source_table}
            """
        )
    with engine.begin() as conn:
        conn.execute(insert_temp)

    for dimension in config.dimensions:
        join_clause = " AND ".join(
            f"fs.{s} = d.{dcol}"
            for s, dcol in zip(dimension.staging_columns, dimension.dim_columns)
        )
        extra_sets = ", ".join(
            f"{col} = d.{field}" for col, field in dimension.add_columns.items()
        )
        set_clause = f"{dimension.dim_table}_sk = d.surrogate_key"
        if extra_sets:
            set_clause = f"{set_clause}, {extra_sets}"
        update_stmt = text(
            f"""
            UPDATE {config.temp_table} fs
            SET {set_clause}
            FROM {config.temp_table} fs
            JOIN {dimension.dim_table} d
              ON {join_clause}
            WHERE d.current_ind = 1
            """
        )
        with engine.begin() as conn:
            conn.execute(update_stmt)
        if dimension.require_not_null:
            cond = " OR ".join(f"{col} IS NULL" for col in dimension.require_not_null)
            with engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT COUNT(1) FROM {config.temp_table} WHERE {cond}")
                ).scalar()
            if result:
                raise RuntimeError(
                    f"Null values found for required columns {dimension.require_not_null}"
                )

    try:
        with engine.begin() as conn:
            insert_cols = ", ".join(config.fact_columns)
            select_cols = ", ".join(config.fact_columns)
            insert_sql = text(
                f"""
                INSERT INTO {config.batch.fact_table} ({insert_cols})
                SELECT {select_cols} FROM {config.temp_table}
                """
            )
            conn.execute(insert_sql)
    except SQLAlchemyError as exc:
        raise RuntimeError("fact insert failed") from exc
