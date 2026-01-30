from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import hashlib
import re

from sqlalchemy import Engine, Inspector, text
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
    surrogate_column: str | None = None
    filter_target_current: bool = True
    surrogate_column: str | None = None


@dataclass
class FactBatchMetadata:
    fact_table: str
    validations: Dict[str, str]  # {filename_col: hash_col}


@dataclass
class FactConfig:
    batch: FactBatchMetadata
    dimensions: Sequence[DimensionJoinSpec]
    fact_columns: Sequence[str]
    source_table: str
    temp_table: str
    temp_columns: Mapping[str, Optional[str]]
    batch_id_column_name: str = "batch_id"
    batch_id_column_type: Optional[str] = "NVARCHAR(4000)"


@dataclass
class StageFileSpec:
    organisation_column: str
    filename_column: str
    hash_column: str
    path_columns: Sequence[str] = field(default_factory=list[str])


def _default_surrogate_column_name(dim_table: str) -> str:
    table = dim_table.split(".")[-1]
    match = re.search(r"tbl_[^_]*_(.+)$", table, re.IGNORECASE)
    if match:
        base = match.group(1)
    elif table.lower().startswith("tbl_"):
        base = table[len("tbl_") :]
    else:
        base = table
    base = base.lstrip("d_")
    return f"{base}_sk"


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


_DEFAULT_COL_TYPE = "TEXT"


def _column_type_for_engine(engine: Engine) -> str:
    dialect = engine.dialect.name
    if dialect == "mssql":
        return "NVARCHAR(4000)"
    if dialect == "sqlite":
        return "TEXT"
    return "TEXT"


def _render_column_defs(
    columns: Mapping[str, Optional[str]], engine: Engine
) -> str:
    defs = []
    for name, dtype in columns.items():
        col_type = dtype if dtype else _column_type_for_engine(engine)
        defs.append(f"{name} {col_type}")
    return ", ".join(defs)


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
        column_defs = _render_column_defs(columns, engine)
        create_sql = f"CREATE TABLE {table_name} ({column_defs})"
        conn.execute(text(create_sql))


def _get_existing_columns(engine: Engine, table_name: str) -> set[str]:
    inspector = Inspector.from_engine(engine)
    schema = table_name.split(".")[0] if "." in table_name else None
    table = table_name.split(".")[-1]
    if not inspector.has_table(table, schema):
        return set()
    return {col["name"] for col in inspector.get_columns(table, schema)}


def _ensure_fact_table(
    engine: Engine, table_name: str, columns: Mapping[str, Optional[str]]
) -> None:
    if not columns:
        return
    schema = table_name.split(".")[0] if "." in table_name else None
    if schema:
        ensure_schema_exists(engine, schema)
    existing = _get_existing_columns(engine, table_name)
    if not existing:
        column_defs = _render_column_defs(columns, engine)
        with engine.begin() as conn:
            conn.execute(text(f"CREATE TABLE {table_name} ({column_defs})"))
        return
    new_columns = {
        name: dtype for name, dtype in columns.items() if name not in existing
    }
    if not new_columns:
        return
    with engine.begin() as conn:
        for name, dtype in new_columns.items():
            col_type = dtype if dtype else _column_type_for_engine(engine)
            conn.execute(text(f"ALTER TABLE {table_name} ADD {name} {col_type}"))


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


def ingest_fact(engine: Engine, config: FactConfig, *, batch_id: str, mode: str = "replace") -> None:
    base_cols = list(config.temp_columns.keys())
    temp_columns = dict(config.temp_columns)
    for dimension in config.dimensions:
        surrogate_col = dimension.surrogate_column or _default_surrogate_column_name(
            dimension.dim_table
        )
        temp_columns.setdefault(surrogate_col, "BIGINT")
        for col in dimension.add_columns:
            temp_columns.setdefault(col, "BIGINT")
    _ensure_temp_table(engine, config.temp_table, temp_columns)
    if not base_cols:
        raise RuntimeError("No base columns defined for temp table copy")
    cols = ", ".join(base_cols)
    insert_temp = text(
        f"""
        INSERT INTO {config.temp_table} ({cols})
        SELECT {cols} FROM {config.source_table}
        """
    )
    with engine.begin() as conn:
        conn.execute(insert_temp)

    for dimension in config.dimensions:
        predicate = " AND ".join(
            f"{config.temp_table}.{s} = d.{dcol}"
            for s, dcol in zip(dimension.staging_columns, dimension.dim_columns)
        )
        surrogate_col = dimension.surrogate_column or _default_surrogate_column_name(
            dimension.dim_table
        )
        current_clause = ""
        if dimension.filter_target_current:
            current_clause = " AND (d.current_ind = 1 OR d.current_ind IS NULL)"
        set_clauses = [
            f"{surrogate_col} = (SELECT d.surrogate_key FROM {dimension.dim_table} d WHERE {predicate}{current_clause})"
        ]
        for col, dim_col in dimension.add_columns.items():
            if col == surrogate_col:
                continue
            set_clauses.append(
                f"{col} = (SELECT d.{dim_col} FROM {dimension.dim_table} d WHERE {predicate}{current_clause})"
            )
        update_stmt = text(
            f"""
            UPDATE {config.temp_table}
            SET {', '.join(set_clauses)}
            WHERE EXISTS (
                SELECT 1 FROM {dimension.dim_table} d
                WHERE {predicate}{current_clause}
            )
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

    fact_columns_types = {
        col: temp_columns.get(col) for col in config.fact_columns
    }
    fact_columns_types.setdefault(
        config.batch_id_column_name, config.batch_id_column_type
    )
    fact_columns_types.setdefault("Insert_Date", "DATETIME2(3)")
    _ensure_fact_table(engine, config.batch.fact_table, fact_columns_types)

    try:
        with engine.begin() as conn:
            insert_cols = ", ".join(
                [config.batch_id_column_name] + config.fact_columns + ["Insert_Date"]
            )
            select_cols = ", ".join(config.fact_columns)
            select_clause = f"SELECT :batch_id, {select_cols}, CURRENT_TIMESTAMP FROM {config.temp_table}"
            insert_sql = text(
                f"""
                INSERT INTO {config.batch.fact_table} ({insert_cols})
                {select_clause}
                """
            )
            conn.execute(insert_sql, {"batch_id": batch_id})
    except SQLAlchemyError as exc:
        raise RuntimeError("fact insert failed") from exc
