from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import hashlib
import re

from sqlalchemy import Engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError

from dataprepkit.helpers.schema import ensure_schema_exists


class HashMismatchError(RuntimeError):
    pass


class MissingStageFileError(RuntimeError):
    pass


@dataclass(frozen=True)
class TableRef:
    table: str
    schema: str | None = None


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
    internal_columns: Mapping[str, str] = field(default_factory=dict[str, str])
    join_chain: Sequence["DimensionJoinSpec"] = field(default_factory=list["DimensionJoinSpec"])


@dataclass
class ExtraColumnSpec:
    dim_table: str
    staging_columns: Sequence[str]
    dim_columns: Sequence[str]
    column: str
    dim_column: str
    filter_target_current: bool = True
    require_not_null: bool = False


@dataclass
class FactBatchMetadata:
    fact_table: str | TableRef
    validations: Dict[str, str]  # {filename_col: hash_col}
    batch_id: str | None = None


@dataclass
class FactConfig:
    batch: FactBatchMetadata
    dimensions: Sequence[DimensionJoinSpec]
    fact_columns: Sequence[str]
    source_table: str | TableRef
    temp_table: str | TableRef
    temp_columns: Mapping[str, Optional[str]]
    extra_columns: Sequence[ExtraColumnSpec] = field(default_factory=list[ExtraColumnSpec])
    batch_id_column_name: str = "batch_id"
    batch_id_column_type: Optional[str] = "NVARCHAR(4000)"


@dataclass
class StageFileSpec:
    organisation_column: str
    filename_column: str
    hash_column: str
    path_columns: Sequence[str] = field(default_factory=list[str])


def _parse_table_ref(table_name: str | TableRef) -> TableRef:
    if isinstance(table_name, TableRef):
        return table_name
    if "." not in table_name:
        return TableRef(table=table_name)
    schema, table = table_name.split(".", 1)
    return TableRef(table=table, schema=schema)


def _quote_mssql_identifier(identifier: str) -> str:
    return f"[{identifier.replace(']', ']]')}]"


def _render_table_name(engine: Engine, table_name: str | TableRef) -> str:
    table_ref = _parse_table_ref(table_name)
    if engine.dialect.name == "mssql":
        table_sql = _quote_mssql_identifier(table_ref.table)
        if table_ref.schema:
            return f"{_quote_mssql_identifier(table_ref.schema)}.{table_sql}"
        return table_sql
    if table_ref.schema:
        return f"{table_ref.schema}.{table_ref.table}"
    return table_ref.table


def _quote_identifier(engine: Engine, identifier: str) -> str:
    if engine.dialect.name == "mssql":
        return _quote_mssql_identifier(identifier)
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def _resolve_table_ref(
    table_name: str | TableRef, schema: str | None = None
) -> TableRef:
    table_ref = _parse_table_ref(table_name)
    if schema is None:
        return table_ref
    if table_ref.schema and table_ref.schema != schema:
        raise ValueError("Conflicting schema values provided for table.")
    return TableRef(table=table_ref.table, schema=schema)


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
    table_name: str | TableRef,
    spec: StageFileSpec,
    filters: Optional[Dict[str, str]] = None,
) -> Iterable[Tuple[str, str, Optional[str], Mapping[str, str]]]:
    table_name_sql = _render_table_name(engine, table_name)
    select_cols = [
        spec.organisation_column,
        spec.filename_column,
        spec.hash_column,
        *spec.path_columns,
    ]
    query = f"SELECT DISTINCT {', '.join(select_cols)} FROM {table_name_sql}"
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
    engine: Engine, table_name: str | TableRef, columns: Mapping[str, Optional[str]]
) -> None:
    if not any(dtype for dtype in columns.values()):
        return
    table_ref = _parse_table_ref(table_name)
    schema = table_ref.schema
    table_name_sql = _render_table_name(engine, table_ref)
    if schema:
        ensure_schema_exists(engine, schema)
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name_sql}"))
        column_defs = _render_column_defs(columns, engine)
        create_sql = f"CREATE TABLE {table_name_sql} ({column_defs})"
        conn.execute(text(create_sql))


def _get_existing_columns(engine: Engine, table_name: str | TableRef) -> set[str]:
    inspector = inspect(engine)
    table_ref = _parse_table_ref(table_name)
    schema = table_ref.schema
    table = table_ref.table
    if not inspector.has_table(table, schema):
        return set()
    return {col["name"] for col in inspector.get_columns(table, schema)}


def _has_current_indicator(engine: Engine, table_name: str | TableRef) -> bool:
    columns = _get_existing_columns(engine, table_name)
    return any(col.lower() == "current_ind" for col in columns)


def _ensure_fact_table(
    engine: Engine, table_name: str | TableRef, columns: Mapping[str, Optional[str]]
) -> None:
    if not columns:
        return
    table_ref = _parse_table_ref(table_name)
    schema = table_ref.schema
    table_name_sql = _render_table_name(engine, table_ref)
    if schema:
        ensure_schema_exists(engine, schema)
    existing = _get_existing_columns(engine, table_name)
    if not existing:
        column_defs = _render_column_defs(columns, engine)
        try:
            with engine.begin() as conn:
                conn.execute(text(f"CREATE TABLE {table_name_sql} ({column_defs})"))
            return
        except SQLAlchemyError as exc:
            # Fabric/MSSQL metadata inspection can lag; if the table already exists,
            # continue and reconcile missing columns instead of failing hard.
            error_text = str(exc).lower()
            if (
                "already an object named" not in error_text
                and "already exists" not in error_text
            ):
                raise
            existing = _get_existing_columns(engine, table_name)
    existing_lower = {name.lower() for name in existing}
    new_columns = {
        name: dtype
        for name, dtype in columns.items()
        if name.lower() not in existing_lower
    }
    if not new_columns:
        return
    with engine.begin() as conn:
        for name, dtype in new_columns.items():
            col_type = dtype if dtype else _column_type_for_engine(engine)
            try:
                conn.execute(text(f"ALTER TABLE {table_name_sql} ADD {name} {col_type}"))
            except SQLAlchemyError as exc:
                error_text = str(exc).lower()
                duplicate_column_error = (
                    "specified more than once" in error_text
                    or "duplicate column name" in error_text
                    or "column names in each table must be unique" in error_text
                    or "already exists" in error_text
                )
                if not duplicate_column_error:
                    raise


def verify_stage_file_hashes(
    engine: Engine,
    table_name: str | TableRef | None,
    spec: StageFileSpec,
    *,
    schema: str | None = None,
    table: str | None = None,
    path_resolver: Optional[
        Callable[[str, str, Mapping[str, str]], str]
    ] = None,
    base_path: Optional[str] = None,
) -> None:
    """
    Ensure each staging row file hash matches the checksum of the referenced file.

    Parameters:
        engine: SQLAlchemy engine bound to the staging database.
        table_name: Staging table name, optionally schema-qualified.
        schema: Optional explicit schema for the staging table.
        table: Optional explicit table name for the staging table.
        spec: Columns describing organisation, filename, and hash.
        path_resolver: Callable mapping (organisation, filename) -> path to actual file.
    """
    if table is not None:
        if table_name is not None:
            raise ValueError("Provide either table_name or table/schema, not both.")
        stage_table = TableRef(table=table, schema=schema)
    else:
        if table_name is None:
            raise ValueError("Either table_name or table must be provided.")
        stage_table = _parse_table_ref(table_name)
        if schema is not None and stage_table.schema and stage_table.schema != schema:
            raise ValueError("Conflicting schema values provided for staging table.")
        if schema is not None:
            stage_table = TableRef(table=stage_table.table, schema=schema)

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
        engine, stage_table, spec
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


def assert_columns_unique(
    engine: Engine,
    table_name: str | TableRef,
    *,
    schema: str | None = None,
    columns: Sequence[str],
) -> None:
    if not columns:
        raise ValueError("columns must not be empty.")
    table_ref = _resolve_table_ref(table_name, schema)
    table_sql = _render_table_name(engine, table_ref)
    rendered_cols = [_quote_identifier(engine, col) for col in columns]
    cols_sql = ", ".join(rendered_cols)
    query = text(
        f"""
        SELECT COUNT(1)
        FROM (
            SELECT 1
            FROM {table_sql}
            GROUP BY {cols_sql}
            HAVING COUNT(1) > 1
        ) AS duplicates
        """
    )
    with engine.connect() as conn:
        duplicate_groups = conn.execute(query).scalar_one()
    if duplicate_groups:
        raise RuntimeError(
            f"Duplicate values found for column set {list(columns)} in {table_sql}."
        )


def assert_columns_not_null(
    engine: Engine,
    table_name: str | TableRef,
    *,
    schema: str | None = None,
    columns: Sequence[str],
) -> None:
    if not columns:
        raise ValueError("columns must not be empty.")
    table_ref = _resolve_table_ref(table_name, schema)
    table_sql = _render_table_name(engine, table_ref)
    rendered_cols = [_quote_identifier(engine, col) for col in columns]
    where_clause = " OR ".join(f"{col} IS NULL" for col in rendered_cols)
    query = text(f"SELECT COUNT(1) FROM {table_sql} WHERE {where_clause}")
    with engine.connect() as conn:
        null_count = conn.execute(query).scalar_one()
    if null_count:
        raise RuntimeError(
            f"Null values found in required columns {list(columns)} for {table_sql}."
        )


def ingest_fact(engine: Engine, config: FactConfig, *, batch_id: str, mode: str = "replace") -> None:
    base_cols = list(config.temp_columns.keys())
    temp_columns = dict(config.temp_columns)
    source_table_sql = _render_table_name(engine, config.source_table)
    temp_table_sql = _render_table_name(engine, config.temp_table)
    fact_table_sql = _render_table_name(engine, config.batch.fact_table)

    def _register_dimension_columns(spec: DimensionJoinSpec) -> None:
        surrogate_col = spec.surrogate_column or _default_surrogate_column_name(
            spec.dim_table
        )
        temp_columns.setdefault(surrogate_col, "BIGINT")
        for col in spec.add_columns:
            temp_columns.setdefault(col, "BIGINT")
        for col in spec.internal_columns:
            temp_columns.setdefault(col, "BIGINT")
        for chained in spec.join_chain:
            _register_dimension_columns(chained)

    for dimension in config.dimensions:
        _register_dimension_columns(dimension)
    for extra in config.extra_columns:
        temp_columns.setdefault(extra.column, "BIGINT")
    _ensure_temp_table(engine, config.temp_table, temp_columns)
    if not base_cols:
        raise RuntimeError("No base columns defined for temp table copy")
    cols = ", ".join(base_cols)
    insert_temp = text(
        f"""
        INSERT INTO {temp_table_sql} ({cols})
        SELECT {cols} FROM {source_table_sql}
        """
    )
    with engine.begin() as conn:
        conn.execute(insert_temp)

    def _apply_dimension(base_table: str, spec: DimensionJoinSpec) -> None:
        predicate = " AND ".join(
            f"{base_table}.{s} = d.{dcol}"
            for s, dcol in zip(spec.staging_columns, spec.dim_columns)
        )
        dim_table_columns = _get_existing_columns(engine, spec.dim_table)
        surrogate_col = spec.surrogate_column or _default_surrogate_column_name(
            spec.dim_table
        )
        surrogate_dim_col: str | None = None
        if spec.surrogate_column and spec.surrogate_column in dim_table_columns:
            surrogate_dim_col = spec.surrogate_column
        elif "surrogate_key" in dim_table_columns:
            surrogate_dim_col = "surrogate_key"
        current_clause = ""
        if spec.filter_target_current and _has_current_indicator(engine, spec.dim_table):
            current_clause = " AND (d.current_ind = 1 OR d.current_ind IS NULL)"
        set_clauses: list[str] = []
        if surrogate_dim_col:
            set_clauses.append(
                f"{surrogate_col} = (SELECT d.{surrogate_dim_col} FROM {spec.dim_table} d WHERE {predicate}{current_clause})"
            )
        for col, dim_col in spec.add_columns.items():
            if col == surrogate_col and surrogate_dim_col:
                continue
            set_clauses.append(
                f"{col} = (SELECT d.{dim_col} FROM {spec.dim_table} d WHERE {predicate}{current_clause})"
            )
        for col, dim_col in spec.internal_columns.items():
            set_clauses.append(
                f"{col} = (SELECT d.{dim_col} FROM {spec.dim_table} d WHERE {predicate}{current_clause})"
            )
        if set_clauses:
            update_stmt = text(
                f"""
                UPDATE {base_table}
                SET {', '.join(set_clauses)}
                WHERE EXISTS (
                    SELECT 1 FROM {spec.dim_table} d
                    WHERE {predicate}{current_clause}
                )
                """
            )
            with engine.begin() as conn:
                conn.execute(update_stmt)
        if spec.require_not_null:
            cond = " OR ".join(f"{col} IS NULL" for col in spec.require_not_null)
            with engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT COUNT(1) FROM {base_table} WHERE {cond}")
                ).scalar()
            if result:
                raise RuntimeError(
                    f"Null values found for required columns {spec.require_not_null}"
                )

    def _apply_extra_column(base_table: str, spec: ExtraColumnSpec) -> None:
        predicate = " AND ".join(
            f"{base_table}.{s} = d.{dcol}"
            for s, dcol in zip(spec.staging_columns, spec.dim_columns)
        )
        current_clause = ""
        if spec.filter_target_current and _has_current_indicator(engine, spec.dim_table):
            current_clause = " AND (d.current_ind = 1 OR d.current_ind IS NULL)"
        set_clause = (
            f"{spec.column} = (SELECT d.{spec.dim_column} FROM {spec.dim_table} d WHERE {predicate}{current_clause})"
        )
        update_stmt = text(
            f"""
            UPDATE {base_table}
            SET {set_clause}
            WHERE EXISTS (
                SELECT 1 FROM {spec.dim_table} d
                WHERE {predicate}{current_clause}
            )
            """
        )
        with engine.begin() as conn:
            conn.execute(update_stmt)
        if spec.require_not_null:
            with engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT COUNT(1) FROM {base_table} WHERE {spec.column} IS NULL")
                ).scalar()
            if result:
                raise RuntimeError(
                    f"Null values found for required column {spec.column}"
                )

    for dimension in config.dimensions:
        _apply_dimension(temp_table_sql, dimension)
        for extra in config.extra_columns:
            if extra.dim_table == dimension.dim_table:
                _apply_extra_column(temp_table_sql, extra)
        for chained in dimension.join_chain:
            _apply_dimension(temp_table_sql, chained)
            for extra in config.extra_columns:
                if extra.dim_table == chained.dim_table:
                    _apply_extra_column(temp_table_sql, extra)

    fact_columns_types = {
        col: temp_columns.get(col) for col in config.fact_columns
    }
    fact_columns_types.setdefault(
        config.batch_id_column_name, config.batch_id_column_type
    )
    fact_columns_types.setdefault("Insert_Date", "DATETIME2(3)")
    _ensure_fact_table(engine,
                       config.batch.fact_table,
                       fact_columns_types)

    try:
        with engine.begin() as conn:
            insert_cols = ", ".join(
                [config.batch_id_column_name] + config.fact_columns + ["Insert_Date"]
            )
            select_cols = ", ".join(config.fact_columns)
            select_clause = f"SELECT :batch_id, {select_cols}, CURRENT_TIMESTAMP FROM {temp_table_sql}"
            insert_sql = text(
                f"""
                INSERT INTO {fact_table_sql} ({insert_cols})
                {select_clause}
                """
            )
            conn.execute(insert_sql, {"batch_id": batch_id})
    except SQLAlchemyError as exc:
        raise RuntimeError("fact insert failed") from exc
