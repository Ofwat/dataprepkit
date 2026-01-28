"""Metadata-driven orchestrator for dataprepkit dimensions."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Sequence, Literal

import logging
import pandas as pd
import uuid
import warnings
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from dataprepkit.scd2 import DEFAULT_SYSTEM_COLUMNS, apply_changes
from dataprepkit.storage import archive_dataframe_path


logger = logging.getLogger(__name__)

class DependencyJoin(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    table: str
    schema_name: str | None = Field(default=None, alias="schema")
    how: Literal["left", "inner"] = "left"
    filter_target_current: bool = True
    on: Sequence[Mapping[str, str]]
    select: Mapping[str, str]
    on_missing: Literal["error", "null"] = "error"
    where: Mapping[str, Sequence[str]] = Field(default_factory=dict)


class SchemaHandling(BaseModel):
    mode: Literal["suggest", "evolve"] = "suggest"


class RunPolicy(BaseModel):
    on_table_failure: Literal["continue", "abort"] = "abort"
    on_dependency_failure: Literal["skip_dependents", "abort"] = "skip_dependents"


DEFAULT_DATETIME_INPUT_FORMAT = "%d/%m/%Y %H:%M"


class ColumnSpec(BaseModel):
    type: str | None = None
    nullable: bool = True
    unique: bool = False
    default: str | None = None
    parse_format: str | None = None
    dayfirst: bool = False


class DimensionMetadata(BaseModel):
    """Defines the metadata required to load a single dimension table."""

    name: str
    target_table: str
    natural_key_cols: Sequence[str]
    natural_key_specs: Mapping[str, ColumnSpec] = Field(default_factory=dict[str, ColumnSpec])
    data_columns: Mapping[str, ColumnSpec]
    surrogate_key: str
    join_numeric_key: str
    filepath: str
    column_renames: Mapping[str, str] = Field(default_factory=dict[str, str])
    description: str | None = None
    schema_handling: SchemaHandling = Field(default_factory=SchemaHandling)
    dependencies: Sequence[DependencyJoin] = Field(default_factory=list)
    run_policy: RunPolicy = Field(default_factory=RunPolicy)
    processing_class: Callable[[pd.DataFrame], pd.DataFrame] | None = None
    archive_path: str | None = None
    target_schema: str | None = None
    archive_batch_id: str | None = None

    @field_validator("natural_key_cols")
    def must_define_key_columns(cls, value: Sequence[str]) -> Sequence[str]:
        if not value:
            raise ValueError("Must provide at least one column.")
        return value

    @field_validator("data_columns")
    def must_define_data_columns(cls, value: Mapping[str, ColumnSpec]) -> Mapping[str, ColumnSpec]:
        if not value:
            raise ValueError("Must provide at least one column.")
        return value


ROOT = Path(__file__).resolve().parents[1]
METADATA_REGISTRY: Dict[str, DimensionMetadata] = {}


def _normalize_column_specs(
    raw: Sequence[str] | Mapping[str, Any]
) -> Mapping[str, ColumnSpec]:
    if isinstance(raw, Mapping):
        normalized = {}
        for name, spec in raw.items():
            if isinstance(spec, ColumnSpec):
                normalized[name] = spec
            else:
                normalized[name] = ColumnSpec(**spec)
        return normalized
    return {
        name: ColumnSpec(type=None, nullable=False)
        for name in raw
    }


def register_metadata(
    name: str,
    metadata: Dict[str, object],
    *,
    archive_base_dir: str | None = None,
    archive_batch_id: str | None = None,
    batch_id: str | None = None,
) -> None:
    """Register metadata using a JSON-like dictionary for familiarity with old_code.py."""
    metadata = metadata.copy()
    schema = metadata.pop("target_schema", None)
    archive_config = metadata.pop("archive_config", None)
    resolved_batch_id = archive_batch_id or batch_id
    if not archive_config and archive_base_dir and resolved_batch_id:
        archive_config = {"base_dir": archive_base_dir, "batch_id": resolved_batch_id}
    if resolved_batch_id:
        metadata["archive_batch_id"] = resolved_batch_id
    if schema is None:
        schema = metadata.pop("schema", None)
    if "data_columns" in metadata:
        metadata["data_columns"] = _normalize_column_specs(
            metadata["data_columns"]  # type: ignore[arg-type]
        )
    if "natural_key_specs" in metadata:
        metadata["natural_key_specs"] = _normalize_column_specs(
            metadata["natural_key_specs"]  # type: ignore[arg-type]
        )
    if schema:
        table = metadata.get("target_table") or ""
        if schema and table and "." not in table:
            metadata["target_table"] = f"{schema}.{table}"
        metadata["target_schema"] = schema
    if archive_config:
        target_table = metadata.get("target_table", "")
        path_info = archive_dataframe_path(
            table_name=target_table,
            batch_id=archive_config.get("batch_id"),
            base_dir=archive_config.get("base_dir", ""),
        )
        metadata["archive_path"] = path_info.file_path
    METADATA_REGISTRY[name] = DimensionMetadata(name=name, **metadata)


def _register_default_metadata() -> None:
    sample_path = ROOT / "examples" / "dummy_dimension.csv"
    register_metadata(
        "dummy_dimension",
        {
            "target_table": "dimension",
            "natural_key_cols": ["natural_key"],
            "data_columns": {"data_column": {"type": "TEXT"}},
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": str(sample_path),
            "description": "Placeholder dimension used in demos.",
        },
    )


def _read_excel_one_sheet_openpyxl(filepath: str) -> pd.DataFrame:
    from openpyxl import load_workbook

    wb = load_workbook(filepath, data_only=True, read_only=True)

    if len(wb.sheetnames) != 1:
        raise ValueError(f"Expected exactly one sheet, found {len(wb.sheetnames)}")

    ws = wb[wb.sheetnames[0]]

    rows = list(ws.iter_rows(values_only=True))
    header, data = rows[0], rows[1:]

    df = pd.DataFrame(data, columns=header)
    df = df.fillna("")
    return df


def _default_csv_reader(filepath: str) -> pd.DataFrame:
    if filepath.lower().endswith(".xlsx"):
        return _read_excel_one_sheet_openpyxl(filepath)

    return pd.read_csv(
        filepath,
        header=0,
        encoding="utf-8",
        dtype=str,
        keep_default_na=False,
        na_values=["NULL"],
    )


_register_default_metadata()


def get_metadata(name: str) -> DimensionMetadata:
    """Return metadata record by name."""
    try:
        return METADATA_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(f"Unknown metadata entry: '{name}'") from exc


def run_dimension(
    engine: Engine,
    metadata_name: str,
    *,
    override_df: pd.DataFrame | None = None,
    csv_reader: callable = _default_csv_reader,
) -> pd.DataFrame:
    """
    Load a dimension based on metadata and apply SCD2 semantics.

    Parameters
    ----------
    engine
        SQLAlchemy engine.
    metadata_name
        Identifier in the metadata registry.
    override_df
        Optional DataFrame to bypass file reading (useful for tests).
    csv_reader
        Callable to read the source file (defaults to pandas.read_csv).
    """
    metadata = get_metadata(metadata_name)
    incoming = (
        override_df.copy()
        if override_df is not None
        else csv_reader(metadata.filepath)
    )
    logger.info(
        "Read raw snapshot for %s from %s (%d rows, columns=%s)",
        metadata_name,
        metadata.filepath,
        len(incoming),
        list(incoming.columns),
    )
    incoming = _cast_data_columns(incoming, metadata)
    _ensure_target_table(engine, metadata)
    _apply_table_description(engine, metadata)
    if metadata.column_renames:
        incoming = incoming.rename(columns=metadata.column_renames)
        if incoming.columns.duplicated().any():
            logger.error(
                "Column rename collision detected with %s",
                metadata.column_renames,
            )
            raise ValueError(
                "Column renames introduced duplicate column names; check metadata."
            )
    if metadata.processing_class:
        incoming = metadata.processing_class(incoming)
        logger.debug(
            "Applied processing_class for %s; first rows:\n%s",
            metadata_name,
            incoming.head(),
        )
    incoming = _apply_dependency_joins(incoming, metadata.dependencies, engine)
    logger.debug(
        "After dependency joins for %s: %d rows, columns=%s",
        metadata_name,
        len(incoming),
        list(incoming.columns),
    )
    execution_time = _capture_execution_time()
    execution_time_iso = execution_time.isoformat(timespec="milliseconds")
    archive_dest = _resolve_archive_destination(metadata, execution_time)
    archive_filename = archive_dest.name if archive_dest else Path(metadata.filepath).name
    metadata_batch_id = metadata.archive_batch_id or metadata.name
    logger.info(
        "Loaded dimension '%s' from %s (%d rows)",
        metadata.name,
        metadata.filepath,
        len(incoming),
    )
    logger.info("Execution timestamp: %s", execution_time_iso)
    available_columns = _get_target_columns(engine, metadata.target_table)
    safe_data_columns, missing = _handle_schema_drift(
        engine,
        metadata,
        available_columns,
        metadata.data_columns,
    )
    has_batch_id = DEFAULT_SYSTEM_COLUMNS["batch_id"] in available_columns
    has_archive_filename = DEFAULT_SYSTEM_COLUMNS["archive_filename"] in available_columns
    system_columns = DEFAULT_SYSTEM_COLUMNS
    if missing:
        logger.warning(
            "Schema drift for target %s: missing columns %s; using safe write set %s",
            metadata.target_table,
            missing,
            safe_data_columns,
        )

    rows_processed = len(incoming)
    logger.info(
        "Starting table %s: rows=%d, execution_time=%s",
        metadata.target_table,
        rows_processed,
        execution_time,
    )
    start_ts = datetime.now(timezone.utc)
    changes_applied = False
    try:
            changes_applied = apply_changes(
                engine=engine,
                target_table=metadata.target_table,
                incoming=incoming,
                natural_key_cols=list(metadata.natural_key_cols),
                data_cols=safe_data_columns,
                join_numeric_key_col=metadata.join_numeric_key,
                surrogate_key_col=metadata.surrogate_key,
                system_columns=system_columns,
                nullable_columns=[
                    name
                    for name, spec in metadata.data_columns.items()
                    if spec.nullable
                ],
                execution_time=execution_time_iso,
                batch_id=metadata_batch_id,
                archive_filename=archive_filename,
                has_batch_id=has_batch_id,
                has_archive_filename=has_archive_filename,
            )
    except Exception as exc:
        duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
        logger.error("SCD2 invocation failed for %s: %s", metadata.name, exc)
        logger.info("Run policy on table failure: %s", metadata.run_policy.on_table_failure)
        logger.info(
            "Table %s failed after %.3fs (rows=%d)",
            metadata.target_table,
            duration,
            rows_processed,
        )
        if metadata.run_policy.on_table_failure == "abort":
            raise
        logger.info("Continuing despite table failure per policy.")
        return incoming
    else:
        duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
        logger.info("Run policy on success: %s", metadata.run_policy.on_table_failure)
        logger.info(
            "Table %s completed in %.3fs (rows=%d)",
            metadata.target_table,
            duration,
            rows_processed,
        )
        if archive_dest:
            if {"batch_id", "archive_filename"} <= system_columns.keys():
                if changes_applied:
                    _archive_snapshot(incoming, metadata, archive_dest)
            else:
                missing_cols = {"batch_id", "archive_filename"} - set(system_columns.keys())
                logger.warning(
                    "Failed to archive snapshot to %s: missing system columns %s",
                    archive_dest,
                    missing_cols,
                )
    logger.info("SCD2 classification counts: not available")
    _post_scd2_validation(engine, metadata.target_table, metadata.natural_key_cols)
    return incoming


def _get_target_columns(engine: Engine, table_name: str) -> set[str]:
    inspector = inspect(engine)
    schema, table = _split_table_name(table_name)
    if not inspector.has_table(table, schema=schema):
        raise RuntimeError(f"Target table '{table_name}' does not exist.")
    return {col["name"] for col in inspector.get_columns(table, schema=schema)}


def _column_spec_clause(name: str, spec: ColumnSpec, engine: Engine) -> str:
    column_type = spec.type or _column_type_for_engine(engine)
    constraints = []
    if not spec.nullable:
        constraints.append("NOT NULL")
    if spec.unique:
        constraints.append("UNIQUE")
    if spec.default is not None:
        constraints.append(f"DEFAULT {spec.default}")
    constraint_sql = " ".join(constraints)
    return f"{name} {column_type} {constraint_sql}".strip()


def _ensure_target_table(engine: Engine, metadata: DimensionMetadata) -> None:
    inspector = inspect(engine)
    schema_name, table_name = _split_table_name(metadata.target_table)
    _ensure_schema_exists(engine, schema_name)
    if inspector.has_table(table_name, schema=schema_name):
        current_columns = {col["name"] for col in inspector.get_columns(table_name, schema=schema_name)}
        expected_columns = _expected_column_names(metadata)
        missing = expected_columns - current_columns
        if missing:
            logger.warning(
                "Existing table '%s' is missing metadata columns %s; schema handling=%s",
                metadata.target_table,
                missing,
                metadata.schema_handling.mode,
            )
            if metadata.schema_handling.mode == "evolve":
                _evolve_table_columns(engine, metadata, missing)
        return
    natural_specs = {
        **{col: spec for col, spec in metadata.natural_key_specs.items()},
        **{
            col: ColumnSpec(type=None, nullable=False)
            for col in metadata.natural_key_cols
            if col not in metadata.natural_key_specs
        },
    }
    column_defs = [
        _column_spec_clause(name, spec, engine)
        for name, spec in natural_specs.items()
    ]
    column_defs += [
        _column_spec_clause(name, spec, engine)
        for name, spec in metadata.data_columns.items()
    ]
    system_columns = [
        f"{DEFAULT_SYSTEM_COLUMNS['row_hash']} {_system_column_type(DEFAULT_SYSTEM_COLUMNS['row_hash'], engine)} NOT NULL",
        f"{DEFAULT_SYSTEM_COLUMNS['insert_date']} {_system_column_type(DEFAULT_SYSTEM_COLUMNS['insert_date'], engine)} NOT NULL",
        f"{DEFAULT_SYSTEM_COLUMNS['update_date']} {_system_column_type(DEFAULT_SYSTEM_COLUMNS['update_date'], engine)}",
        f"{DEFAULT_SYSTEM_COLUMNS['current_ind']} {_system_column_type(DEFAULT_SYSTEM_COLUMNS['current_ind'], engine)} NOT NULL",
        f"{DEFAULT_SYSTEM_COLUMNS['deleted_ind']} {_system_column_type(DEFAULT_SYSTEM_COLUMNS['deleted_ind'], engine)} NOT NULL",
        f"{DEFAULT_SYSTEM_COLUMNS['batch_id']} {_system_column_type(DEFAULT_SYSTEM_COLUMNS['batch_id'], engine)} NOT NULL",
        f"{DEFAULT_SYSTEM_COLUMNS['archive_filename']} {_system_column_type(DEFAULT_SYSTEM_COLUMNS['archive_filename'], engine)} NOT NULL",
    ]
    surrogate_clause = _surrogate_column_clause(engine, metadata.surrogate_key)
    join_numeric_clause = _join_numeric_clause(engine, metadata.join_numeric_key)
    create_sql = f"""
        CREATE TABLE {metadata.target_table} (
            {surrogate_clause},
            {join_numeric_clause},
            {', '.join(column_defs + system_columns)}
        )
        """
    logger.info("Creating target table %s with DDL:\n%s", metadata.target_table, create_sql)
    with engine.begin() as conn:
        conn.execute(text(create_sql))


def _split_table_name(name: str) -> tuple[str | None, str]:
    if "." in name:
        schema, table = name.split(".", 1)
    else:
        return None, name
    schema = schema.strip("[]\"")
    table = table.strip("[]\"")
    return schema, table


def _ensure_schema_exists(engine: Engine, schema: str | None) -> None:
    if not schema:
        return
    if engine.dialect.name != "mssql":
        return
    schema_safe = schema.replace("]", "]]")
    create_sql = text(
        f"""
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = :schema)
            EXEC('CREATE SCHEMA [{schema_safe}]')
        """
    )
    with engine.begin() as conn:
        conn.execute(create_sql, {"schema": schema})


def _expected_column_names(metadata: DimensionMetadata) -> set[str]:
    names = {metadata.surrogate_key, metadata.join_numeric_key}
    names.update(metadata.natural_key_cols)
    names.update(metadata.data_columns.keys())
    names.update(DEFAULT_SYSTEM_COLUMNS.values())
    return names


def _system_column_comments() -> dict[str, str]:
    return {
        DEFAULT_SYSTEM_COLUMNS["batch_id"]: "Batch identifier for this run",
        DEFAULT_SYSTEM_COLUMNS["archive_filename"]: "Archive file name for the snapshot",
        DEFAULT_SYSTEM_COLUMNS["row_hash"]: "Hash of all data columns",
        DEFAULT_SYSTEM_COLUMNS["current_ind"]: "Indicates whether row is current",
        DEFAULT_SYSTEM_COLUMNS["deleted_ind"]: "Indicates whether row is marked deleted",
        DEFAULT_SYSTEM_COLUMNS["insert_date"]: "Insert timestamp",
        DEFAULT_SYSTEM_COLUMNS["update_date"]: "Last update timestamp",
    }


def _apply_table_description(engine: Engine, metadata: DimensionMetadata) -> None:
    description = metadata.description
    if not description or engine.dialect.name != "mssql":
        return

    schema, table = _split_table_name(metadata.target_table)
    schema = schema or "dbo"
    params = {"description": description, "schema": schema, "table": table}
    update_sql = text(
        "EXEC sys.sp_updateextendedproperty @name=N'MS_Description', @value=:description, "
        "@level0type=N'SCHEMA', @level0name=:schema, "
        "@level1type=N'TABLE', @level1name=:table"
    )
    add_sql = text(
        "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=:description, "
        "@level0type=N'SCHEMA', @level0name=:schema, "
        "@level1type=N'TABLE', @level1name=:table"
    )
    column_comments = _system_column_comments()
    with engine.begin() as conn:
        try:
            conn.execute(update_sql, params)
        except Exception:
            conn.execute(add_sql, params)
        for column, comment in column_comments.items():
            column_params = {**params, "column": column, "comment": comment}
            col_update = text(
                "EXEC sys.sp_updateextendedproperty @name=N'MS_Description', @value=:comment, "
                "@level0type=N'SCHEMA', @level0name=:schema, "
                "@level1type=N'TABLE', @level1name=:table, "
                "@level2type=N'COLUMN', @level2name=:column"
            )
            col_add = text(
                "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=:comment, "
                "@level0type=N'SCHEMA', @level0name=:schema, "
                "@level1type=N'TABLE', @level1name=:table, "
                "@level2type=N'COLUMN', @level2name=:column"
            )
            try:
                conn.execute(col_update, column_params)
            except Exception:
                conn.execute(col_add, column_params)
# End patch


def _apply_table_description(engine: Engine, metadata: DimensionMetadata) -> None:
    description = metadata.description
    if not description:
        return

    if engine.dialect.name != "mssql":
        return

    schema, table = _split_table_name(metadata.target_table)
    schema = schema or "dbo"
    params = {"description": description, "schema": schema, "table": table}
    update_sql = text(
        "EXEC sys.sp_updateextendedproperty @name=N'MS_Description', @value=:description, "
        "@level0type=N'SCHEMA', @level0name=:schema, "
        "@level1type=N'TABLE', @level1name=:table"
    )
    add_sql = text(
        "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=:description, "
        "@level0type=N'SCHEMA', @level0name=:schema, "
        "@level1type=N'TABLE', @level1name=:table"
    )
    with engine.begin() as conn:
        try:
            conn.execute(update_sql, params)
        except Exception:
            conn.execute(add_sql, params)
        logger.info("Applied table description to %s", metadata.target_table)

def _column_spec_for_missing(name: str, metadata: DimensionMetadata) -> ColumnSpec | None:
    if name in metadata.natural_key_specs:
        return metadata.natural_key_specs[name]
    if name in metadata.natural_key_cols:
        return ColumnSpec(type=None, nullable=False)
    return metadata.data_columns.get(name)


def _evolve_table_columns(
    engine: Engine,
    metadata: DimensionMetadata,
    missing_columns: set[str],
) -> None:
    dialect = engine.dialect.name
    with engine.begin() as conn:
        for column in sorted(missing_columns):
            spec = _column_spec_for_missing(column, metadata)
            if spec is None:
                logger.warning("Cannot evolve column '%s': no spec defined", column)
                continue
            clause = _column_spec_clause(column, spec, engine)
            add_sql = text(f"ALTER TABLE {metadata.target_table} ADD {clause}")
            conn.execute(add_sql)
            logger.info("Evolved table %s by adding column %s for mode=%s", metadata.target_table, column, metadata.schema_handling.mode)

def _surrogate_column_clause(engine: Engine, name: str) -> str:
    dialect = engine.dialect.name
    if dialect == "mssql":
        return f"{name} BIGINT IDENTITY(1,1) PRIMARY KEY"
    return f"{name} INTEGER PRIMARY KEY AUTOINCREMENT"


def _join_numeric_clause(engine: Engine, name: str) -> str:
    dialect = engine.dialect.name
    if dialect == "mssql":
        return f"{name} BIGINT NOT NULL"
    return f"{name} INTEGER NOT NULL"


def _resolve_safe_data_columns(
    requested: Sequence[str], available: set[str]
) -> tuple[list[str], list[str]]:
    safe = [col for col in requested if col in available]
    missing = [col for col in requested if col not in available]
    if not safe and missing:
        raise RuntimeError("No safe data columns available to write.")
    return safe, missing


def _apply_dependency_joins(
    incoming: pd.DataFrame,
    dependencies: Sequence[DependencyJoin],
    engine: Engine,
) -> pd.DataFrame:
    for dep in dependencies:
        schema, table = dep.schema_name, dep.table
        table_ref = f"[{schema}].[{table}]" if schema else f"[{table}]"
        on_source = [relation["source"] for relation in dep.on]
        on_target = [relation["target"] for relation in dep.on]
        select_aliases = dict(dep.select)
        select_columns = on_target + list(select_aliases.keys())
        quoted_cols = ", ".join(f"[{col}]" for col in select_columns)
        where_clauses = []
        if dep.filter_target_current:
            where_clauses.append("[Current_Ind] = 1")
        for expressions in dep.where.values():
            where_clauses.extend(expressions)
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        query = text(f"SELECT {quoted_cols} FROM {table_ref} {where_sql}")
        dep_df = pd.read_sql_query(query, con=engine)

        dep_df = dep_df.rename(columns=dict(zip(on_target, on_source)))
        dep_df = dep_df.rename(columns=select_aliases)

        dep_map = {}
        for _, row in dep_df.iterrows():
            key = tuple(row[src] for src in on_source)
            dep_map[key] = {alias: row[alias] for alias in select_aliases.values()}

        key_series = incoming[on_source].apply(lambda row: tuple(row[src] for src in on_source), axis=1)
        for alias in select_aliases.values():
            incoming[alias] = key_series.map(lambda key: dep_map.get(key, {}).get(alias))

        if dep.on_missing == "error":
            missing_mask = incoming[list(select_aliases.values())].isna().any(axis=1)
            if missing_mask.any():
                raise RuntimeError(f"Dependency join {dep.table} produced missing values.")

    return incoming


def _cast_data_columns(incoming: pd.DataFrame, metadata: DimensionMetadata) -> pd.DataFrame:
    df = incoming.copy()
    for name, spec in metadata.data_columns.items():
        if spec.type and "DATETIME" in spec.type.upper():
            fmt = spec.parse_format or DEFAULT_DATETIME_INPUT_FORMAT
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message="Could not infer format, so each element will be parsed individually.*",
                )
                df[name] = pd.to_datetime(
                    df[name],
                    format=fmt,
                    errors="coerce",
                    dayfirst=spec.dayfirst,
                )
    return df


def _handle_schema_drift(
    engine: Engine,
    metadata: DimensionMetadata,
    available_columns: set[str],
    requested_columns: Mapping[str, ColumnSpec],
) -> tuple[list[str], list[str]]:
    requested_names = list(requested_columns.keys())
    safe, missing = _resolve_safe_data_columns(requested_names, available_columns)
    if missing:
        plan = f"Missing columns: {missing}"
        if metadata.schema_handling.mode == "evolve":
            missing_specs = {name: requested_columns[name] for name in missing}
            _evolve_schema(engine, metadata.target_table, missing_specs)
            safe = list(requested_columns)
            logger.info("Schema evolution applied for %s: added %s", metadata.target_table, missing)
        else:
            logger.warning(
                "Schema evolution plan for %s: %s",
                metadata.target_table,
                plan,
            )
    return safe, missing


def _column_type_for_engine(engine: Engine) -> str:
    dialect = engine.dialect.name
    if dialect == "mssql":
        return "NVARCHAR(4000)"
    return "TEXT"


def _system_column_type(column: str, engine: Engine) -> str:
    dialect = engine.dialect.name
    if column in {DEFAULT_SYSTEM_COLUMNS["current_ind"], DEFAULT_SYSTEM_COLUMNS["deleted_ind"]}:
        if dialect == "mssql":
            return "BIT"
        return "BOOLEAN"
    if column == DEFAULT_SYSTEM_COLUMNS["row_hash"]:
        if dialect == "mssql":
            return "NVARCHAR(4000)"
        return "TEXT"
    if column in {DEFAULT_SYSTEM_COLUMNS["batch_id"], DEFAULT_SYSTEM_COLUMNS["archive_filename"]}:
        if dialect == "mssql":
            return "NVARCHAR(4000)"
        return "TEXT"
    if dialect == "mssql":
        return "DATETIME2(3)"
    return "DATETIME"


def _evolve_schema(engine: Engine, table_name: str, missing: Mapping[str, ColumnSpec]) -> None:
    with engine.begin() as conn:
        for column, spec in missing.items():
            column_type = spec.type or _column_type_for_engine(engine)
            constraints = []
            if not spec.nullable:
                constraints.append("NOT NULL")
            if spec.unique:
                constraints.append("UNIQUE")
            if spec.default is not None:
                constraints.append(f"DEFAULT {spec.default}")
            constraint_sql = " ".join(constraints)
            definition = f"{column_type} {constraint_sql}".strip()
            conn.execute(
                text(f"ALTER TABLE {table_name} ADD COLUMN {column} {definition}")
            )


def _capture_execution_time() -> datetime:
    now = datetime.now(timezone.utc)
    milliseconds = (now.microsecond // 1000) * 1000
    return now.replace(microsecond=milliseconds)


def _post_scd2_validation(engine: Engine, table: str, natural_key_cols: Sequence[str]) -> None:
    key_expr = ", ".join(natural_key_cols)
    current_dups_sql = text(
        f"""
        SELECT {key_expr}, COUNT(*) AS cnt
        FROM {table}
        WHERE Current_Ind = 1
        GROUP BY {key_expr}
        HAVING COUNT(*) > 1
        """
    )

    dialect = engine.dialect.name
    top = "TOP 1 " if dialect == "mssql" else ""
    limit = "" if dialect == "mssql" else " LIMIT 1"
    validation_checks = [
        (
            text(
                f"SELECT {top}1 FROM {table} WHERE Current_Ind = 1 AND Deleted_Ind = 1{limit}"
            ),
            "row has Current_Ind=1 and Deleted_Ind=1",
        ),
        (
            text(
                f"SELECT {top}1 FROM {table} WHERE Current_Ind = 1 AND Update_Date IS NOT NULL{limit}"
            ),
            "current row has Update_Date not NULL",
        ),
        (
            text(
                f"SELECT {top}1 FROM {table} WHERE Current_Ind = 0 AND Update_Date IS NULL{limit}"
            ),
            "historical row missing Update_Date",
        ),
    ]

    with engine.connect() as conn:
        if conn.execute(current_dups_sql).first():
            raise RuntimeError("Multiple current rows found for a natural key.")
        for sql_stmt, message in validation_checks:
            if conn.execute(sql_stmt).first():
                raise RuntimeError(f"Post-SCD2 validation failed: {message}")


def _resolve_archive_destination(
    metadata: DimensionMetadata, execution_time: datetime
) -> Path | None:
    if not metadata.archive_path:
        return None

    base = Path(metadata.archive_path)
    if base.suffix:
        return base

    suffix = ".parquet"
    return base / f"{metadata.name}_{execution_time.strftime('%Y%m%d%H%M%S')}{suffix}"


def _archive_snapshot(
    incoming: pd.DataFrame,
    metadata: DimensionMetadata,
    archive_path: Path | None,
) -> None:
    if not archive_path:
        return

    archive_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        incoming.to_parquet(archive_path, index=False)
        logger.info("Archived snapshot to %s", archive_path)
    except Exception as exc:
        logger.warning("Failed to archive snapshot to %s: %s", archive_path, exc)
