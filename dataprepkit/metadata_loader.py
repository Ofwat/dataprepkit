"""Metadata-driven orchestrator for dataprepkit dimensions."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Mapping, Sequence, Literal

from typing import Literal

import logging
import pandas as pd
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from dataprepkit.scd2 import apply_changes


logger = logging.getLogger(__name__)

class DependencyJoin(BaseModel):
    table: str
    how: Literal["left", "inner"] = "left"
    filter_target_current: bool = True
    on: Sequence[Mapping[str, str]]
    select: Mapping[str, str]
    on_missing: Literal["error", "null"] = "error"


class SchemaHandling(BaseModel):
    mode: Literal["suggest", "evolve"] = "suggest"


class RunPolicy(BaseModel):
    on_table_failure: Literal["continue", "abort"] = "abort"
    on_dependency_failure: Literal["skip_dependents", "abort"] = "skip_dependents"


class DimensionMetadata(BaseModel):
    """Defines the metadata required to load a single dimension table."""

    name: str
    target_table: str
    natural_key_cols: Sequence[str]
    data_columns: Sequence[str]
    surrogate_key: str
    join_numeric_key: str
    filepath: str
    column_renames: Mapping[str, str] = Field(default_factory=dict[str, str])
    description: str | None = None
    schema_handling: SchemaHandling = Field(default_factory=SchemaHandling)
    dependencies: Sequence[DependencyJoin] = Field(default_factory=list)
    run_policy: RunPolicy = Field(default_factory=RunPolicy)

    @field_validator("natural_key_cols", "data_columns")
    def must_define_columns(cls, value: Sequence[str]) -> Sequence[str]:
        if not value:
            raise ValueError("Must provide at least one column.")
        return value


ROOT = Path(__file__).resolve().parents[1]
METADATA_REGISTRY: Dict[str, DimensionMetadata] = {}


def register_metadata(name: str, metadata: Dict[str, object]) -> None:
    """Register metadata using a JSON-like dictionary for familiarity with old_code.py."""
    METADATA_REGISTRY[name] = DimensionMetadata(name=name, **metadata)


def _register_default_metadata() -> None:
    sample_path = ROOT / "examples" / "dummy_dimension.csv"
    register_metadata(
        "dummy_dimension",
        {
            "target_table": "dimension",
            "natural_key_cols": ["natural_key"],
            "data_columns": ["data_column"],
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": str(sample_path),
            "description": "Placeholder dimension used in demos.",
        },
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
    csv_reader: callable = pd.read_csv,
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
    incoming = _apply_dependency_joins(incoming, metadata.dependencies, engine)
    execution_time = _capture_execution_time()
    logger.info(
        "Loaded dimension '%s' from %s (%d rows)",
        metadata.name,
        metadata.filepath,
        len(incoming),
    )
    logger.info("Execution timestamp: %s", execution_time)
    available_columns = _get_target_columns(engine, metadata.target_table)
    safe_data_columns, missing = _handle_schema_drift(
        engine,
        metadata,
        available_columns,
        metadata.data_columns,
    )
    if missing:
        logger.warning(
            "Schema drift for target %s: missing columns %s; using safe write set %s",
            metadata.target_table,
            missing,
            safe_data_columns,
        )

    try:
        apply_changes(
            engine=engine,
            target_table=metadata.target_table,
            incoming=incoming,
            natural_key_cols=list(metadata.natural_key_cols),
            data_cols=safe_data_columns,
            join_numeric_key_col=metadata.join_numeric_key,
            surrogate_key_col=metadata.surrogate_key,
            execution_time=execution_time,
        )
    except Exception as exc:
        logger.error("SCD2 invocation failed for %s: %s", metadata.name, exc)
        logger.info("Run policy on table failure: %s", metadata.run_policy.on_table_failure)
        if metadata.run_policy.on_table_failure == "abort":
            raise
        logger.info("Continuing despite table failure per policy.")
        return incoming
    else:
        logger.info("Run policy on success: %s", metadata.run_policy.on_table_failure)
    logger.info("SCD2 classification counts: not available")
    _post_scd2_validation(engine, metadata.target_table, metadata.natural_key_cols)
    return incoming


def _get_target_columns(engine: Engine, table_name: str) -> set[str]:
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        raise RuntimeError(f"Target table '{table_name}' does not exist.")
    return {col["name"] for col in inspector.get_columns(table_name)}


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
        dep_df = pd.read_sql_table(dep.table, con=engine)
        if dep.filter_target_current and "Current_Ind" in dep_df.columns:
            dep_df = dep_df[dep_df["Current_Ind"] == 1]

        on_source = [relation["source"] for relation in dep.on]
        on_target = [relation["target"] for relation in dep.on]
        rename_map = dict(zip(on_target, on_source))
        dep_df = dep_df.rename(columns=rename_map)

        select_aliases = {}
        for target_col, alias in dep.select.items():
            dep_df = dep_df.rename(columns={target_col: alias})
            select_aliases[alias] = target_col

        columns_to_keep = on_source + list(select_aliases.keys())
        dep_df = dep_df[columns_to_keep]

        incoming = incoming.merge(
            dep_df.drop_duplicates(subset=on_source),
            on=on_source,
            how=dep.how,
        )

        if dep.on_missing == "error":
            missing_mask = incoming[select_aliases.keys()].isna().any(axis=1)
            if missing_mask.any():
                raise RuntimeError(f"Dependency join {dep.table} produced missing values.")

    return incoming


def _handle_schema_drift(
    engine: Engine,
    metadata: DimensionMetadata,
    available_columns: set[str],
    requested_columns: Sequence[str],
) -> tuple[list[str], list[str]]:
    safe, missing = _resolve_safe_data_columns(requested_columns, available_columns)
    if missing:
        plan = f"Missing columns: {missing}"
        if metadata.schema_handling.mode == "evolve":
            _evolve_schema(engine, metadata.target_table, missing)
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


def _evolve_schema(engine: Engine, table_name: str, missing: Sequence[str]) -> None:
    column_type = _column_type_for_engine(engine)
    with engine.begin() as conn:
        for column in missing:
            conn.execute(
                text(f"ALTER TABLE {table_name} ADD COLUMN {column} {column_type}")
            )


def _capture_execution_time() -> str:
    now = datetime.now(timezone.utc)
    milliseconds = (now.microsecond // 1000) * 1000
    truncated = now.replace(microsecond=milliseconds)
    return truncated.isoformat(timespec="milliseconds")


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

    validation_checks = [
        (
            text(f"SELECT 1 FROM {table} WHERE Current_Ind = 1 AND Deleted_Ind = 1 LIMIT 1"),
            "row has Current_Ind=1 and Deleted_Ind=1",
        ),
        (
            text(f"SELECT 1 FROM {table} WHERE Current_Ind = 1 AND Update_Date IS NOT NULL LIMIT 1"),
            "current row has Update_Date not NULL",
        ),
        (
            text(f"SELECT 1 FROM {table} WHERE Current_Ind = 0 AND Update_Date IS NULL LIMIT 1"),
            "historical row missing Update_Date",
        ),
    ]

    with engine.connect() as conn:
        if conn.execute(current_dups_sql).first():
            raise RuntimeError("Multiple current rows found for a natural key.")
        for sql_stmt, message in validation_checks:
            if conn.execute(sql_stmt).first():
                raise RuntimeError(f"Post-SCD2 validation failed: {message}")
