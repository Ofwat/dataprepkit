"""Metadata-driven orchestrator for dataprepkit dimensions."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Mapping, Sequence

import logging
import pandas as pd
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import inspect
from sqlalchemy.engine import Engine

from dataprepkit.scd2 import apply_changes


logger = logging.getLogger(__name__)

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
    logger.info(
        "Loaded dimension '%s' from %s (%d rows)",
        metadata.name,
        metadata.filepath,
        len(incoming),
    )
    available_columns = _get_target_columns(engine, metadata.target_table)
    safe_data_columns, missing = _resolve_safe_data_columns(
        metadata.data_columns, available_columns
    )
    if missing:
        logger.warning(
            "Schema drift for target %s: missing columns %s; using safe write set %s",
            metadata.target_table,
            missing,
            safe_data_columns,
        )

    apply_changes(
        engine=engine,
        target_table=metadata.target_table,
        incoming=incoming,
        natural_key_cols=list(metadata.natural_key_cols),
        data_cols=safe_data_columns,
        join_numeric_key_col=metadata.join_numeric_key,
        surrogate_key_col=metadata.surrogate_key,
    )
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
