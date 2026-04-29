"""
SCD2 utilities for deterministic dimension management.

Provides an atomic `apply_changes` helper that ingests pandas data and
updates a target table with system columns (surrogate keys, hashes, flags).

The logic assumes the target table already exists and exposes the system
columns indicated via the configuration mapping.

TODO(prod-hardening):
- Capture and persist cast rejects when raw->typed staging `TRY_CAST` yields NULL.
- Add run-level metrics/logging for raw rows, casted rows, rejected rows, and applied SCD2 changes.
- Add retention/cleanup policy for transient raw/typed staging tables and staged parquet files.
- Add integration coverage on real Fabric SQL DB for representative schemas and larger volumes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import re
from typing import Mapping, Sequence

import pandas as pd
import math
from sqlalchemy.engine import Engine
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError
import uuid
from dataprepkit.helpers.staging import stage_dataframe


DEFAULT_SYSTEM_COLUMNS = {
    "surrogate_key": "surrogate_key",
    "join_numeric_key": "join_numeric_key",
    "row_hash": "row_hash",
    "insert_date": "Insert_Date",
    "update_date": "Update_Date",
    "effective_date_start": "Effective_Date_Start",
    "effective_date_end": "Effective_Date_End",
    "current_ind": "Current_Ind",
    "deleted_ind": "Deleted_Ind",
    "batch_id": "Batch_Id",
    "archive_filename": "Archive_Filename",
}

EFFECTIVE_DATE_MIN = "1900-01-01T00:00:00.000"
EFFECTIVE_DATE_MAX = "9999-12-31T23:59:59.999"


class SCD2ValidationError(ValueError):
    """Raised when the provided data does not satisfy the required schema."""


@dataclass
class SCD2ChangeSummary:
    incoming_rows: int = 0
    target_rows_before: int = 0
    target_rows_after: int = 0
    inserted_rows: int = 0
    new_rows: int = 0
    edited_rows: int = 0
    edited_rows_detail: list[dict[str, object]] | None = None
    edited_natural_keys: list[dict[str, object]] | None = None
    soft_deleted_rows: int = 0
    soft_deleted_natural_keys: list[dict[str, object]] | None = None
    reactivated_rows: int = 0
    reactivated_natural_keys: list[dict[str, object]] | None = None
    new_natural_keys: list[dict[str, object]] | None = None
    unchanged_rows: int = 0
    changes_applied_override: bool | None = None

    @property
    def changes_applied(self) -> bool:
        if self.changes_applied_override is not None:
            return self.changes_applied_override
        return any(
            (
                self.inserted_rows,
                self.edited_rows,
                self.soft_deleted_rows,
                self.reactivated_rows,
            )
        )

    def __bool__(self) -> bool:
        return self.changes_applied


def _normalize_bracket_identifier(name: str | None) -> str | None:
    if name is None:
        return None
    stripped = name.strip()
    if stripped.startswith("[") and stripped.endswith("]"):
        return stripped[1:-1].replace("]]", "]")
    return stripped


def apply_changes(
    engine: Engine,
    target_table: str,
    incoming: pd.DataFrame,
    natural_key_cols: Sequence[str],
    data_cols: Sequence[str],
    join_numeric_key_col: str,
    surrogate_key_col: str,
    system_columns: Mapping[str, str] | None = None,
    nullable_columns: Sequence[str] | None = None,
    execution_time: str | None = None,
    batch_id: str | None = None,
    archive_filename: str | None = None,
    has_batch_id: bool = False,
    has_archive_filename: bool = False,
    staging_use_openrowset_parquet: bool = False,
    staging_parquet_base_dir: str | None = None,
    staging_copy_source_base_url: str | None = None,
    staging_copy_into_options: str = "",
    return_summary: bool = False,
) -> bool | SCD2ChangeSummary:
    """
    Apply SCD2 semantics to the target table using the incoming DataFrame.

    The function computes row hashes, detects inserts/deletes/updates, expires
    old rows, and inserts new rows while respecting the configured system
    columns. All writes happen within a single transaction for atomicity.
    """
    cols = system_columns or DEFAULT_SYSTEM_COLUMNS
    required_keys = {
        "row_hash",
        "insert_date",
        "update_date",
        "effective_date_start",
        "effective_date_end",
        "current_ind",
        "deleted_ind",
    }
    missing = required_keys - cols.keys()
    if missing:
        raise SCD2ValidationError(f"Missing system column configuration: {missing}")

    incoming_df = pd.DataFrame(incoming).copy()
    if not set(natural_key_cols).issubset(incoming_df.columns):
        raise SCD2ValidationError("Incoming data must include all natural key columns.")
    if not set(data_cols).issubset(incoming_df.columns):
        raise SCD2ValidationError("Incoming data must include all declared data columns.")
    _raise_if_duplicate_natural_keys(incoming_df, natural_key_cols)

    hash_col = cols["row_hash"]
    incoming_df[hash_col] = incoming_df.apply(lambda row: _compute_row_hash(row, data_cols), axis=1)

    base_name = f"temp_snapshot_{uuid.uuid4().hex}"
    use_openrowset_staging = staging_use_openrowset_parquet and engine.dialect.name == "mssql"
    staging_table = _resolve_staging_table_name_for_mode(
        engine, base_name, use_temp_table=not use_openrowset_staging
    )
    execution_time = execution_time or _execution_timestamp()

    with engine.begin() as conn:
        pre_total = _count_rows(conn, target_table)
        column_types = _get_column_types(conn, target_table)
        extra_columns = ["existing_join_numeric"]
        existing_join_map = {}
        if natural_key_cols:
            select_cols = ", ".join(
                _quote_identifier(conn.engine, col)
                for col in [*natural_key_cols, join_numeric_key_col]
            )
            rows = conn.execute(
                text(
                    f"SELECT {select_cols} FROM {target_table} "
                    f"WHERE {_quote_identifier(conn.engine, cols['current_ind'])} = 1"
                )
            ).fetchall()
            for row in rows:
                key = tuple(_normalize_natural_key_value(value) for value in row[:-1])
                existing_join_map[key] = row[-1]
        incoming_df = incoming_df.copy()
        if natural_key_cols:
            incoming_df["existing_join_numeric"] = incoming_df.apply(
                lambda row: existing_join_map.get(
                    tuple(
                        _normalize_natural_key_value(row[col])
                        for col in natural_key_cols
                    )
                ),
                axis=1,
            )
        else:
            incoming_df["existing_join_numeric"] = None
        if not use_openrowset_staging:
            _create_staging_table(
                conn,
                staging_table,
                natural_key_cols,
                data_cols,
                hash_col,
                column_types,
                extra_columns=extra_columns,
                nullable_data_cols=nullable_columns,
                extra_column_type_overrides={
                    "existing_join_numeric": _column_type_for_column(
                        join_numeric_key_col,
                        column_types,
                        conn.engine,
                        preserve_mssql_types=True,
                    )
                },
            )
        change_summary = SCD2ChangeSummary(
            incoming_rows=len(incoming_df),
            target_rows_before=pre_total,
            target_rows_after=pre_total,
        )
        raw_staging_table: str | None = None
        try:
            if use_openrowset_staging:
                if not staging_parquet_base_dir:
                    raise SCD2ValidationError(
                        "staging_parquet_base_dir is required when staging_use_openrowset_parquet=True."
                    )
                raw_staging_table = f"{staging_table}__raw"
                all_snapshot_columns = list(natural_key_cols) + list(data_cols) + [hash_col]
                if extra_columns:
                    all_snapshot_columns.extend(extra_columns)
                staging_schema, staging_table_name = _split_table_name(staging_table)
                raw_schema, raw_table_name = _split_table_name(raw_staging_table)
                snapshot_df = incoming_df[all_snapshot_columns].copy()
                raw_snapshot_df = snapshot_df.astype("string")
                raw_type_overrides = {
                    "existing_join_numeric": _column_type_for_column(
                        join_numeric_key_col,
                        column_types,
                        conn.engine,
                        preserve_mssql_types=True,
                    )
                }
                for column in all_snapshot_columns:
                    target_type = raw_type_overrides.get(column) or _column_type_for_column(
                        column,
                        column_types,
                        conn.engine,
                        preserve_mssql_types=True,
                    )
                    if _is_integer_like_type(target_type):
                        raw_snapshot_df[column] = snapshot_df[column].map(
                            _normalize_integer_like_value_for_raw
                        )
                if "existing_join_numeric" in raw_snapshot_df.columns:
                    raw_snapshot_df["existing_join_numeric"] = snapshot_df[
                        "existing_join_numeric"
                    ].map(_normalize_existing_join_numeric_for_raw)
                stage_dataframe(
                    engine,
                    raw_table_name,
                    raw_snapshot_df,
                    if_exists="replace",
                    index=False,
                    schema=raw_schema,
                    use_copy_into_parquet=True,
                    parquet_base_dir=staging_parquet_base_dir,
                    copy_source_base_url=staging_copy_source_base_url,
                    copy_into_options=staging_copy_into_options,
                )
                _create_staging_table(
                    conn,
                    staging_table,
                    natural_key_cols,
                    data_cols,
                    hash_col,
                    column_types,
                    extra_columns=extra_columns,
                    nullable_data_cols=nullable_columns,
                    preserve_mssql_types=True,
                    extra_column_type_overrides={
                        "existing_join_numeric": _column_type_for_column(
                            join_numeric_key_col,
                            column_types,
                            conn.engine,
                            preserve_mssql_types=True,
                        )
                    },
                )
                _insert_snapshot_rows_from_raw(
                    conn,
                    raw_table=raw_staging_table,
                    target_table=staging_table,
                    columns=all_snapshot_columns,
                    column_types=column_types,
                    column_type_overrides={
                        "existing_join_numeric": _column_type_for_column(
                            join_numeric_key_col,
                            column_types,
                            conn.engine,
                            preserve_mssql_types=True,
                        )
                    },
                    source_df=snapshot_df,
                    natural_key_cols=natural_key_cols,
                )
            else:
                _insert_snapshot_rows(
                    conn,
                    staging_table,
                    incoming_df,
                    natural_key_cols,
                    data_cols,
                    hash_col,
                    extra_columns=extra_columns,
                )
            change_summary = _coerce_change_summary(
                _apply_snapshot_to_target(
                    conn,
                    staging_table,
                    target_table,
                    natural_key_cols,
                    data_cols,
                    join_numeric_key_col,
                    cols,
                    column_types,
                    hash_col,
                    execution_time,
                    batch_id=batch_id,
                    archive_filename=archive_filename,
                    has_batch_id=has_batch_id,
                    has_archive_filename=has_archive_filename,
                    incoming_rows=len(incoming_df),
                    target_rows_before=pre_total,
                ),
                incoming_rows=len(incoming_df),
                target_rows_before=pre_total,
            )
        finally:
            if raw_staging_table:
                conn.execute(text(f"DROP TABLE IF EXISTS {raw_staging_table}"))
            conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))
        _validate_current_join_numeric_unique(
            conn,
            target_table,
            join_numeric_key_col,
            cols["current_ind"],
        )
        post_total = _count_rows(conn, target_table)
        _validate_row_growth(conn, target_table, pre_total, post_total)
        change_summary.target_rows_after = post_total
        if return_summary:
            return change_summary
        return change_summary.changes_applied


def _execution_timestamp() -> str:
    now = datetime.now(timezone.utc)
    milliseconds = (now.microsecond // 1000) * 1000
    truncated = now.replace(microsecond=milliseconds)
    return truncated.isoformat(timespec="milliseconds")


def _coerce_change_summary(
    result: object,
    *,
    incoming_rows: int,
    target_rows_before: int,
) -> SCD2ChangeSummary:
    if isinstance(result, SCD2ChangeSummary):
        return result
    return SCD2ChangeSummary(
        incoming_rows=incoming_rows,
        target_rows_before=target_rows_before,
        target_rows_after=target_rows_before,
        changes_applied_override=bool(result),
    )


def _resolve_staging_table_name(engine: Engine, base_name: str) -> str:
    return _resolve_staging_table_name_for_mode(engine, base_name, use_temp_table=True)


def _resolve_staging_table_name_for_mode(
    engine: Engine, base_name: str, *, use_temp_table: bool
) -> str:
    dialect = engine.dialect.name
    if dialect == "mssql":
        return f"#{base_name}" if use_temp_table else base_name
    if dialect == "sqlite":
        return f"temp_{base_name}"
    return base_name


def _compute_row_hash(row: pd.Series, data_columns: Sequence[str]) -> str:
    tokens = []
    for column in sorted(data_columns):
        value = row.get(column)
        if pd.isna(value):
            value = ""
        elif isinstance(value, float) and value.is_integer():
            value = int(value)
        tokens.append(f"{column}={value}")
    return hashlib.sha256("|".join(tokens).encode("utf-8")).hexdigest()


def synchronize_current_row_hashes(
    engine: Engine,
    target_table: str,
    data_cols: Sequence[str],
    surrogate_key_col: str,
    system_columns: Mapping[str, str] | None = None,
) -> int:
    cols = system_columns or DEFAULT_SYSTEM_COLUMNS
    hash_col = cols["row_hash"]
    current_col = cols["current_ind"]
    select_columns = [surrogate_key_col, *data_cols, hash_col]

    with engine.begin() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT {', '.join(select_columns)}
                FROM {target_table}
                WHERE {current_col} = 1
                """
            )
        ).fetchall()
        mismatches = []
        for row in rows:
            row_data = dict(zip(select_columns, row))
            expected_hash = _compute_row_hash(pd.Series(row_data), data_cols)
            if row_data[hash_col] != expected_hash:
                mismatches.append(
                    {
                        "surrogate_key": row_data[surrogate_key_col],
                        "row_hash": expected_hash,
                    }
                )
        if not mismatches:
            return 0
        conn.execute(
            text(
                f"""
                UPDATE {target_table}
                SET {hash_col} = :row_hash
                WHERE {surrogate_key_col} = :surrogate_key
                """
            ),
            mismatches,
        )
        return len(mismatches)


def _create_staging_table(
    conn,
    table_name,
    natural_key_cols,
    data_cols,
    hash_col,
    column_types: Mapping[str, str],
    extra_columns: Sequence[str] | None = None,
    nullable_data_cols: Sequence[str] | None = None,
    preserve_mssql_types: bool = False,
    extra_column_type_overrides: Mapping[str, str] | None = None,
):
    dialect = conn.engine.dialect.name
    quote = lambda value: _quote_identifier(conn.engine, value)
    column_defs = []
    nullable_set = set(nullable_data_cols or [])
    for col in natural_key_cols:
        column_defs.append(
            f"{quote(col)} {_column_type_for_column(col, column_types, conn.engine, preserve_mssql_types=preserve_mssql_types)} NOT NULL"
        )
    for col in data_cols:
        null_clause = (
            ""
            if col in nullable_set
            else " NOT NULL"
        )
        column_defs.append(
            f"{quote(col)} {_column_type_for_column(col, column_types, conn.engine, preserve_mssql_types=preserve_mssql_types)}{null_clause}"
        )
    column_defs.append(
        f"{quote(hash_col)} {_column_type_for_column(hash_col, column_types, conn.engine, preserve_mssql_types=preserve_mssql_types)} NOT NULL"
    )
    for extra in extra_columns or []:
        extra_type = (
            extra_column_type_overrides.get(extra)
            if extra_column_type_overrides
            else None
        )
        column_defs.append(
            f"{quote(extra)} {extra_type or _column_type_for_column(extra, column_types, conn.engine, preserve_mssql_types=preserve_mssql_types)}"
        )
    unique_clause = (
        f", UNIQUE({', '.join(quote(col) for col in natural_key_cols)})"
        if natural_key_cols
        else ""
    )
    create_sql = text(
        f"""
        CREATE {'TABLE' if dialect != 'sqlite' else 'TEMP TABLE'} {table_name} (
            {', '.join(column_defs)}
            {unique_clause}
        )
        """
    )
    conn.execute(create_sql)


def _insert_snapshot_rows(
    conn,
    table_name,
    incoming_df,
    natural_key_cols,
    data_cols,
    hash_col,
    extra_columns: Sequence[str] | None = None,
):
    columns = list(natural_key_cols) + list(data_cols) + [hash_col]
    if extra_columns:
        columns.extend(extra_columns)
    engine = getattr(conn, "engine", None)
    rendered_columns = ", ".join(
        _quote_identifier(engine, col) if engine is not None else col
        for col in columns
    )
    param_names = (
        {col: f"value_{index}" for index, col in enumerate(columns)}
        if engine is not None
        else {col: col for col in columns}
    )
    insert_sql = text(
        f"""
        INSERT INTO {table_name} ({rendered_columns})
        VALUES ({', '.join(':' + param_names[col] for col in columns)})
        """
    )
    def _sanitize(value):
        if pd.isna(value):
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        if isinstance(value, float) and value.is_integer():
            return int(value)
        return value

    records = []
    for _, row in incoming_df[columns].iterrows():
        record = {}
        for col in columns:
            record[param_names[col]] = _sanitize(row[col])
        records.append(record)
    try:
        if records:
            conn.execute(insert_sql, records)
    except IntegrityError as exc:
        raise SCD2ValidationError(
            _duplicate_natural_keys_error_message(
                incoming_df,
                natural_key_cols,
                database_detected=True,
            )
        ) from exc


def _insert_snapshot_rows_from_raw(
    conn,
    *,
    raw_table: str,
    target_table: str,
    columns: Sequence[str],
    column_types: Mapping[str, str],
    column_type_overrides: Mapping[str, str] | None = None,
    source_df: pd.DataFrame | None = None,
    natural_key_cols: Sequence[str] | None = None,
) -> None:
    rendered_columns = ", ".join(_quote_identifier(conn.engine, col) for col in columns)
    select_parts = []
    for col in columns:
        target_type = (
            column_type_overrides.get(col)
            if column_type_overrides
            else None
        ) or _column_type_for_column(
            col, column_types, conn.engine, preserve_mssql_types=True
        )
        quoted = _quote_identifier(conn.engine, col)
        if conn.engine.dialect.name == "mssql":
            select_parts.append(f"TRY_CAST(NULLIF(src.{quoted}, '') AS {target_type})")
        else:
            select_parts.append(f"src.{quoted}")
    select_sql = ", ".join(select_parts)
    insert_sql = text(
        f"""
        INSERT INTO {target_table} ({rendered_columns})
        SELECT {select_sql}
        FROM {raw_table} src
        """
    )
    try:
        conn.execute(insert_sql)
    except IntegrityError as exc:
        raise SCD2ValidationError(
            _duplicate_natural_keys_error_message(
                source_df,
                natural_key_cols or [],
                database_detected=True,
            )
        ) from exc


def _normalize_existing_join_numeric_for_raw(value) -> str | None:
    return _normalize_integer_like_value_for_raw(value)


def _normalize_natural_key_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if value.is_integer():
            return str(int(value))
    if isinstance(value, int):
        return str(value)
    return str(value)


def _normalize_integer_like_value_for_raw(value) -> str | None:
    if pd.isna(value):
        return None
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if value.is_integer():
            return str(int(value))
    return str(value)


def _raise_if_duplicate_natural_keys(
    incoming_df: pd.DataFrame,
    natural_key_cols: Sequence[str],
    *,
    sample_limit: int = 10,
) -> None:
    examples = _find_duplicate_natural_keys(
        incoming_df,
        natural_key_cols,
        sample_limit=sample_limit,
    )
    if not examples:
        return
    raise SCD2ValidationError(
        _duplicate_natural_keys_error_message(
            incoming_df,
            natural_key_cols,
            examples=examples,
        )
    )


def _duplicate_natural_keys_error_message(
    incoming_df: pd.DataFrame | None,
    natural_key_cols: Sequence[str],
    *,
    database_detected: bool = False,
    examples: list[dict[str, object]] | None = None,
) -> str:
    base = "Incoming data contains duplicate natural keys."
    if not natural_key_cols:
        return base
    examples = examples or _find_duplicate_natural_keys(
        incoming_df,
        natural_key_cols,
        trim_strings=database_detected,
        casefold_strings=database_detected,
    )
    detail = (
        " after staging normalization/collation"
        if database_detected
        else ""
    )
    if examples:
        return (
            f"Incoming data contains duplicate natural keys{detail}. "
            f"Natural key columns: {list(natural_key_cols)}. "
            f"Example duplicate keys: {examples}"
        )
    if database_detected:
        return (
            f"Incoming data contains duplicate natural keys{detail}. "
            f"Natural key columns: {list(natural_key_cols)}. "
            "Unable to isolate duplicate keys from the in-memory snapshot."
        )
    return (
        f"{base} Natural key columns: {list(natural_key_cols)}."
    )


def _find_duplicate_natural_keys(
    incoming_df: pd.DataFrame | None,
    natural_key_cols: Sequence[str],
    *,
    sample_limit: int = 10,
    trim_strings: bool = False,
    casefold_strings: bool = False,
) -> list[dict[str, object]]:
    if incoming_df is None or not natural_key_cols or incoming_df.empty:
        return []
    if not natural_key_cols or incoming_df.empty:
        return []
    key_frame = incoming_df.loc[:, list(natural_key_cols)].copy()
    normalized = key_frame.copy()
    for column in natural_key_cols:
        normalized[column] = normalized[column].map(
            lambda value: _normalize_duplicate_key_value(
                value,
                trim_strings=trim_strings,
                casefold_strings=casefold_strings,
            )
        )
    duplicate_counts = (
        normalized.groupby(list(natural_key_cols), dropna=False)
        .size()
        .reset_index(name="count")
    )
    duplicates = duplicate_counts.loc[duplicate_counts["count"] > 1]
    if duplicates.empty:
        return []
    examples = []
    for _, row in duplicates.head(sample_limit).iterrows():
        example = {col: row[col] for col in natural_key_cols}
        example["count"] = int(row["count"])
        examples.append(example)
    return examples


def _normalize_duplicate_key_value(
    value,
    *,
    trim_strings: bool = False,
    casefold_strings: bool = False,
):
    normalized = _normalize_natural_key_value(value)
    if isinstance(normalized, str):
        if trim_strings:
            normalized = normalized.strip()
        if casefold_strings:
            normalized = normalized.casefold()
    return normalized


def _is_integer_like_type(type_name: str) -> bool:
    normalized = type_name.strip().upper()
    return normalized.startswith(("BIGINT", "INT", "INTEGER", "SMALLINT", "TINYINT"))


def _quote_identifier(engine: Engine, identifier: str) -> str:
    normalized = _normalize_bracket_identifier(identifier) or ""
    if engine.dialect.name == "mssql":
        return f"[{normalized.replace(']', ']]')}]"
    return f'"{normalized.replace(chr(34), chr(34) * 2)}"'


def _build_join_condition(left_alias: str, right_alias: str, columns: Sequence[str]) -> str:
    return " AND ".join(
        f"{left_alias}.{_quote_identifier_engine(col)} = {right_alias}.{_quote_identifier_engine(col)}"
        for col in columns
    )


def _quote_identifier_engine(identifier: str) -> str:
    normalized = _normalize_bracket_identifier(identifier) or ""
    return f'"{normalized.replace(chr(34), chr(34) * 2)}"'


def _is_text_like_type(type_name: str | None) -> bool:
    if not type_name:
        return False
    normalized = type_name.strip().upper()
    return any(token in normalized for token in ("CHAR", "TEXT", "CLOB"))


def _case_sensitive_match_expression(
    engine: Engine,
    alias: str,
    column: str,
    column_types: Mapping[str, str],
) -> str:
    quoted = f"{alias}.{_quote_identifier(engine, column)}"
    if not _is_text_like_type(column_types.get(column.lower())):
        return quoted
    if engine.dialect.name == "sqlite":
        return f"{quoted} COLLATE BINARY"
    if engine.dialect.name == "mssql":
        return f"{quoted} COLLATE Latin1_General_100_BIN2"
    return quoted


def _build_natural_key_match_condition(
    engine: Engine,
    left_alias: str,
    right_alias: str,
    columns: Sequence[str],
    column_types: Mapping[str, str],
) -> str:
    conditions = []
    for col in columns:
        left_expr = _case_sensitive_match_expression(
            engine,
            left_alias,
            col,
            column_types,
        )
        right_expr = _case_sensitive_match_expression(
            engine,
            right_alias,
            col,
            column_types,
        )
        condition = f"{left_expr} = {right_expr}"
        if engine.dialect.name == "mssql" and _is_text_like_type(
            column_types.get(col.lower())
        ):
            quoted = _quote_identifier(engine, col)
            condition = (
                f"({condition} AND "
                f"DATALENGTH({left_alias}.{quoted}) = "
                f"DATALENGTH({right_alias}.{quoted}))"
            )
        conditions.append(condition)
    return " AND ".join(conditions)


def _apply_snapshot_to_target(
    conn,
    staging_table: str,
    target_table: str,
    natural_key_cols: Sequence[str],
    data_cols: Sequence[str],
    join_numeric_key_col: str,
    columns: Mapping[str, str],
    column_types: Mapping[str, str],
    hash_col: str,
    execution_time: str,
    batch_id: str | None = None,
    archive_filename: str | None = None,
    has_batch_id: bool = False,
    has_archive_filename: bool = False,
    incoming_rows: int = 0,
    target_rows_before: int = 0,
) -> SCD2ChangeSummary:
    quote = lambda value: _quote_identifier(conn.engine, value)
    join_condition = _build_natural_key_match_condition(
        conn.engine,
        target_table,
        "s",
        natural_key_cols,
        column_types,
    )
    target_to_staging_condition = _build_natural_key_match_condition(
        conn.engine,
        "t",
        "s",
        natural_key_cols,
        column_types,
    )
    deleted_ind_column = quote(columns["deleted_ind"])
    update_date_column = quote(columns["update_date"])
    effective_end_column = quote(columns["effective_date_end"])
    current_ind_column = quote(columns["current_ind"])
    hash_column = quote(hash_col)
    join_numeric_column = quote(join_numeric_key_col)
    summary = _count_snapshot_changes(
        conn,
        staging_table=staging_table,
        target_table=target_table,
        natural_key_cols=natural_key_cols,
        data_cols=data_cols,
        match_condition=target_to_staging_condition,
        hash_column=hash_column,
        current_ind_column=current_ind_column,
        deleted_ind_column=deleted_ind_column,
        incoming_rows=incoming_rows,
        target_rows_before=target_rows_before,
    )

    delete_sql = text(
        f"""
        UPDATE {target_table}
        SET {deleted_ind_column} = 1,
            {update_date_column} = :execution_time,
            {effective_end_column} = :execution_time
        WHERE {current_ind_column} = 1
          AND {deleted_ind_column} = 0
          AND NOT EXISTS (
            SELECT 1 FROM {staging_table} s
            WHERE {join_condition}
          )
        """
    )
    delete_result = conn.execute(delete_sql, {"execution_time": execution_time})

    update_changed_sql = text(
        f"""
        UPDATE {target_table}
        SET {current_ind_column} = 0,
            {update_date_column} = :execution_time,
            {effective_end_column} = :execution_time
        WHERE {current_ind_column} = 1
          AND {deleted_ind_column} = 0
          AND EXISTS (
            SELECT 1 FROM {staging_table} s
            WHERE {join_condition}
              AND s.{hash_column} != {target_table}.{hash_column}
          )
        """
    )
    update_changed_result = conn.execute(
        update_changed_sql,
        {"execution_time": execution_time},
    )

    update_reactivated_sql = text(
        f"""
        UPDATE {target_table}
        SET {current_ind_column} = 0,
            {update_date_column} = :execution_time,
            {effective_end_column} = :execution_time
        WHERE {current_ind_column} = 1
          AND {deleted_ind_column} = 1
          AND EXISTS (
            SELECT 1 FROM {staging_table} s
            WHERE {join_condition}
          )
        """
    )
    update_reactivated_result = conn.execute(
        update_reactivated_sql,
        {"execution_time": execution_time},
    )

    max_join_sql = text(f"SELECT COALESCE(MAX({join_numeric_column}), 0) FROM {target_table}")
    max_join_numeric = conn.execute(max_join_sql).scalar() or 0

    order_by = ", ".join(f"s.{quote(col)}" for col in natural_key_cols) or "1"

    insert_columns = list(natural_key_cols) + list(data_cols) + [join_numeric_key_col]
    if has_batch_id:
        insert_columns.append(columns["batch_id"])
    if has_archive_filename:
        insert_columns.append(columns["archive_filename"])
    insert_columns.extend(
        [
            hash_col,
            columns["insert_date"],
            columns["update_date"],
            columns["effective_date_start"],
            columns["effective_date_end"],
            columns["current_ind"],
            columns["deleted_ind"],
        ]
    )

    current_join_condition = (
        f"{_build_natural_key_match_condition(conn.engine, 't', 's', natural_key_cols, column_types)} "
        f"AND t.{current_ind_column} = 1"
        if natural_key_cols
        else f"t.{current_ind_column} = 1"
    )

    select_parts = [
        *[f"s.{quote(col)}" for col in natural_key_cols],
        *[f"s.{quote(col)}" for col in data_cols],
        "COALESCE(s.existing_join_numeric, :join_numeric_base + rn)",
    ]
    if has_batch_id:
        select_parts.append(":batch_id")
    if has_archive_filename:
        select_parts.append(":archive_filename")
    select_parts.extend(
        [
            f"s.{hash_column}",
            ":execution_time",
            "NULL",
            "CASE WHEN s.existing_join_numeric IS NULL THEN :effective_date_min ELSE :execution_time END",
            ":effective_date_max",
            "1",
            "0",
        ]
    )

    insert_sql = text(
        f"""
        WITH candidates AS (
            SELECT
                s.*,
                ROW_NUMBER() OVER (ORDER BY {order_by}) AS rn
            FROM {staging_table} s
            LEFT JOIN {target_table} t ON {current_join_condition}
            WHERE t.{current_ind_column} IS NULL
               OR s.{hash_column} != t.{hash_column}
               OR t.{deleted_ind_column} = 1
        )
        INSERT INTO {target_table} ({', '.join(quote(col) for col in insert_columns)})
        SELECT
            {', '.join(select_parts)}
        FROM candidates s
        ORDER BY rn
        """
    )

    params = {
        "join_numeric_base": max_join_numeric,
        "execution_time": execution_time,
        "effective_date_min": EFFECTIVE_DATE_MIN,
        "effective_date_max": EFFECTIVE_DATE_MAX,
    }
    if has_batch_id and batch_id is not None:
        params["batch_id"] = batch_id
    if has_archive_filename and archive_filename is not None:
        params["archive_filename"] = archive_filename
    insert_result = conn.execute(insert_sql, params)
    if summary.inserted_rows == 0 and (insert_result.rowcount or 0) > 0:
        summary.inserted_rows = insert_result.rowcount or 0
    if summary.edited_rows == 0 and (update_changed_result.rowcount or 0) > 0:
        summary.edited_rows = update_changed_result.rowcount or 0
    if summary.soft_deleted_rows == 0 and (delete_result.rowcount or 0) > 0:
        summary.soft_deleted_rows = delete_result.rowcount or 0
    if summary.reactivated_rows == 0 and (update_reactivated_result.rowcount or 0) > 0:
        summary.reactivated_rows = update_reactivated_result.rowcount or 0
    return summary


def _count_snapshot_changes(
    conn,
    *,
    staging_table: str,
    target_table: str,
    natural_key_cols: Sequence[str],
    data_cols: Sequence[str],
    match_condition: str,
    hash_column: str,
    current_ind_column: str,
    deleted_ind_column: str,
    incoming_rows: int,
    target_rows_before: int,
) -> SCD2ChangeSummary:
    match_condition = match_condition or "1 = 1"
    active_current = f"t.{current_ind_column} = 1 AND t.{deleted_ind_column} = 0"
    deleted_current = f"t.{current_ind_column} = 1 AND t.{deleted_ind_column} = 1"
    edited_rows_detail = _sample_edited_rows_detail(
        conn,
        target_table=target_table,
        staging_table=staging_table,
        natural_key_cols=natural_key_cols,
        data_cols=data_cols,
        match_condition=match_condition,
        hash_column=hash_column,
        active_current=active_current,
    )
    edited_natural_keys = _sample_edited_natural_keys(edited_rows_detail)
    new_natural_keys = _sample_matching_natural_keys(
        conn,
        target_table=target_table,
        natural_key_cols=natural_key_cols,
        sql_filter=(
            f"NOT EXISTS ("
            f"SELECT 1 FROM {target_table} t "
            f"WHERE t.{current_ind_column} = 1 AND {match_condition}"
            f")"
        ),
        table_alias="s",
        source_table=staging_table,
    )
    edited_rows = _scalar_count(
        conn,
        f"""
        SELECT COUNT(1)
        FROM {target_table} t
        WHERE {active_current}
          AND EXISTS (
            SELECT 1
            FROM {staging_table} s
            WHERE {match_condition}
              AND s.{hash_column} != t.{hash_column}
          )
        """,
    )
    soft_deleted_rows = _scalar_count(
        conn,
        f"""
        SELECT COUNT(1)
        FROM {target_table} t
        WHERE {active_current}
          AND NOT EXISTS (
            SELECT 1
            FROM {staging_table} s
            WHERE {match_condition}
          )
        """,
    )
    soft_deleted_natural_keys = _sample_matching_natural_keys(
        conn,
        target_table=target_table,
        natural_key_cols=natural_key_cols,
        sql_filter=(
            f"{active_current} AND NOT EXISTS ("
            f"SELECT 1 FROM {staging_table} s WHERE {match_condition}"
            f")"
        ),
        table_alias="t",
        source_table=target_table,
    )
    reactivated_rows = _scalar_count(
        conn,
        f"""
        SELECT COUNT(1)
        FROM {target_table} t
        WHERE {deleted_current}
          AND EXISTS (
            SELECT 1
            FROM {staging_table} s
            WHERE {match_condition}
          )
        """,
    )
    reactivated_natural_keys = _sample_matching_natural_keys(
        conn,
        target_table=target_table,
        natural_key_cols=natural_key_cols,
        sql_filter=(
            f"{deleted_current} AND EXISTS ("
            f"SELECT 1 FROM {staging_table} s WHERE {match_condition}"
            f")"
        ),
        table_alias="t",
        source_table=target_table,
    )
    new_rows = _scalar_count(
        conn,
        f"""
        SELECT COUNT(1)
        FROM {staging_table} s
        WHERE NOT EXISTS (
            SELECT 1
            FROM {target_table} t
            WHERE t.{current_ind_column} = 1
              AND {match_condition}
        )
        """,
    )
    unchanged_rows = _scalar_count(
        conn,
        f"""
        SELECT COUNT(1)
        FROM {staging_table} s
        WHERE EXISTS (
            SELECT 1
            FROM {target_table} t
            WHERE {active_current}
              AND {match_condition}
              AND s.{hash_column} = t.{hash_column}
        )
        """,
    )
    return SCD2ChangeSummary(
        incoming_rows=incoming_rows,
        target_rows_before=target_rows_before,
        target_rows_after=target_rows_before,
        inserted_rows=new_rows + edited_rows + reactivated_rows,
        new_rows=new_rows,
        edited_rows=edited_rows,
        edited_rows_detail=edited_rows_detail,
        edited_natural_keys=edited_natural_keys,
        soft_deleted_rows=soft_deleted_rows,
        soft_deleted_natural_keys=soft_deleted_natural_keys,
        reactivated_rows=reactivated_rows,
        reactivated_natural_keys=reactivated_natural_keys,
        new_natural_keys=new_natural_keys,
        unchanged_rows=unchanged_rows,
    )


def _scalar_count(conn, sql: str) -> int:
    return conn.execute(text(sql)).scalar() or 0


def _sample_edited_rows_detail(
    conn,
    *,
    target_table: str,
    staging_table: str,
    natural_key_cols: Sequence[str],
    data_cols: Sequence[str],
    match_condition: str,
    hash_column: str,
    active_current: str,
    limit: int = 10,
) -> list[dict[str, object]]:
    if not natural_key_cols:
        return []
    natural_key_select = [
        (
            f"t.{_quote_identifier(conn.engine, col)} AS "
            f"{_quote_identifier(conn.engine, f'key__{col}')}"
        )
        for col in natural_key_cols
    ]
    data_select = []
    for col in data_cols:
        data_select.append(
            f"t.{_quote_identifier(conn.engine, col)} AS "
            f"{_quote_identifier(conn.engine, f'old__{col}')}"
        )
        data_select.append(
            f"s.{_quote_identifier(conn.engine, col)} AS "
            f"{_quote_identifier(conn.engine, f'new__{col}')}"
        )
    select_cols = ", ".join([*natural_key_select, *data_select])
    rows = conn.execute(
        text(
            _select_with_limit(
                conn,
                select_cols=select_cols,
                source_sql=f"""
                FROM {target_table} t
                JOIN {staging_table} s ON {match_condition}
                WHERE {active_current}
                  AND s.{hash_column} != t.{hash_column}
                """,
                limit=limit,
            )
        )
    ).mappings().all()
    result: list[dict[str, object]] = []
    for row in rows:
        keys = {col: row[f"key__{col}"] for col in natural_key_cols}
        changes = {}
        for col in data_cols:
            old_value = row[f"old__{col}"]
            new_value = row[f"new__{col}"]
            if old_value != new_value:
                changes[col] = {"from": old_value, "to": new_value}
        result.append({**keys, "changes": changes})
    return result


def _sample_edited_natural_keys(
    edited_rows_detail: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    keys: list[dict[str, object]] = []
    for row in edited_rows_detail:
        keys.append({key: value for key, value in row.items() if key != "changes"})
    return keys


def _sample_matching_natural_keys(
    conn,
    *,
    target_table: str,
    natural_key_cols: Sequence[str],
    sql_filter: str,
    table_alias: str,
    source_table: str,
    limit: int = 10,
) -> list[dict[str, object]]:
    if not natural_key_cols:
        return []
    select_cols = ", ".join(
        f"{table_alias}.{_quote_identifier(conn.engine, col)}"
        for col in natural_key_cols
    )
    rows = conn.execute(
        text(
            _select_with_limit(
                conn,
                select_cols=select_cols,
                source_sql=f"FROM {source_table} {table_alias} WHERE {sql_filter}",
                limit=limit,
            )
        )
    ).fetchall()
    return [
        {column: row[index] for index, column in enumerate(natural_key_cols)}
        for row in rows
    ]


def _select_with_limit(
    conn,
    *,
    select_cols: str,
    source_sql: str,
    limit: int,
) -> str:
    if conn.engine.dialect.name == "mssql":
        return f"SELECT TOP {limit} {select_cols} {source_sql}"
    return f"SELECT {select_cols} {source_sql} LIMIT {limit}"


def _validate_current_join_numeric_unique(
    conn,
    target_table: str,
    join_numeric_key_col: str,
    current_ind_col: str,
) -> None:
    quote = lambda value: _quote_identifier(conn.engine, value)
    join_numeric_column = quote(join_numeric_key_col)
    current_ind_column = quote(current_ind_col)
    duplicate_sql = text(
        f"""
        SELECT {join_numeric_column}, COUNT(*) AS cnt
        FROM {target_table}
        WHERE {current_ind_column} = 1
        GROUP BY {join_numeric_column}
        HAVING COUNT(*) > 1
        """
    )
    duplicate_rows = conn.execute(duplicate_sql).fetchall()[:10]
    if not duplicate_rows:
        return

    examples = [
        {join_numeric_key_col: row[0]}
        for row in duplicate_rows
    ]
    raise SCD2ValidationError(
        f"Multiple current rows found for join numeric key column "
        f"'{join_numeric_key_col}'. Example duplicate values: {examples}"
    )


def _count_rows(conn, target_table: str) -> int:
    result = conn.execute(text(f"SELECT COUNT(1) FROM {target_table}"))
    return result.scalar() or 0


def _validate_row_growth(
    conn,
    target_table: str,
    previous_total: int,
    latest: int | None = None,
) -> None:
    latest = _count_rows(conn, target_table) if latest is None else latest
    if latest < previous_total:
        raise SCD2ValidationError(
            f"Row count validation failed: table shrank from {previous_total} to {latest}"
        )


def _get_column_types(conn, target_table: str) -> Mapping[str, str]:
    inspector = inspect(conn.engine)
    schema, table = _split_table_name(target_table)
    columns = inspector.get_columns(table, schema=schema)
    result: dict[str, str] = {}
    for column in columns:
        name = column["name"]
        result[name.lower()] = str(column["type"])
    return result


def _column_type_for_column(
    column_name: str,
    column_types: Mapping[str, str],
    engine: Engine,
    *,
    preserve_mssql_types: bool = False,
) -> str:
    key = column_name.lower()
    explicit = column_types.get(key)
    if explicit:
        sanitized = explicit.split()[0]
        if engine.dialect.name == "mssql" and not preserve_mssql_types:
            return "NVARCHAR(4000)"
        return sanitized
    return _column_type_for_engine(engine)


def _column_type_for_engine(engine: Engine) -> str:
    dialect = engine.dialect.name
    if dialect == "sqlite":
        return "INTEGER"
    if dialect == "mssql":
        return "NVARCHAR(4000)"
    return "TEXT"


def _split_table_name(name: str) -> tuple[str | None, str]:
    if "." in name:
        schema, table = name.split(".", 1)
        schema = schema.strip("[]\"")
        table = table.strip("[]\"")
        return schema, table
    return None, name
