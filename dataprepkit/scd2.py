"""
SCD2 utilities for deterministic dimension management.

Provides an atomic `apply_changes` helper that ingests pandas data and
updates a target table with system columns (surrogate keys, hashes, flags).

The logic assumes the target table already exists and exposes the system
columns indicated via the configuration mapping.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Mapping, Sequence

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError
import uuid


DEFAULT_SYSTEM_COLUMNS = {
    "surrogate_key": "surrogate_key",
    "join_numeric_key": "join_numeric_key",
    "row_hash": "row_hash",
    "insert_date": "Insert_Date",
    "update_date": "Update_Date",
    "current_ind": "Current_Ind",
    "deleted_ind": "Deleted_Ind",
}


class SCD2ValidationError(ValueError):
    """Raised when the provided data does not satisfy the required schema."""


def apply_changes(
    engine: Engine,
    target_table: str,
    incoming: pd.DataFrame,
    natural_key_cols: Sequence[str],
    data_cols: Sequence[str],
    join_numeric_key_col: str,
    surrogate_key_col: str,
    system_columns: Mapping[str, str] | None = None,
    execution_time: str | None = None,
) -> None:
    """
    Apply SCD2 semantics to the target table using the incoming DataFrame.

    The function computes row hashes, detects inserts/deletes/updates, expires
    old rows, and inserts new rows while respecting the configured system
    columns. All writes happen within a single transaction for atomicity.
    """
    cols = system_columns or DEFAULT_SYSTEM_COLUMNS
    required_keys = {"row_hash", "insert_date", "update_date", "current_ind", "deleted_ind"}
    missing = required_keys - cols.keys()
    if missing:
        raise SCD2ValidationError(f"Missing system column configuration: {missing}")

    incoming_df = pd.DataFrame(incoming).copy()
    if not set(natural_key_cols).issubset(incoming_df.columns):
        raise SCD2ValidationError("Incoming data must include all natural key columns.")
    if not set(data_cols).issubset(incoming_df.columns):
        raise SCD2ValidationError("Incoming data must include all declared data columns.")

    hash_col = cols["row_hash"]
    incoming_df[hash_col] = incoming_df.apply(lambda row: _compute_row_hash(row, data_cols), axis=1)

    base_name = f"temp_snapshot_{uuid.uuid4().hex}"
    staging_table = _resolve_staging_table_name(engine, base_name)
    execution_time = execution_time or _execution_timestamp()

    with engine.begin() as conn:
        pre_total = _count_rows(conn, target_table)
        column_types = _get_column_types(conn, target_table)
        _create_staging_table(
            conn,
            staging_table,
            natural_key_cols,
            data_cols,
            hash_col,
            column_types,
        )
        try:
            _insert_snapshot_rows(conn, staging_table, incoming_df, natural_key_cols, data_cols, hash_col)
            _apply_snapshot_to_target(
                conn,
                staging_table,
                target_table,
                natural_key_cols,
                data_cols,
                join_numeric_key_col,
                cols,
                hash_col,
                execution_time,
            )
        finally:
            conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))
        _validate_row_growth(conn, target_table, pre_total)


def _execution_timestamp() -> str:
    now = datetime.now(timezone.utc)
    milliseconds = (now.microsecond // 1000) * 1000
    truncated = now.replace(microsecond=milliseconds)
    return truncated.isoformat(timespec="milliseconds")


def _resolve_staging_table_name(engine: Engine, base_name: str) -> str:
    dialect = engine.dialect.name
    if dialect == "mssql":
        return f"#{base_name}"
    if dialect == "sqlite":
        return f"temp_{base_name}"
    return base_name


def _compute_row_hash(row: pd.Series, data_columns: Sequence[str]) -> str:
    tokens = []
    for column in sorted(data_columns):
        value = row.get(column)
        if pd.isna(value):
            value = ""
        tokens.append(f"{column}={value}")
    return hashlib.sha256("|".join(tokens).encode("utf-8")).hexdigest()


def _create_staging_table(
    conn,
    table_name,
    natural_key_cols,
    data_cols,
    hash_col,
    column_types: Mapping[str, str],
):
    dialect = conn.engine.dialect.name
    column_defs = []
    for col in natural_key_cols + data_cols:
        column_defs.append(
            f"{col} {_column_type_for_column(col, column_types, conn.engine)} NOT NULL"
        )
    column_defs.append(
        f"{hash_col} {_column_type_for_column(hash_col, column_types, conn.engine)} NOT NULL"
    )
    unique_clause = f", UNIQUE({', '.join(natural_key_cols)})" if natural_key_cols else ""
    create_sql = text(
        f"""
        CREATE {'TABLE' if dialect != 'sqlite' else 'TEMP TABLE'} {table_name} (
            {', '.join(column_defs)}
            {unique_clause}
        )
        """
    )
    conn.execute(create_sql)


def _insert_snapshot_rows(conn, table_name, incoming_df, natural_key_cols, data_cols, hash_col):
    columns = list(natural_key_cols) + list(data_cols) + [hash_col]
    insert_sql = text(
        f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES ({', '.join(':' + col for col in columns)})
        """
    )
    records = [
        {col: row[col] for col in columns}
        for _, row in incoming_df[columns].iterrows()
    ]
    try:
        if records:
            conn.execute(insert_sql, records)
    except IntegrityError as exc:
        raise SCD2ValidationError("Incoming data contains duplicate natural keys.") from exc


def _build_join_condition(left_alias: str, right_alias: str, columns: Sequence[str]) -> str:
    return " AND ".join(f"{left_alias}.{col} = {right_alias}.{col}" for col in columns)


def _apply_snapshot_to_target(
    conn,
    staging_table: str,
    target_table: str,
    natural_key_cols: Sequence[str],
    data_cols: Sequence[str],
    join_numeric_key_col: str,
    columns: Mapping[str, str],
    hash_col: str,
    execution_time: str,
):
    join_condition = _build_join_condition(target_table, "s", natural_key_cols)

    delete_sql = text(
        f"""
        UPDATE {target_table}
        SET {columns['current_ind']} = 0,
            {columns['deleted_ind']} = 1,
            {columns['update_date']} = :execution_time
        WHERE {columns['current_ind']} = 1
          AND NOT EXISTS (
            SELECT 1 FROM {staging_table} s
            WHERE {join_condition}
          )
        """
    )
    conn.execute(delete_sql, {"execution_time": execution_time})

    update_sql = text(
        f"""
        UPDATE {target_table}
        SET {columns['current_ind']} = 0,
            {columns['update_date']} = :execution_time,
            {columns['deleted_ind']} = 0
        WHERE {columns['current_ind']} = 1
          AND EXISTS (
            SELECT 1 FROM {staging_table} s
            WHERE {join_condition}
              AND s.{hash_col} != {target_table}.{hash_col}
          )
        """
    )
    conn.execute(update_sql, {"execution_time": execution_time})

    max_join_sql = text(f"SELECT COALESCE(MAX({join_numeric_key_col}), 0) FROM {target_table}")
    max_join_numeric = conn.execute(max_join_sql).scalar() or 0

    order_by = ", ".join(f"s.{col}" for col in natural_key_cols) or "1"

    insert_columns = list(natural_key_cols) + list(data_cols) + [
        join_numeric_key_col,
        hash_col,
        columns["insert_date"],
        columns["update_date"],
        columns["current_ind"],
        columns["deleted_ind"],
    ]

    current_join_condition = (
        f"{_build_join_condition('t', 's', natural_key_cols)} AND t.{columns['current_ind']} = 1"
        if natural_key_cols
        else f"t.{columns['current_ind']} = 1"
    )

    insert_sql = text(
        f"""
        WITH candidates AS (
            SELECT
                s.*,
                ROW_NUMBER() OVER (ORDER BY {order_by}) AS rn
            FROM {staging_table} s
            LEFT JOIN {target_table} t ON {current_join_condition}
            WHERE t.{columns['current_ind']} IS NULL
               OR s.{hash_col} != t.{hash_col}
        )
        INSERT INTO {target_table} ({', '.join(insert_columns)})
        SELECT
            {', '.join(f"s.{col}" for col in natural_key_cols)},
            {', '.join(f"s.{col}" for col in data_cols)},
            :join_numeric_base + rn,
            s.{hash_col},
            :execution_time,
            NULL,
            1,
            0
        FROM candidates s
        ORDER BY rn
        """
    )

    conn.execute(
        insert_sql,
        {"join_numeric_base": max_join_numeric, "execution_time": execution_time},
    )


def _count_rows(conn, target_table: str) -> int:
    result = conn.execute(text(f"SELECT COUNT(1) FROM {target_table}"))
    return result.scalar() or 0


def _validate_row_growth(conn, target_table: str, previous_total: int) -> None:
    latest = _count_rows(conn, target_table)
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
    column_name: str, column_types: Mapping[str, str], engine: Engine
) -> str:
    key = column_name.lower()
    explicit = column_types.get(key)
    if explicit:
        sanitized = explicit.split()[0]
        if engine.dialect.name == "mssql":
            if sanitized.upper() == "TEXT":
                return "NVARCHAR(4000)"
            return "NVARCHAR(4000)"
        return sanitized
    return _column_type_for_engine(engine)


def _split_table_name(name: str) -> tuple[str | None, str]:
    if "." in name:
        schema, table = name.split(".", 1)
        schema = schema.strip("[]\"")
        table = table.strip("[]\"")
        return schema, table
    return None, name
