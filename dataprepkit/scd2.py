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
from sqlalchemy import text


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

    if incoming_df.duplicated(subset=list(natural_key_cols)).any():
        raise SCD2ValidationError("Incoming data contains duplicate natural keys.")

    hash_col = cols["row_hash"]
    incoming_df[hash_col] = incoming_df.apply(lambda row: _compute_row_hash(row, data_cols), axis=1)

    with engine.begin() as conn:
        existing = pd.read_sql_table(target_table, con=engine)
        current_mask = existing[cols["current_ind"]] == 1
        current_rows = existing[current_mask]

        existing_map = {
            _make_key(record, natural_key_cols): record
            for _, record in current_rows.iterrows()
        }

        existing_keys = set(existing_map)
        incoming_map = {
            _make_key(row, natural_key_cols): row for _, row in incoming_df.iterrows()
        }
        incoming_keys = set(incoming_map)

        to_delete = existing_keys - incoming_keys
        to_change = []
        to_insert = []

        for key in incoming_keys:
            if key not in existing_keys:
                to_insert.append(key)
                continue
            existing_row = existing_map[key]
            incoming_row = incoming_map[key]
            if existing_row[hash_col] != incoming_row[hash_col]:
                to_change.append(key)
            else:
                # row unchanged; skip it explicitly
                continue

        now = _execution_timestamp()
        where_clause = " AND ".join(f"{col} = :{col}" for col in natural_key_cols)

        for key in to_delete:
            _update_row_state(
                conn,
                target_table,
                natural_key_cols,
                key,
                cols,
                now,
                deleted=1,
            )

        for key in to_change:
            _update_row_state(
                conn,
                target_table,
                natural_key_cols,
                key,
                cols,
                now,
                deleted=0,
            )

        insert_keys = sorted(to_insert + to_change)
        max_join_numeric = existing[join_numeric_key_col].max()
        if pd.isna(max_join_numeric):
            next_join_numeric = 0
        else:
            next_join_numeric = int(max_join_numeric)

        insert_columns = list(natural_key_cols) + list(data_cols) + [
            join_numeric_key_col,
            hash_col,
            cols["insert_date"],
            cols["update_date"],
            cols["current_ind"],
            cols["deleted_ind"],
        ]
        insert_sql = text(
            f"""
            INSERT INTO {target_table} (
                {", ".join(insert_columns)}
            ) VALUES (
                {", ".join(":" + col for col in insert_columns)}
            )
            """
        )

        for key in insert_keys:
            incoming_row = incoming_map[key]
            next_join_numeric += 1
            join_numeric = next_join_numeric

            params = {col: incoming_row[col] for col in natural_key_cols + list(data_cols)}
            params[join_numeric_key_col] = join_numeric
            params[hash_col] = incoming_row[hash_col]
            params[cols["insert_date"]] = now
            params[cols["update_date"]] = None
            params[cols["current_ind"]] = 1
            params[cols["deleted_ind"]] = 0

            conn.execute(insert_sql, params)


def _execution_timestamp() -> str:
    now = datetime.now(timezone.utc)
    milliseconds = (now.microsecond // 1000) * 1000
    truncated = now.replace(microsecond=milliseconds)
    return truncated.isoformat(timespec="milliseconds")


def _compute_row_hash(row: pd.Series, data_columns: Sequence[str]) -> str:
    tokens = []
    for column in sorted(data_columns):
        value = row.get(column)
        if pd.isna(value):
            value = ""
        tokens.append(f"{column}={value}")
    return hashlib.sha256("|".join(tokens).encode("utf-8")).hexdigest()


def _make_key(row, natural_key_cols: Sequence[str]) -> tuple:
    return tuple(row[col] for col in natural_key_cols)


def _update_row_state(
    conn,
    target_table: str,
    natural_key_cols: Sequence[str],
    key: tuple,
    columns: Mapping[str, str],
    update_ts: str,
    deleted: int,
) -> None:
    clause = " AND ".join(f"{col} = :{col}" for col in natural_key_cols)
    update_sql = text(
        f"""
        UPDATE {target_table}
        SET {columns['current_ind']} = 0,
            {columns['update_date']} = :update_ts,
            {columns['deleted_ind']} = :deleted
        WHERE {clause} AND {columns['current_ind']} = 1
        """
    )
    params = dict(zip(natural_key_cols, key))
    params.update(update_ts=update_ts, deleted=deleted)
    conn.execute(update_sql, params)
