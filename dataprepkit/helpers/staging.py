import re
from typing import Literal
from pathlib import Path
import uuid

import pandas as pd
from pandas.api.types import (
    is_datetime64_any_dtype,
    is_datetime64tz_dtype,
    is_object_dtype,
)
from sqlalchemy import Engine, inspect, text
from sqlalchemy.dialects.mssql import DATETIME2
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql.elements import quoted_name
from dataprepkit.helpers.schema import ensure_schema_exists
from dataprepkit.storage import archive_dataframe_path


def _quote_mssql_identifier(identifier: str) -> str:
    return f"[{identifier.replace(']', ']]')}]"


def _normalize_bracket_identifier(name: str | None) -> str | None:
    if name is None:
        return None
    stripped = name.strip()
    if stripped.startswith("[") and stripped.endswith("]"):
        return stripped[1:-1].replace("]]", "]")
    return stripped


def _split_qualified_name(name: str) -> tuple[str | None, str]:
    in_brackets = False
    idx = 0
    while idx < len(name):
        char = name[idx]
        if char == "[" and not in_brackets:
            in_brackets = True
        elif char == "]" and in_brackets:
            if idx + 1 < len(name) and name[idx + 1] == "]":
                idx += 1
            else:
                in_brackets = False
        elif char == "." and not in_brackets:
            return name[:idx], name[idx + 1 :]
        idx += 1
    return None, name


def _normalize_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    for col in normalized.columns:
        series = normalized[col]
        if is_datetime64_any_dtype(series.dtype) or is_datetime64tz_dtype(series.dtype):
            normalized[col] = series.map(
                lambda value: (
                    None
                    if pd.isna(value)
                    else pd.Timestamp(value).strftime("%Y-%m-%d %H:%M:%S.%f")
                )
            )
            continue
        if not is_object_dtype(series.dtype):
            continue
        normalized[col] = series.map(
            lambda value: (
                None
                if pd.isna(value)
                else value.decode("utf-8", errors="replace")
                if isinstance(value, (bytes, bytearray))
                else str(value)
            )
        )
    return normalized


def stage_dataframe(
    engine: Engine,
    table_name: str,
    df: pd.DataFrame,
    *,
    if_exists: Literal["fail", "replace", "append"] = "replace",
    index: bool = False,
    schema: str | None = None,
    use_copy_into_parquet: bool = False,
    parquet_base_dir: str | None = None,
    copy_source_base_url: str | None = None,
    copy_into_options: str = "",
    openrowset_max_rows_per_file: int = 1_000_000,
) -> None:
    """
    Write a DataFrame into a staging table (generic helper).

    Parameters
    ----------
    engine
        SQLAlchemy engine.
    table_name
        Target table name (schema qualified if needed).
    df
        DataFrame to persist.
    if_exists
        pandas strategy when the table already exists.
    index
        Whether to write DataFrame index.
    schema
        Optional schema name.
    use_copy_into_parquet
        If True, write a parquet snapshot and load via COPY INTO (MSSQL/Fabric).
    parquet_base_dir
        Local/mounted base directory where parquet snapshots are written.
    copy_source_base_url
        Optional SQL-visible base path/URI for OPENROWSET BULK. Defaults to parquet_base_dir.
    copy_into_options
        Additional COPY INTO options suffix (for example: ", MAXERRORS = 10").
    openrowset_max_rows_per_file
        Maximum rows per parquet part file for OPENROWSET loads.
    """
    resolved_schema = _normalize_bracket_identifier(schema)
    resolved_table = table_name.strip()
    if resolved_schema is None:
        name_schema, name_table = _split_qualified_name(resolved_table)
        if name_schema is not None:
            resolved_schema = _normalize_bracket_identifier(name_schema)
            resolved_table = name_table
    resolved_table = _normalize_bracket_identifier(resolved_table) or resolved_table
    ensure_schema_exists(engine, resolved_schema)
    schema_for_sql: str | None = resolved_schema
    table_for_sql: str = resolved_table
    if engine.dialect.name == "mssql":
        if schema_for_sql is not None:
            schema_for_sql = quoted_name(schema_for_sql, True)
        table_for_sql = quoted_name(table_for_sql, True)
    else:
        schema_for_sql = None

    if engine.dialect.name == "mssql":
        dtype_overrides = {
            col: DATETIME2(precision=3)
            for col, dtype in df.dtypes.items()
            if is_datetime64_any_dtype(dtype) or is_datetime64tz_dtype(dtype)
        }

        if use_copy_into_parquet:
            if not parquet_base_dir:
                raise ValueError("parquet_base_dir is required when use_copy_into_parquet=True.")
            if openrowset_max_rows_per_file < 1:
                raise ValueError("openrowset_max_rows_per_file must be >= 1.")
            resolved_copy_source_base_url = copy_source_base_url or parquet_base_dir

            table_exists = inspect(engine).has_table(
                str(table_for_sql),
                schema=str(schema_for_sql) if schema_for_sql is not None else None,
            )
            if if_exists == "fail" and table_exists:
                raise ValueError(f"Table '{table_name}' already exists.")
            if not table_exists:
                df.head(0).to_sql(
                    table_for_sql,
                    engine,
                    if_exists="fail",
                    index=index,
                    schema=schema_for_sql,
                    dtype=dtype_overrides or None,
                )

            path_info = archive_dataframe_path(
                table_name=resolved_table,
                batch_id=f"stage_{uuid.uuid4().hex[:8]}",
                base_dir=parquet_base_dir,
            )
            normalized_df = _normalize_for_parquet(df)
            parquet_path = Path(path_info.file_path)
            part_dir = parquet_path.with_suffix("")
            part_dir.mkdir(parents=True, exist_ok=True)
            total_rows = len(normalized_df)
            if total_rows == 0:
                normalized_df.to_parquet(part_dir / "part-00000.parquet", index=index)
            else:
                for part_idx, start in enumerate(
                    range(0, total_rows, openrowset_max_rows_per_file)
                ):
                    chunk = normalized_df.iloc[start : start + openrowset_max_rows_per_file]
                    chunk.to_parquet(part_dir / f"part-{part_idx:05d}.parquet", index=index)

            source_url = (
                f"{resolved_copy_source_base_url.rstrip('/')}/{resolved_table}/{part_dir.name}/*.parquet"
            )
            schema_sql = (
                _quote_mssql_identifier(str(schema_for_sql))
                if schema_for_sql is not None
                else None
            )
            table_sql = _quote_mssql_identifier(str(table_for_sql))
            destination_table = (
                f"{schema_sql}.{table_sql}" if schema_sql is not None else table_sql
            )

            options_sql = copy_into_options.strip()
            if options_sql and not options_sql.startswith(","):
                options_sql = f", {options_sql}"

            with engine.begin() as conn:
                if if_exists == "replace" and table_exists:
                    try:
                        conn.execute(text(f"TRUNCATE TABLE {destination_table}"))
                    except ProgrammingError:
                        conn.execute(text(f"DELETE FROM {destination_table}"))
                selected_columns = [_quote_mssql_identifier(str(col)) for col in df.columns]
                if not selected_columns:
                    return
                columns_sql = ", ".join(selected_columns)
                escaped_source = source_url.replace("'", "''")
                openrowset_sql = text(
                    f"""
                    INSERT INTO {destination_table} ({columns_sql})
                    SELECT {columns_sql}
                    FROM OPENROWSET(
                        BULK '{escaped_source}',
                        FORMAT = 'PARQUET'{options_sql}
                    ) AS src
                    """
                )
                conn.execute(openrowset_sql)
            return

        if dtype_overrides:
            df.to_sql(
                table_for_sql,
                engine,
                if_exists=if_exists,
                index=index,
                schema=schema_for_sql,
                dtype=dtype_overrides,
            )
            return

    df.to_sql(
        table_for_sql,
        engine,
        if_exists=if_exists,
        index=index,
        schema=schema_for_sql,
    )


def union_tables_by_name_regex(
    engine: Engine,
    schema: str | None,
    table_name_regex: str,
    output_table_name: str,
) -> None:
    """
    Union all rows from tables whose names match a regex into one output table.

    Parameters
    ----------
    engine
        SQLAlchemy engine.
    schema
        Optional schema name used to discover source tables and write output.
    table_name_regex
        Regular expression applied to table names (not schema-qualified names).
    output_table_name
        Destination table name where the unioned result is written.
    """
    resolved_schema = _normalize_bracket_identifier(schema)
    ensure_schema_exists(engine, resolved_schema)
    schema_for_sql: str | None = resolved_schema
    output_table_for_sql: str = (
        _normalize_bracket_identifier(output_table_name) or output_table_name.strip()
    )
    if engine.dialect.name == "mssql":
        if schema_for_sql is not None:
            schema_for_sql = quoted_name(schema_for_sql, True)
        output_table_for_sql = quoted_name(output_table_for_sql, True)
    else:
        schema_for_sql = None

    pattern = re.compile(table_name_regex)
    inspector = inspect(engine)
    table_names = inspector.get_table_names(schema=schema_for_sql)
    output_table_compare = str(output_table_for_sql)
    matched_tables = sorted(
        name for name in table_names if name != output_table_compare and pattern.search(name)
    )
    if not matched_tables:
        raise ValueError(
            f"No tables matched regex '{table_name_regex}' in schema '{schema_for_sql}'."
        )

    dataframes = [
        pd.read_sql_table(name, con=engine, schema=schema_for_sql)
        for name in matched_tables
    ]
    union_df = pd.concat(dataframes, ignore_index=True)
    union_df.to_sql(
        output_table_for_sql,
        con=engine,
        if_exists="replace",
        index=False,
        schema=schema_for_sql,
    )


def drop_tables_by_name_regex(
    engine: Engine,
    schema: str | None,
    table_name_regex: str,
) -> list[str]:
    """
    Drop all tables in a schema whose names match a regex.

    Parameters
    ----------
    engine
        SQLAlchemy engine.
    schema
        Optional schema name used to discover and drop tables.
    table_name_regex
        Regular expression applied to table names (not schema-qualified names).
    """
    resolved_schema = _normalize_bracket_identifier(schema)
    ensure_schema_exists(engine, resolved_schema)
    schema_for_sql: str | None = resolved_schema
    if engine.dialect.name == "mssql":
        if schema_for_sql is not None:
            schema_for_sql = quoted_name(schema_for_sql, True)
    else:
        schema_for_sql = None

    pattern = re.compile(table_name_regex)
    inspector = inspect(engine)
    matched_tables = sorted(
        name for name in inspector.get_table_names(schema=schema_for_sql) if pattern.search(name)
    )
    if not matched_tables:
        return []

    with engine.begin() as conn:
        for name in matched_tables:
            if schema_for_sql is not None:
                if engine.dialect.name == "mssql":
                    table_ref = (
                        f"{_quote_mssql_identifier(str(schema_for_sql))}."
                        f"{_quote_mssql_identifier(name)}"
                    )
                else:
                    table_ref = f"{schema_for_sql}.{name}"
            else:
                table_ref = (
                    _quote_mssql_identifier(name)
                    if engine.dialect.name == "mssql"
                    else name
                )
            conn.execute(text(f"DROP TABLE {table_ref}"))
    return matched_tables
