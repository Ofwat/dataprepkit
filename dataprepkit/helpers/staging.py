import re
from typing import Literal

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_datetime64tz_dtype
from sqlalchemy import Engine, inspect, text
from sqlalchemy.dialects.mssql import DATETIME2
from sqlalchemy.sql.elements import quoted_name
from dataprepkit.helpers.schema import ensure_schema_exists


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


def stage_dataframe(
    engine: Engine,
    table_name: str,
    df: pd.DataFrame,
    *,
    if_exists: Literal["fail", "replace", "append"] = "replace",
    index: bool = False,
    schema: str | None = None,
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
