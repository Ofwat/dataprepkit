import re
from typing import Literal

import pandas as pd
from sqlalchemy import Engine, inspect
from dataprepkit.helpers.schema import ensure_schema_exists


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
    ensure_schema_exists(engine, schema)
    schema_for_sql = schema if engine.dialect.name == "mssql" else None

    df.to_sql(
        table_name,
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
    ensure_schema_exists(engine, schema)
    schema_for_sql = schema if engine.dialect.name == "mssql" else None

    pattern = re.compile(table_name_regex)
    inspector = inspect(engine)
    table_names = inspector.get_table_names(schema=schema_for_sql)
    matched_tables = sorted(
        name for name in table_names if name != output_table_name and pattern.search(name)
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
        output_table_name,
        con=engine,
        if_exists="replace",
        index=False,
        schema=schema_for_sql,
    )
