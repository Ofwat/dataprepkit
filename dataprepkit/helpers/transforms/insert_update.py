# pylint: disable=C0301
"""
Module: insert_update

This module provides helper functions to support ETL-like operations involving
table creation, validation, and data movement using SQLAlchemy and T-SQL-compatible syntax.

Functions included:
- `create_table_from_existing_table_schema`: Clone a table's schema and optionally add a surrogate key.
- `validate_table_uniqueness`: Check for duplicate business keys in a table.
- `validate_table_no_nulls`: Ensure no NULLs exist in specified business key columns.
- `insert_new_records_dynamic`: Insert new records based on business key uniqueness, generating surrogate keys.
- `update_records_tsql`: Update target records with columns from a source table using join keys.

Private helpers include:
- `_parse_qualified_table`: Parses a string table name in formats like '[schema].[table]' or '[table]'.
- `_make_qualified_table_name`: Rebuilds a SQL-compliant table name string.
- `_table_exists`: Checks whether a table exists in the connected database.
- `_is_table_empty`: Returns True if a table has no rows.
- `_add_surrogate_key_column`: Adds a new surrogate key column to an existing table.
- `_generate_insert_sql`: Creates dynamic T-SQL to insert new records from source to target.

Notes:
- The implementation assumes T-SQL-compatible behavior (e.g., SQL Server).
- Table names are expected to follow the `[schema].[table]` or `[table]` format.
- Surrogate key logic assumes integer-based keys generated via `ROW_NUMBER()`.

Intended for use within pipelines where schema introspection, surrogate key handling,
and data deduplication are necessary before inserts or updates.
"""

import re
import logging
from typing import List, Union, Optional, Dict
from sqlalchemy import Table, MetaData, Column, Integer
from sqlalchemy.engine import Engine
from sqlalchemy import inspect, text

__all__ = [
    "create_table_from_existing_table_schema",
    "validate_table_uniqueness",
    "validate_table_no_nulls",
    "insert_new_records_dynamic",
    "update_records_tsql",
]

logger = logging.getLogger(__name__)


def _parse_qualified_table(qname: str):
    # Support format: [schema].[table]
    pattern = r"^\[(?P<schema>[^\]]+)\]\.\[(?P<table>[^\]]+)\]$"
    match = re.match(pattern, qname)
    if match:
        return match.group("schema"), match.group("table")

    # Support simple table name with no schema
    simple_pattern = r"^\[?(?P<table>\w+)\]?$"
    match = re.match(simple_pattern, qname)
    if match:
        return None, match.group("table")

    raise ValueError(f"Table name '{qname}' is not in the format [schema].[table] or [table]")


def _make_qualified_table_name(schema: Optional[str], table: str) -> str:
    return f"[{schema}].[{table}]" if schema else f"[{table}]"


def _table_exists(engine: Engine, table: str, schema: Optional[str]) -> bool:
    inspector = inspect(engine)
    return inspector.has_table(table, schema=schema)


def _is_table_empty(engine: Engine, qualified_table: str) -> bool:
    with engine.connect() as conn:
        query = f"SELECT COUNT(*) AS cnt FROM {qualified_table}"
        logger.debug("Running query to check if table empty: %s", query)
        result = conn.execute(text(query)).fetchone()
        assert result is not None
        logger.debug("Row count: %s", result.cnt)
        return result.cnt == 0


def _add_surrogate_key_column(engine: Engine, qualified_table: str, surrogate_key: str) -> None:
    alter_sql = f"ALTER TABLE {qualified_table} ADD {surrogate_key} INT;"
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(alter_sql))
    logger.info("Added surrogate key column '%s' to '%s'.", surrogate_key, qualified_table)

# pylint: disable=R0913, R0917
def _generate_insert_sql(
    qualified_source: str,
    qualified_target: str,
    surrogate_key: str,
    common_columns: List[str],
    business_keys: List[str],
    start_id: int = 100
) -> str:
    insert_columns = [surrogate_key] + common_columns
    insert_cols_str = ', '.join(f"[{col}]" for col in insert_columns)
    select_cols_str = ', '.join(f"src.[{col}]" for col in common_columns)

    join_condition = ' AND '.join(f"src.[{key}] = tgt.[{key}]" for key in business_keys)
    null_condition = ' AND '.join(f"tgt.[{key}] IS NULL" for key in business_keys)

    return f"""
        INSERT INTO {qualified_target} (
            {insert_cols_str}
        )
        SELECT
            {start_id} + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS [{surrogate_key}],
            {select_cols_str}
        FROM {qualified_source} src
        LEFT JOIN {qualified_target} tgt
            ON {join_condition}
        WHERE {null_condition};
    """

def _handle_existing_target_table(
    engine: Engine,
    target_schema: Union[str,None],
    target_tbl: str,
    surrogate_key: str,
    qualified_target: str
) -> Dict[str, bool]:
    logger.info("Target table '%s.%s' already exists.", target_schema, target_tbl)
    inspector = inspect(engine)
    target_columns = {col['name'] for col in inspector.get_columns(target_tbl, schema=target_schema)}
    surrogate_key_exists = surrogate_key in target_columns if surrogate_key else True
    target_is_empty = _is_table_empty(engine, qualified_target)

    if surrogate_key and not surrogate_key_exists and target_is_empty:
        logger.info(
            "Target table '%s.%s' is empty and missing surrogate key '%s'. Adding surrogate key column as INTEGER.",
            target_schema, target_tbl, surrogate_key
        )
        _add_surrogate_key_column(engine, qualified_target, surrogate_key)
    else:
        logger.info("Surrogate key exists: %s, target empty: %s", surrogate_key_exists, target_is_empty)

    return {
        "created_table": False,
        "added_surrogate_key": bool(surrogate_key and not surrogate_key_exists and target_is_empty)
    }

def create_table_from_existing_table_schema(
    engine: Engine,
    source_table: str,
    target_table: str,
    surrogate_key: str = ""
) -> Dict[str, bool]:
    """
    Creates a target table based on the schema of an existing source table, optionally adding a surrogate key.

    If the target table already exists:
    - Checks if the surrogate key column exists.
    - If the surrogate key is missing and the target table is empty, adds the surrogate key column as an INTEGER.
    - Otherwise, leaves the target table unchanged.

    If the target table does not exist:
    - Creates it by copying the schema of the source table.
    - Adds the surrogate key column if specified and missing.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy Engine connected to the database.
    source_table : str
        Fully qualified name of the source table (e.g., "[schema].[table]" or "[table]").
    target_table : str
        Fully qualified name of the target table to create or modify.
    surrogate_key : str, optional
        Name of the surrogate key column to add if missing (default is empty string, meaning no surrogate key added).

    Returns
    -------
    Dict[str, bool]
        Dictionary indicating the results of the operation:
        - "created_table": True if the table was created, False if it already existed.
        - "added_surrogate_key": True if a surrogate key column was added, False otherwise.

    Raises
    ------
    Exception
        Propagates any exceptions raised during table creation or modification.

    Examples
    --------
    >>> from sqlalchemy import create_engine
    >>> engine = create_engine("mssql+pyodbc://my_dsn")
    >>> result = create_table_from_existing_table_schema(
    ...     engine,
    ...     source_table='[dbo].[source_table]',
    ...     target_table='[dbo].[target_table]',
    ...     surrogate_key='Assurance_Id'
    ... )
    >>> print(result)
    {'created_table': True, 'added_surrogate_key': True}
    """
    source_schema, source_tbl = _parse_qualified_table(source_table)
    target_schema, target_tbl = _parse_qualified_table(target_table)

    _qualified_source = _make_qualified_table_name(source_schema, source_tbl)
    qualified_target = _make_qualified_table_name(target_schema, target_tbl)

    try:
        if _table_exists(engine, target_tbl, target_schema):
            return _handle_existing_target_table(engine, target_schema, target_tbl, surrogate_key, qualified_target)

        return _create_table_from_source_schema(engine, source_schema, source_tbl, target_schema, target_tbl, surrogate_key)

    except Exception as e:
        logger.error("Error in creating table or adding surrogate key: %s", e)
        raise

def _create_table_from_source_schema(
    engine: Engine,
    source_schema: Union[str, None],
    source_tbl: str,
    target_schema: Union[str, None],
    target_tbl: str,
    surrogate_key: str
) -> Dict[str, bool]:
    inspector = inspect(engine)
    columns_info = inspector.get_columns(source_tbl, schema=source_schema)

    metadata = MetaData(schema=target_schema)
    columns = []

    surrogate_key_created = False
    add_surrogate_key = bool(surrogate_key and surrogate_key.strip())

    for col in columns_info:
        col_name = col['name']
        col_type = col['type']
        columns.append(Column(col_name, col_type))
        if add_surrogate_key and col_name == surrogate_key:
            surrogate_key_created = True

    if add_surrogate_key and not surrogate_key_created:
        columns.insert(0, Column(surrogate_key, Integer))

    Table(target_tbl, metadata, *columns, schema=target_schema)
    metadata.create_all(engine)

    logger.info(
        "Created target table '%s.%s' from source table '%s.%s' schema.",
        target_schema, target_tbl, source_schema, source_tbl
    )
    return {
        "created_table": True,
        "added_surrogate_key": bool(surrogate_key and not surrogate_key_created)
    }


def validate_table_uniqueness(
        engine: Engine,
        qualified_source: str,
        business_keys: List[str]) -> None:
    # pylint: disable=C0301
    """
    Validates that the specified business key columns form a unique constraint in the table.

    This function checks for duplicate rows based on the provided business key columns.
    If any duplicates are found, it raises a `ValueError` and includes a sample of the duplicate keys.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine connected to the database.
    qualified_source : str
        Fully qualified source table name in the format `[schema].[table]`.
    business_keys : List[str]
        List of column names that should form a unique key across the table.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If duplicate combinations of the business key columns are found in the table.

    Example
    -------
    >>> from sqlalchemy import create_engine
    >>> engine = create_engine("mssql+pyodbc://my_dsn")
    >>> validate_table_uniqueness(engine, "[dbo].[customers]", ["CustomerId", "RegionCode"])
    # If duplicates exist, raises ValueError like:
    # ValueError: Duplicate business keys found in table: [('123', 'US'), ('456', 'CA')]...
    """

    with engine.connect() as conn:
        key_expr = ', '.join(f"[{k}]" for k in business_keys)
        sql = f"""
            SELECT {key_expr}, COUNT(*) as cnt
            FROM {qualified_source}
            GROUP BY {key_expr}
            HAVING COUNT(*) > 1;
        """
        result = conn.execute(text(sql)).fetchall()
        if result:
            raise ValueError(f"Duplicate business keys found in table: {result[:5]}...")


def validate_table_no_nulls(engine: Engine, qualified_table: str, business_keys: List[str]) -> None:
    """
    Validates that the specified business key columns in a table do not contain NULL values.

    This function checks the target table for any rows where one or more of the specified
    business key columns contain NULLs. If any such rows are found, a `ValueError` is raised
    with details about how many invalid rows were detected.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine connected to the database.
    qualified_table : str
        Fully qualified table name in the format `[schema].[table]`.
    business_keys : List[str]
        List of column names to check for NULL values.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If any rows contain NULL values in one or more of the specified business key columns.

    Example
    -------
    >>> from sqlalchemy import create_engine
    >>> engine = create_engine("mssql+pyodbc://my_dsn")
    >>> validate_table_no_nulls(engine, "[dbo].[customers]", ["CustomerId", "RegionCode"])
    # If NULLs are present, raises ValueError with details
    """

    with engine.connect() as conn:
        null_conditions = ' OR '.join(f"[{key}] IS NULL" for key in business_keys)
        sql = f"""
            SELECT COUNT(*) AS null_count
            FROM {qualified_table}
            WHERE {null_conditions};
        """
        result = conn.execute(text(sql)).fetchone()
        assert result is not None
        if result.null_count > 0:
            raise ValueError(
                # pylint: disable=C0301
                f"Found {result.null_count} rows with NULL values in business key(s) {business_keys} "
                f"in table '{qualified_table}'."
            )

def insert_new_records_dynamic(
    engine: Engine,
    source_table: str,  # [schema].[table] or table
    target_table: str,
    surrogate_key: str = 'Assurance_Id',
    business_key: Union[str, List[str]] = 'Assurance_Cd',
    default_start_id: int = 100
) -> None:
    # pylint: disable=C0301
    """
    Inserts new records from the source table into the target table based on business key comparison.
    
    A surrogate key is generated for new records using a `ROW_NUMBER()` window function, starting from the
    maximum existing surrogate key in the target table. Records are considered new if their business keys
    do not already exist in the target table.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine connected to the target database.
    source_table : str
        Fully-qualified or unqualified source table name in the format `[schema].[table]` or `[table]`.
    target_table : str
        Fully-qualified or unqualified target table name in the format `[schema].[table]` or `[table]`.
    surrogate_key : str, optional
        The name of the surrogate key column to populate in the target table (default is `'Assurance_Id'`).
    business_key : str or List[str], optional
        One or more column names used to determine whether a record is new. Must exist in both source and target
        tables (default is `'Assurance_Cd'`).
    default_start_id : int, optional
        Starting value for the surrogate key if the target table is empty (default is `100`).

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If source or target table doesn't exist, business keys are missing, or there are no common columns to insert.
    Exception
        If the insert operation fails during execution.

    Example
    -------
    >>> from sqlalchemy import create_engine
    >>> engine = create_engine("mssql+pyodbc://my_dsn")
    >>> insert_new_records_dynamic(
    ...     engine=engine,
    ...     source_table="[staging].[source_data]",
    ...     target_table="[dbo].[target_table]",
    ...     surrogate_key="Record_Id",
    ...     business_key=["Account_Id", "Effective_Date"],
    ...     default_start_id=1000
    ... )
    # Logs: Inserted N new record(s) into '[dbo].[target_table]'
    """

    business_keys = _normalize_business_keys(business_key)
    source_schema, source_tbl = _parse_qualified_table(source_table)
    target_schema, target_tbl = _parse_qualified_table(target_table)
    qualified_source = _make_qualified_table_name(source_schema, source_tbl)
    qualified_target = _make_qualified_table_name(target_schema, target_tbl)

    _validate_tables_and_keys(engine, source_schema, source_tbl, target_schema, target_tbl, qualified_source, qualified_target, business_keys, surrogate_key)

    max_id = _get_max_surrogate_key(engine, surrogate_key, qualified_target, default_start_id)
    insert_sql = _generate_insert_sql(
        qualified_source=qualified_source,
        qualified_target=qualified_target,
        surrogate_key=surrogate_key,
        common_columns=_get_common_columns(engine, source_schema, source_tbl, target_schema, target_tbl, surrogate_key),
        business_keys=business_keys,
        start_id=max_id
    )

    _execute_insert(engine, insert_sql, qualified_target)


def _normalize_business_keys(business_key: Union[str, List[str]]) -> List[str]:
    if isinstance(business_key, str):
        return [business_key]
    if isinstance(business_key, list) and all(isinstance(k, str) for k in business_key):
        return business_key
    raise ValueError("`business_key` must be a string or a list of strings.")


def _validate_tables_and_keys(
    engine: Engine,
    source_schema: Union[str, None],
    source_tbl: str,
    target_schema: Union[str, None],
    target_tbl: str,
    qualified_source: str,
    qualified_target: str,
    business_keys: List[str],
    surrogate_key: str,
) -> None:
    if not _table_exists(engine, source_tbl, source_schema):
        raise ValueError(f"Source table '{qualified_source}' does not exist.")
    if not _table_exists(engine, target_tbl, target_schema):
        raise ValueError(f"Target table '{qualified_target}' does not exist.")

    inspector = inspect(engine)
    source_columns = {col['name'] for col in inspector.get_columns(source_tbl, schema=source_schema)}
    target_columns = {col['name'] for col in inspector.get_columns(target_tbl, schema=target_schema)}

    common_columns = (source_columns & target_columns) - {surrogate_key}
    if not common_columns:
        raise ValueError("No common columns available for insert.")

    missing_keys = [k for k in business_keys if k not in source_columns or k not in target_columns]
    if missing_keys:
        raise ValueError(f"Business key(s) missing in source or target: {missing_keys}")


def _get_common_columns(
    engine: Engine,
    source_schema: Union[str, None],
    source_tbl: str,
    target_schema: Union[str, None],
    target_tbl: str,
    surrogate_key: str
) -> List[str]:
    inspector = inspect(engine)
    source_columns = {col['name'] for col in inspector.get_columns(source_tbl, schema=source_schema)}
    target_columns = {col['name'] for col in inspector.get_columns(target_tbl, schema=target_schema)}
    return sorted((source_columns & target_columns) - {surrogate_key})


def _get_max_surrogate_key(
    engine: Engine,
    surrogate_key: str,
    qualified_target: str,
    default_start_id: int
) -> int:
    with engine.connect() as conn:
        max_id_result = conn.execute(text(f"SELECT MAX([{surrogate_key}]) AS max_id FROM {qualified_target}")).fetchone()
        assert max_id_result is not None
        return max_id_result.max_id if max_id_result.max_id is not None else default_start_id


def _execute_insert(engine: Engine, insert_sql: str, qualified_target: str) -> None:
    with engine.begin() as conn:
        result = conn.execute(text(insert_sql))
        logger.info("Inserted %s new record(s) into '%s'", result.rowcount, qualified_target)

def update_records_tsql(
    engine: Engine,
    target_table: str,
    source_table: str,
    join_keys: Union[str, List[str]],
    surrogate_key: str,
    columns_to_update: List[str]
) -> int:
    # pylint: disable=C0301
    """
    Updates records in the target table using matching rows from the source table,
    based on the specified join keys. Only rows with a non-null surrogate key in
    the target table are updated.

    This function generates and executes a T-SQL `UPDATE ... FROM` statement,
    which performs a set-based update on multiple columns.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine connected to the target database.
    target_table : str
        Fully-qualified or unqualified target table name in the format `[schema].[table]` or `[table]`.
    source_table : str
        Fully-qualified or unqualified source table name in the format `[schema].[table]` or `[table]`.
    join_keys : str or List[str]
        One or more column names used to join source and target tables.
    surrogate_key : str
        Name of the surrogate key column; only rows with a non-null surrogate key in the target are updated.
    columns_to_update : List[str]
        List of column names to update in the target table from the source table.

    Returns
    -------
    int
        The number of rows updated in the target table.

    Raises
    ------
    ValueError
        If `columns_to_update` is empty.

    Example
    -------
    >>> from sqlalchemy import create_engine
    >>> engine = create_engine("mssql+pyodbc://my_dsn")
    >>> rows_updated = update_records_tsql(
    ...     engine=engine,
    ...     target_table="[dbo].[target]",
    ...     source_table="[staging].[source]",
    ...     join_keys=["business_id"],
    ...     surrogate_key="record_id",
    ...     columns_to_update=["col1", "col2"]
    ... )
    >>> print(f"{rows_updated} rows updated.")
    """

    if isinstance(join_keys, str):
        join_keys = [join_keys]

    if not columns_to_update:
        raise ValueError("`columns_to_update` must be a non-empty list.")

    qualified_target = _get_qualified_table(target_table)
    qualified_source = _get_qualified_table(source_table)

    set_clause = _build_set_clause(columns_to_update)
    join_condition = _build_join_condition(join_keys)

    tsql = _build_update_tsql(
        qualified_target=qualified_target,
        qualified_source=qualified_source,
        set_clause=set_clause,
        join_condition=join_condition,
        surrogate_key=surrogate_key
    )

    return _execute_update(engine, tsql, qualified_target)


def _get_qualified_table(table_name: str) -> str:
    schema, tbl = _parse_qualified_table(table_name)
    return _make_qualified_table_name(schema, tbl)


def _build_set_clause(columns: List[str]) -> str:
    return ",\n    ".join(f"tgt.[{col}] = src.[{col}]" for col in columns)


def _build_join_condition(join_keys: List[str]) -> str:
    return " AND ".join(f"tgt.[{key}] = src.[{key}]" for key in join_keys)


def _build_update_tsql(
    qualified_target: str,
    qualified_source: str,
    set_clause: str,
    join_condition: str,
    surrogate_key: str
) -> str:
    return f"""
    UPDATE tgt
    SET
        {set_clause}
    FROM {qualified_target} tgt
    INNER JOIN {qualified_source} src
        ON {join_condition}
    WHERE tgt.[{surrogate_key}] IS NOT NULL;
    """


def _execute_update(engine: Engine, tsql: str, qualified_target: str) -> int:
    with engine.begin() as conn:
        result = conn.execute(text(tsql))
        logger.info("Updated %s rows in %s.", result.rowcount, qualified_target)
        return result.rowcount
