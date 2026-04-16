from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine, inspect, text

from dataprepkit.helpers.schema import ensure_schema_exists
from dataprepkit.storage import archive_dataframe_path


def _quote_identifier(engine: Engine, identifier: str) -> str:
    if engine.dialect.name == "mssql":
        return f"[{identifier.replace(']', ']]')}]"
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def _render_table_name(engine: Engine, schema: str | None, table: str) -> str:
    table_sql = _quote_identifier(engine, table)
    if schema:
        return f"{_quote_identifier(engine, schema)}.{table_sql}"
    return table_sql


def _compile_column_type(engine: Engine, column_type: object) -> str:
    if hasattr(column_type, "compile"):
        compiled = column_type.compile(dialect=engine.dialect)
        if compiled:
            return str(compiled)
    return str(column_type)


def _build_column_definition(name: str, column_type: str, *, nullable: bool) -> str:
    null_sql = "" if nullable else " NOT NULL"
    return f"{name} {column_type}{null_sql}"


def _default_string_type(engine: Engine) -> str:
    if engine.dialect.name == "mssql":
        return "NVARCHAR(4000)"
    return "TEXT"


def _default_datetime_type(engine: Engine) -> str:
    if engine.dialect.name == "mssql":
        return "DATETIME2(3)"
    return "TEXT"


def _fact_pk_clause(engine: Engine, column_name: str) -> str:
    if engine.dialect.name == "mssql":
        return f"{_quote_identifier(engine, column_name)} INT IDENTITY(1,1) PRIMARY KEY"
    if engine.dialect.name == "sqlite":
        return (
            f"{_quote_identifier(engine, column_name)} "
            "INTEGER PRIMARY KEY AUTOINCREMENT"
        )
    return f"{_quote_identifier(engine, column_name)} INT PRIMARY KEY"


def _get_column_type(
    engine: Engine,
    *,
    schema: str | None,
    table: str,
    column: str,
) -> str:
    inspector = inspect(engine)
    for candidate in inspector.get_columns(table, schema=schema):
        if candidate["name"] == column:
            return _compile_column_type(engine, candidate["type"])
    location = f"{schema}.{table}" if schema else table
    raise ValueError(f"Column '{column}' not found in '{location}'.")


def _get_table_columns(
    engine: Engine,
    *,
    schema: str | None,
    table: str,
) -> set[str]:
    inspector = inspect(engine)
    return {candidate["name"] for candidate in inspector.get_columns(table, schema=schema)}


def _get_current_indicator_column(
    engine: Engine,
    *,
    schema: str | None,
    table: str,
) -> str | None:
    for column in _get_table_columns(engine, schema=schema, table=table):
        if column.lower() == "current_ind":
            return column
    return None


def _validate_staging_columns(
    engine: Engine,
    *,
    schema: str | None,
    table: str,
    lookup_map: Mapping[str, Mapping[str, object]],
    data_columns: Sequence[Mapping[str, str]],
) -> tuple[set[str], set[str]]:
    staging_columns = _get_table_columns(engine, schema=schema, table=table)
    lookup_columns = set(lookup_map)
    data_column_names = {column["column"] for column in data_columns}

    missing_lookup_columns = sorted(lookup_columns - staging_columns)
    if missing_lookup_columns:
        location = f"{schema}.{table}" if schema else table
        print(
            "Warning: missing lookup staging columns in "
            f"'{location}': {', '.join(missing_lookup_columns)}"
        )

    missing_data_columns = sorted(data_column_names - staging_columns)
    if missing_data_columns:
        location = f"{schema}.{table}" if schema else table
        raise ValueError(
            f"Missing required data columns in '{location}': {', '.join(missing_data_columns)}"
        )

    used_columns = lookup_columns | data_column_names
    extra_columns = sorted(staging_columns - used_columns)
    if extra_columns:
        location = f"{schema}.{table}" if schema else table
        print(
            f"Warning: unused staging columns in '{location}': {', '.join(extra_columns)}"
        )
    return staging_columns, set(missing_lookup_columns)


def _archive_source_table_snapshot(
    engine: Engine,
    *,
    schema: str | None,
    table: str,
    batch_id: str,
    archive_base_dir: str,
) -> tuple[str, str]:
    archive_path = archive_dataframe_path(
        table_name=table,
        batch_id=batch_id,
        base_dir=archive_base_dir,
    )
    output_path = Path(archive_path.file_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    source_df = pd.read_sql_query(
        text(f"SELECT * FROM {_render_table_name(engine, schema, table)}"),
        con=engine,
    )
    source_df.to_parquet(output_path, index=False)
    return str(output_path), output_path.name


def _apply_comments(
    engine: Engine,
    *,
    schema: str | None,
    table: str,
    table_comment: str | None,
    column_comments: Mapping[str, str],
) -> None:
    if engine.dialect.name != "mssql":
        return
    if not table_comment and not column_comments:
        return

    statements = [
        "DECLARE @schema SYSNAME = :schema;",
        "DECLARE @table SYSNAME = :table;",
        "DECLARE @columns TABLE (ColumnName SYSNAME, Comment NVARCHAR(4000));",
    ]
    params: dict[str, str] = {"schema": schema or "dbo", "table": table}

    if table_comment:
        statements.append("DECLARE @description NVARCHAR(4000) = :description;")
        params["description"] = table_comment
        statements.append(
            """
BEGIN TRY
  EXEC sys.sp_updateextendedproperty
    @name=N'MS_Description', @value=@description,
    @level0type=N'SCHEMA', @level0name=@schema,
    @level1type=N'TABLE', @level1name=@table;
END TRY
BEGIN CATCH
  EXEC sys.sp_addextendedproperty
    @name=N'MS_Description', @value=@description,
    @level0type=N'SCHEMA', @level0name=@schema,
    @level1type=N'TABLE', @level1name=@table;
END CATCH;
"""
        )

    for index, (column, comment) in enumerate(column_comments.items()):
        column_key = f"column_{index}"
        comment_key = f"comment_{index}"
        statements.append(
            f"INSERT INTO @columns (ColumnName, Comment) VALUES (:{column_key}, :{comment_key});"
        )
        params[column_key] = column
        params[comment_key] = comment

    statements.append(
        """
DECLARE @col SYSNAME, @comment NVARCHAR(4000);
WHILE EXISTS (SELECT 1 FROM @columns)
BEGIN
  SELECT TOP 1 @col = ColumnName, @comment = Comment FROM @columns;
  BEGIN TRY
    EXEC sys.sp_updateextendedproperty
      @name=N'MS_Description', @value=@comment,
      @level0type=N'SCHEMA', @level0name=@schema,
      @level1type=N'TABLE', @level1name=@table,
      @level2type=N'COLUMN', @level2name=@col;
  END TRY
  BEGIN CATCH
    EXEC sys.sp_addextendedproperty
      @name=N'MS_Description', @value=@comment,
      @level0type=N'SCHEMA', @level0name=@schema,
      @level1type=N'TABLE', @level1name=@table,
      @level2type=N'COLUMN', @level2name=@col;
  END CATCH;
  DELETE FROM @columns WHERE ColumnName = @col;
END
"""
    )

    with engine.begin() as conn:
        conn.execute(text("\n".join(statements)), params)


def load_fact_from_maps(
    *,
    engine: Engine,
    lookup_map: Mapping[str, Mapping[str, object]],
    data_columns: Sequence[Mapping[str, str]],
    additional_columns: Sequence[Mapping[str, object]],
    metadata_columns: Sequence[Mapping[str, object]] | None = None,
    runtime_values: Mapping[str, object] | None = None,
    staging_table: str,
    staging_schema: str | None,
    fact_table: str,
    fact_schema: str | None = None,
    fact_pk_column: str | None = None,
    archive_base_dir: str | None = None,
    table_comment: str | None = None,
) -> None:
    ensure_schema_exists(engine, fact_schema)
    staging_columns, missing_columns = _validate_staging_columns(
        engine,
        schema=staging_schema,
        table=staging_table,
        lookup_map=lookup_map,
        data_columns=data_columns,
    )

    column_definitions: list[tuple[str, str, bool]] = []
    column_comments: dict[str, str] = {}
    base_selects: list[str] = []
    base_joins: list[str] = []
    metadata_selects: list[str] = []
    metadata_params: dict[str, object] = {}

    staging_sql = _render_table_name(engine, staging_schema, staging_table)
    fact_sql = _render_table_name(engine, fact_schema, fact_table)
    runtime_values = dict(runtime_values or {})
    metadata_columns = list(metadata_columns or [])

    archive_filename: str | None = None
    if archive_base_dir and any(
        column["source"]["kind"] == "archive_filename" for column in metadata_columns
    ):
        batch_id_value = runtime_values.get("batch_id")
        if not isinstance(batch_id_value, str) or not batch_id_value:
            raise ValueError(
                "runtime_values['batch_id'] must be provided when using archive_filename metadata."
            )
        _, archive_filename = _archive_source_table_snapshot(
            engine,
            schema=staging_schema,
            table=staging_table,
            batch_id=batch_id_value,
            archive_base_dir=archive_base_dir,
        )

    for index, (staging_column, config) in enumerate(lookup_map.items()):
        if staging_column in missing_columns:
            continue
        source = config["source"]
        target = config["target"]
        target_column = target["column"]
        source_schema = source.get("schema")
        source_table = source["table"]
        source_lookup_column = source["lookup_column"]
        source_value_column = source["value_column"]
        alias = f"lookup_{index}"
        current_clause = ""
        current_column = _get_current_indicator_column(
            engine,
            schema=source_schema,
            table=source_table,
        )
        if current_column:
            current_clause = (
                f" AND ({alias}.{_quote_identifier(engine, current_column)} = 1 "
                f"OR {alias}.{_quote_identifier(engine, current_column)} IS NULL)"
            )

        base_joins.append(
            "LEFT JOIN "
            f"{_render_table_name(engine, source_schema, source_table)} {alias} "
            f"ON {alias}.{_quote_identifier(engine, source_lookup_column)} = "
            f"s.{_quote_identifier(engine, staging_column)}{current_clause}"
        )
        base_selects.append(
            f"{alias}.{_quote_identifier(engine, source_value_column)} "
            f"AS {_quote_identifier(engine, target_column)}"
        )
        column_definitions.append(
            (
                target_column,
                _get_column_type(
                    engine,
                    schema=source_schema,
                    table=source_table,
                    column=source_value_column,
                ),
                False,
            )
        )
        if target.get("comment"):
            column_comments[target_column] = str(target["comment"])

    for index, config in enumerate(metadata_columns):
        target = config["target"]
        source = config["source"]
        target_column = target["column"]
        source_kind = source["kind"]
        metadata_param_name = f"metadata_{index}"

        if source_kind == "parameter":
            runtime_name = source["name"]
            if runtime_name not in runtime_values:
                raise ValueError(
                    f"runtime_values['{runtime_name}'] must be provided for metadata column '{target_column}'."
                )
            metadata_selects.append(
                f":{metadata_param_name} AS {_quote_identifier(engine, target_column)}"
            )
            metadata_params[metadata_param_name] = runtime_values[runtime_name]
            column_type = _default_string_type(engine)
        elif source_kind == "sql":
            metadata_selects.append(
                f"{source['expression']} AS {_quote_identifier(engine, target_column)}"
            )
            column_type = _default_datetime_type(engine)
        elif source_kind == "archive_filename":
            metadata_selects.append(
                f":{metadata_param_name} AS {_quote_identifier(engine, target_column)}"
            )
            metadata_params[metadata_param_name] = archive_filename
            column_type = _default_string_type(engine)
            nullable = archive_base_dir is None
        else:
            raise ValueError(f"Unsupported metadata source kind '{source_kind}'.")

        if source_kind != "archive_filename":
            nullable = False

        column_definitions.append((target_column, column_type, nullable))
        if target.get("comment"):
            column_comments[target_column] = str(target["comment"])

    for column in data_columns:
        column_name = column["column"]
        if column_name not in staging_columns:
            continue
        base_selects.append(
            f"s.{_quote_identifier(engine, column_name)} "
            f"AS {_quote_identifier(engine, column_name)}"
        )
        column_definitions.append(
            (
                column_name,
                _get_column_type(
                    engine,
                    schema=staging_schema,
                    table=staging_table,
                    column=column_name,
                ),
                True,
            )
        )
        if column.get("comment"):
            column_comments[column_name] = column["comment"]

    final_selects = [
        f"b.{_quote_identifier(engine, name)} AS {_quote_identifier(engine, name)}"
        for name, _, _ in column_definitions
    ]
    additional_joins: list[str] = []

    for index, config in enumerate(additional_columns):
        source = config["source"]
        target = config["target"]
        surrogate_keys = config["surrogate_keys"]
        target_column = target["column"]
        source_schema = source.get("schema")
        source_table = source["table"]
        source_value_column = source["column"]
        fact_key = surrogate_keys["fact"]
        dim_key = surrogate_keys["dim"]
        alias = f"additional_{index}"
        current_clause = ""
        current_column = _get_current_indicator_column(
            engine,
            schema=source_schema,
            table=source_table,
        )
        if current_column:
            current_clause = (
                f" AND ({alias}.{_quote_identifier(engine, current_column)} = 1 "
                f"OR {alias}.{_quote_identifier(engine, current_column)} IS NULL)"
            )

        additional_joins.append(
            "LEFT JOIN "
            f"{_render_table_name(engine, source_schema, source_table)} {alias} "
            f"ON {alias}.{_quote_identifier(engine, dim_key)} = "
            f"b.{_quote_identifier(engine, fact_key)}{current_clause}"
        )
        final_selects.append(
            f"{alias}.{_quote_identifier(engine, source_value_column)} "
            f"AS {_quote_identifier(engine, target_column)}"
        )
        column_definitions.append(
            (
                target_column,
                _get_column_type(
                    engine,
                    schema=source_schema,
                    table=source_table,
                    column=source_value_column,
                ),
                True,
            )
        )
        if target.get("comment"):
            column_comments[target_column] = str(target["comment"])

    create_columns = [
        _build_column_definition(
            _quote_identifier(engine, name),
            column_type,
            nullable=nullable,
        )
        for name, column_type, nullable in column_definitions
    ]
    if fact_pk_column:
        create_columns.insert(0, _fact_pk_clause(engine, fact_pk_column))

    create_columns_sql = ", ".join(
        create_columns
    )
    insert_columns_sql = ", ".join(
        _quote_identifier(engine, name) for name, _, _ in column_definitions
    )
    select_parts = [base_select for base_select in base_selects]
    select_parts.extend(metadata_selects)
    base_select_sql = ", ".join(select_parts)
    final_select_sql = ", ".join(final_selects)
    join_sql = " ".join(base_joins)
    additional_join_sql = " ".join(additional_joins)

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {fact_sql}"))
        conn.execute(text(f"CREATE TABLE {fact_sql} ({create_columns_sql})"))
        conn.execute(
            text(
                f"""
                WITH base_rows AS (
                    SELECT {base_select_sql}
                    FROM {staging_sql} s
                    {join_sql}
                )
                INSERT INTO {fact_sql} ({insert_columns_sql})
                SELECT {final_select_sql}
                FROM base_rows b
                {additional_join_sql}
                """
            ),
            metadata_params,
        )

    _apply_comments(
        engine,
        schema=fact_schema,
        table=fact_table,
        table_comment=table_comment,
        column_comments=column_comments,
    )
