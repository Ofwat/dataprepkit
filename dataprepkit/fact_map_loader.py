from __future__ import annotations

from collections.abc import Mapping, Sequence

from sqlalchemy import Engine, inspect, text

from dataprepkit.helpers.schema import ensure_schema_exists


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
    staging_table: str,
    staging_schema: str | None,
    fact_table: str,
    fact_schema: str | None = None,
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

    column_definitions: list[tuple[str, str]] = []
    column_comments: dict[str, str] = {}
    base_selects: list[str] = []
    base_joins: list[str] = []

    staging_sql = _render_table_name(engine, staging_schema, staging_table)
    fact_sql = _render_table_name(engine, fact_schema, fact_table)

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

        base_joins.append(
            "LEFT JOIN "
            f"{_render_table_name(engine, source_schema, source_table)} {alias} "
            f"ON {alias}.{_quote_identifier(engine, source_lookup_column)} = "
            f"s.{_quote_identifier(engine, staging_column)}"
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
            )
        )
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
            )
        )
        if column.get("comment"):
            column_comments[column_name] = column["comment"]

    final_selects = [
        f"b.{_quote_identifier(engine, name)} AS {_quote_identifier(engine, name)}"
        for name, _ in column_definitions
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

        additional_joins.append(
            "LEFT JOIN "
            f"{_render_table_name(engine, source_schema, source_table)} {alias} "
            f"ON {alias}.{_quote_identifier(engine, dim_key)} = "
            f"b.{_quote_identifier(engine, fact_key)}"
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
            )
        )
        if target.get("comment"):
            column_comments[target_column] = str(target["comment"])

    create_columns_sql = ", ".join(
        f"{_quote_identifier(engine, name)} {column_type}"
        for name, column_type in column_definitions
    )
    insert_columns_sql = ", ".join(
        _quote_identifier(engine, name) for name, _ in column_definitions
    )
    base_select_sql = ", ".join(base_selects)
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
            )
        )

    _apply_comments(
        engine,
        schema=fact_schema,
        table=fact_table,
        table_comment=table_comment,
        column_comments=column_comments,
    )
