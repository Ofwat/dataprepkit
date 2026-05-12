from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
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


def _unique_index_name(table: str, column: str) -> str:
    return f"ux_{table}_{column}"


def _case_insensitive_column_lookup(columns: Iterable[str]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for column in columns:
        key = column.casefold()
        existing = lookup.get(key)
        if existing is not None and existing != column:
            raise RuntimeError(
                "Column names must be unique ignoring case; "
                f"found both '{existing}' and '{column}'."
            )
        lookup[key] = column
    return lookup


def _default_string_type(engine: Engine) -> str:
    if engine.dialect.name == "mssql":
        return "NVARCHAR(4000)"
    return "TEXT"


def _default_datetime_type(engine: Engine) -> str:
    if engine.dialect.name == "mssql":
        return "DATETIME2(3)"
    return "TEXT"


def _is_text_like_type(type_name: str | None) -> bool:
    if not type_name:
        return False
    normalized = type_name.strip().upper()
    return any(token in normalized for token in ("CHAR", "TEXT", "CLOB"))


def _case_sensitive_match_expression(
    engine: Engine,
    expression: str,
    column_type: str | None,
) -> str:
    if not _is_text_like_type(column_type):
        return expression
    if engine.dialect.name == "sqlite":
        return f"{expression} COLLATE BINARY"
    if engine.dialect.name == "mssql":
        return f"{expression} COLLATE Latin1_General_100_BIN2"
    return expression


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
        if candidate["name"].casefold() == column.casefold():
            return _compile_column_type(engine, candidate["type"])
    location = f"{schema}.{table}" if schema else table
    raise ValueError(f"Column '{column}' not found in '{location}'.")


def _resolve_data_column_type(
    engine: Engine,
    *,
    schema: str | None,
    table: str,
    column: Mapping[str, object],
) -> str:
    configured_type = column.get("type")
    if configured_type:
        return str(configured_type)
    return _get_column_type(
        engine,
        schema=schema,
        table=table,
        column=str(column["column"]),
    )


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


def _table_exists(
    engine: Engine,
    *,
    schema: str | None,
    table: str,
) -> bool:
    return inspect(engine).has_table(table, schema=schema)


def _index_exists(
    engine: Engine,
    *,
    schema: str | None,
    table: str,
    index_name: str,
) -> bool:
    return any(
        candidate["name"].casefold() == index_name.casefold()
        for candidate in inspect(engine).get_indexes(table, schema=schema)
    )


def _validate_staging_columns(
    engine: Engine,
    *,
    schema: str | None,
    table: str,
    lookup_map: Mapping[str, Mapping[str, object]],
    data_columns: Sequence[Mapping[str, object]],
    expected_lookup_columns: Sequence[str] | None = None,
) -> tuple[set[str], list[dict[str, object]]]:
    staging_columns = _get_table_columns(engine, schema=schema, table=table)
    lookup_columns = set(lookup_map)
    data_column_names = {column["column"] for column in data_columns}
    active_lookup_columns = list(expected_lookup_columns or lookup_map.keys())
    missing_lookup_configs = [
        column for column in active_lookup_columns if column not in lookup_map
    ]
    if missing_lookup_configs:
        raise ValueError(
            "Missing lookup definitions for expected columns: "
            f"{', '.join(sorted(missing_lookup_configs))}"
        )

    location = f"{schema}.{table}" if schema else table
    active_lookups: list[dict[str, object]] = []
    required_missing_columns: list[str] = []

    for column_name in active_lookup_columns:
        config = lookup_map[column_name]
        fallbacks = config.get("fallbacks") or {}
        fallback_value = fallbacks.get("column_missing_in_staging")
        if column_name in staging_columns:
            active_lookups.append(
                {
                    "staging_column": column_name,
                    "config": config,
                    "lookup_sql": f"s.{_quote_identifier(engine, column_name)}",
                    "lookup_params": {},
                    "lookup_value": None,
                }
            )
            continue
        if fallback_value is not None:
            param_name = f"lookup_fallback_{len(active_lookups)}"
            active_lookups.append(
                {
                    "staging_column": column_name,
                    "config": config,
                    "lookup_sql": f":{param_name}",
                    "lookup_params": {param_name: fallback_value},
                    "lookup_value": fallback_value,
                }
            )
            continue
        required_missing_columns.append(column_name)

    if required_missing_columns and expected_lookup_columns is not None:
        raise ValueError(
            f"Missing required lookup staging columns in '{location}': "
            f"{', '.join(sorted(required_missing_columns))}"
        )

    if expected_lookup_columns is None:
        missing_lookup_columns = sorted(lookup_columns - staging_columns)
        if missing_lookup_columns:
            print(
                "Warning: missing lookup staging columns in "
                f"'{location}': {', '.join(missing_lookup_columns)}"
            )

    missing_data_columns = sorted(data_column_names - staging_columns)
    if missing_data_columns:
        raise ValueError(
            f"Missing required data columns in '{location}': {', '.join(missing_data_columns)}"
        )

    used_columns = set(active_lookup_columns) | data_column_names
    extra_columns = sorted(staging_columns - used_columns)
    if extra_columns:
        print(
            f"Warning: unused staging columns in '{location}': {', '.join(extra_columns)}"
        )
    return staging_columns, active_lookups


def _get_batch_metadata_column(
    metadata_columns: Sequence[Mapping[str, object]],
) -> str | None:
    for config in metadata_columns:
        source = config["source"]
        if source["kind"] == "parameter" and source["name"] == "batch_id":
            return config["target"]["column"]
    return None


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


def _format_missing_lookup_error(
    engine: Engine,
    *,
    staging_schema: str | None,
    staging_table: str,
    staging_column: str,
    dim_schema: str | None,
    dim_table: str,
    dim_lookup_column: str,
    required_column: str,
    lookup_sql: str | None = None,
    lookup_params: Mapping[str, object] | None = None,
    lookup_value: object | None = None,
) -> tuple[int, str]:
    staging_sql = _render_table_name(engine, staging_schema, staging_table)
    dim_sql = _render_table_name(engine, dim_schema, dim_table)
    dim_alias = "d"
    current_clause = ""
    current_column = _get_current_indicator_column(
        engine,
        schema=dim_schema,
        table=dim_table,
    )
    if current_column:
        current_clause = (
            f" AND ({dim_alias}.{_quote_identifier(engine, current_column)} = 1 "
            f"OR {dim_alias}.{_quote_identifier(engine, current_column)} IS NULL)"
        )

    effective_lookup_sql = (
        lookup_sql or f"s.{_quote_identifier(engine, staging_column)}"
    )
    dim_lookup_type = _get_column_type(
        engine,
        schema=dim_schema,
        table=dim_table,
        column=dim_lookup_column,
    )
    staging_lookup_type = (
        dim_lookup_type
        if lookup_value is not None
        else _get_column_type(
            engine,
            schema=staging_schema,
            table=staging_table,
            column=staging_column,
        )
    )
    dim_lookup_sql = _case_sensitive_match_expression(
        engine,
        f"{dim_alias}.{_quote_identifier(engine, dim_lookup_column)}",
        dim_lookup_type,
    )
    effective_lookup_sql = _case_sensitive_match_expression(
        engine,
        effective_lookup_sql,
        staging_lookup_type,
    )
    predicate = (
        f"{dim_lookup_sql} = {effective_lookup_sql}{current_clause}"
    )
    where_clause = f"{dim_alias}.{_quote_identifier(engine, required_column)} IS NULL"
    sample_select = (
        f"s.{_quote_identifier(engine, staging_column)}"
        if lookup_value is None
        else f"{effective_lookup_sql}"
    )
    count_query = text(
        f"""
        SELECT COUNT(1)
        FROM {staging_sql} s
        LEFT JOIN {dim_sql} {dim_alias}
          ON {predicate}
        WHERE {where_clause}
        """
    )
    sample_query = text(
        f"""
        SELECT DISTINCT {sample_select} AS staging_key
        FROM {staging_sql} s
        LEFT JOIN {dim_sql} {dim_alias}
          ON {predicate}
        WHERE {where_clause}
        """
    )
    with engine.connect() as conn:
        count = conn.execute(count_query, dict(lookup_params or {})).scalar() or 0
        rows = conn.execute(sample_query, dict(lookup_params or {})).mappings().fetchmany(10)

    missing_keys = [{staging_column: row["staging_key"]} for row in rows]
    location = f"{dim_schema}.{dim_table}" if dim_schema else dim_table
    source_label = "lookup columns" if lookup_value is not None else "staging columns"
    return count, (
        f"Missing dimension match in {location} for {source_label} "
        f"{[staging_column]} -> dimension columns {[dim_lookup_column]}. "
        f"Required columns {[required_column]} were null for {count} row(s). "
        f"Example missing source keys: {missing_keys}"
    )


def _batch_already_loaded(
    bind,
    engine: Engine,
    *,
    schema: str | None,
    table: str,
    batch_column: str,
    batch_id: object,
) -> bool:
    count = bind.execute(
        text(
            f"""
            SELECT COUNT(1)
            FROM {_render_table_name(engine, schema, table)}
            WHERE {_quote_identifier(engine, batch_column)} = :batch_id
            """
        ),
        {"batch_id": batch_id},
    ).scalar()
    return (count or 0) > 0


def _null_safe_row_match_predicate(
    engine: Engine,
    *,
    left_alias: str,
    right_alias: str,
    columns: Sequence[str],
) -> str:
    predicates = []
    for column in columns:
        quoted = _quote_identifier(engine, column)
        left_expr = f"{left_alias}.{quoted}"
        right_expr = f"{right_alias}.{quoted}"
        predicates.append(
            f"(({left_expr} = {right_expr}) OR ({left_expr} IS NULL AND {right_expr} IS NULL))"
        )
    return " AND ".join(predicates) if predicates else "1 = 1"


def _alter_table_add_column(
    conn,
    engine: Engine,
    *,
    schema: str | None,
    table: str,
    column_name: str,
    column_type: str,
    nullable: bool,
) -> None:
    add_keyword = "ADD" if engine.dialect.name == "mssql" else "ADD COLUMN"
    conn.execute(
        text(
            f"""
            ALTER TABLE {_render_table_name(engine, schema, table)}
            {add_keyword} {_build_column_definition(
                _quote_identifier(engine, column_name),
                column_type,
                nullable=nullable,
            )}
            """
        )
    )


def _alter_column_not_null(
    conn,
    engine: Engine,
    *,
    schema: str | None,
    table: str,
    column_name: str,
    column_type: str,
) -> None:
    if engine.dialect.name != "mssql":
        return
    conn.execute(
        text(
            f"""
            ALTER TABLE {_render_table_name(engine, schema, table)}
            ALTER COLUMN {_quote_identifier(engine, column_name)} {column_type} NOT NULL
            """
        )
    )


def _create_unique_index(
    conn,
    engine: Engine,
    *,
    schema: str | None,
    table: str,
    column: str,
) -> None:
    index_name = _unique_index_name(table, column)
    if _index_exists(engine, schema=schema, table=table, index_name=index_name):
        return

    index_sql = _quote_identifier(engine, index_name)
    table_sql = _render_table_name(engine, schema, table)
    if engine.dialect.name == "sqlite" and schema:
        index_sql = f"{_quote_identifier(engine, schema)}.{index_sql}"
        table_sql = _quote_identifier(engine, table)

    conn.execute(
        text(
            f"""
            CREATE UNIQUE INDEX {index_sql}
            ON {table_sql} ({_quote_identifier(engine, column)})
            """
        )
    )


def _resolve_lookup_value(
    conn,
    engine: Engine,
    *,
    dim_schema: str | None,
    dim_table: str,
    dim_lookup_column: str,
    required_column: str,
    lookup_value: object,
) -> object:
    dim_sql = _render_table_name(engine, dim_schema, dim_table)
    dim_alias = "d"
    current_clause = ""
    current_column = _get_current_indicator_column(
        engine,
        schema=dim_schema,
        table=dim_table,
    )
    if current_column:
        current_clause = (
            f" AND ({dim_alias}.{_quote_identifier(engine, current_column)} = 1 "
            f"OR {dim_alias}.{_quote_identifier(engine, current_column)} IS NULL)"
        )
    lookup_column_type = _get_column_type(
        engine,
        schema=dim_schema,
        table=dim_table,
        column=dim_lookup_column,
    )
    dim_lookup_sql = _case_sensitive_match_expression(
        engine,
        f"{dim_alias}.{_quote_identifier(engine, dim_lookup_column)}",
        lookup_column_type,
    )
    lookup_value_sql = _case_sensitive_match_expression(
        engine,
        ":lookup_value",
        lookup_column_type,
    )
    row = conn.execute(
        text(
            f"""
            SELECT {dim_alias}.{_quote_identifier(engine, required_column)} AS resolved_value
            FROM {dim_sql} {dim_alias}
            WHERE {dim_lookup_sql} = {lookup_value_sql}
            {current_clause}
            """
        ),
        {"lookup_value": lookup_value},
    ).mappings().first()
    if row is None or row["resolved_value"] is None:
        location = f"{dim_schema}.{dim_table}" if dim_schema else dim_table
        raise RuntimeError(
            f"Missing dimension match in {location} for lookup columns "
            f"{[dim_lookup_column]} -> dimension columns {[dim_lookup_column]}. "
            f"Required columns {[required_column]} were null for backfill value(s): "
            f"[{{'{dim_lookup_column}': '{lookup_value}'}}]"
        )
    return row["resolved_value"]


def _validate_lookup_matches(
    engine: Engine,
    *,
    staging_schema: str | None,
    staging_table: str,
    active_lookups: Sequence[Mapping[str, object]],
) -> None:
    for active_lookup in active_lookups:
        staging_column = active_lookup["staging_column"]
        config = active_lookup["config"]
        source = config["source"]
        source_schema = source.get("schema")
        source_table = source["table"]
        source_lookup_column = source["lookup_column"]
        source_value_column = source["value_column"]

        count, message = _format_missing_lookup_error(
            engine,
            staging_schema=staging_schema,
            staging_table=staging_table,
            staging_column=staging_column,
            dim_schema=source_schema,
            dim_table=source_table,
            dim_lookup_column=source_lookup_column,
            required_column=source_value_column,
            lookup_sql=active_lookup["lookup_sql"],
            lookup_params=active_lookup["lookup_params"],
            lookup_value=active_lookup["lookup_value"],
        )
        if count:
            raise RuntimeError(message)


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
    data_columns: Sequence[Mapping[str, object]],
    additional_columns: Sequence[Mapping[str, object]],
    metadata_columns: Sequence[Mapping[str, object]] | None = None,
    runtime_values: Mapping[str, object] | None = None,
    expected_lookup_columns: Sequence[str] | None = None,
    staging_table: str,
    staging_schema: str | None,
    fact_table: str,
    fact_schema: str | None = None,
    fact_pk_column: str | None = None,
    archive_base_dir: str | None = None,
    table_comment: str | None = None,
    mode: str = "replace",
) -> None:
    if mode not in {"replace", "append"}:
        raise ValueError("mode must be either 'replace' or 'append'.")

    ensure_schema_exists(engine, fact_schema)
    staging_columns, missing_columns = _validate_staging_columns(
        engine,
        schema=staging_schema,
        table=staging_table,
        lookup_map=lookup_map,
        data_columns=data_columns,
        expected_lookup_columns=expected_lookup_columns,
    )
    _validate_lookup_matches(
        engine,
        staging_schema=staging_schema,
        staging_table=staging_table,
        active_lookups=missing_columns,
    )

    column_definitions: list[tuple[str, str, bool]] = []
    column_comments: dict[str, str] = {}
    data_column_nullability: dict[str, bool] = {}
    data_column_backfills: dict[str, object] = {}
    data_column_types: dict[str, str] = {}
    unique_data_columns: list[str] = []
    base_selects: list[str] = []
    base_joins: list[str] = []
    metadata_selects: list[str] = []
    metadata_params: dict[str, object] = {}
    duplicate_check_columns: list[str] = []

    staging_sql = _render_table_name(engine, staging_schema, staging_table)
    fact_sql = _render_table_name(engine, fact_schema, fact_table)
    runtime_values = dict(runtime_values or {})
    metadata_columns = list(metadata_columns or [])
    batch_metadata_column = _get_batch_metadata_column(metadata_columns)
    fact_exists = _table_exists(engine, schema=fact_schema, table=fact_table)

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

    active_lookups = list(missing_columns)
    for index, active_lookup in enumerate(active_lookups):
        staging_column = active_lookup["staging_column"]
        config = active_lookup["config"]
        source = config["source"]
        target = config["target"]
        target_column = target["column"]
        source_schema = source.get("schema")
        source_table = source["table"]
        source_lookup_column = source["lookup_column"]
        source_value_column = source["value_column"]
        alias = f"lookup_{index}"
        source_lookup_type = _get_column_type(
            engine,
            schema=source_schema,
            table=source_table,
            column=source_lookup_column,
        )
        staging_lookup_type = (
            source_lookup_type
            if active_lookup["lookup_value"] is not None
            else _get_column_type(
                engine,
                schema=staging_schema,
                table=staging_table,
                column=staging_column,
            )
        )
        source_lookup_sql = _case_sensitive_match_expression(
            engine,
            f"{alias}.{_quote_identifier(engine, source_lookup_column)}",
            source_lookup_type,
        )
        staging_lookup_sql = _case_sensitive_match_expression(
            engine,
            str(active_lookup["lookup_sql"]),
            staging_lookup_type,
        )
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
            f"ON {source_lookup_sql} = {staging_lookup_sql}{current_clause}"
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
        duplicate_check_columns.append(target_column)
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
        column_name = str(column["column"])
        if column_name not in staging_columns:
            continue
        base_selects.append(
            f"s.{_quote_identifier(engine, column_name)} "
            f"AS {_quote_identifier(engine, column_name)}"
        )
        nullable = bool(column.get("nullable", True))
        column_type = _resolve_data_column_type(
            engine,
            schema=staging_schema,
            table=staging_table,
            column=column,
        )
        column_definitions.append(
            (
                column_name,
                column_type,
                nullable,
            )
        )
        data_column_nullability[column_name.casefold()] = nullable
        data_column_types[column_name.casefold()] = column_type
        if "backfill_existing_rows" in column:
            data_column_backfills[column_name.casefold()] = column[
                "backfill_existing_rows"
            ]
        duplicate_check_columns.append(column_name)
        if column.get("unique", False):
            unique_data_columns.append(column_name)
        if column.get("comment"):
            column_comments[column_name] = str(column["comment"])

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
        duplicate_check_columns.append(target_column)
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
    duplicate_predicate = _null_safe_row_match_predicate(
        engine,
        left_alias="e",
        right_alias="src",
        columns=duplicate_check_columns,
    )
    projected_rows_sql = (
        f"SELECT {final_select_sql} FROM base_rows b {additional_join_sql}"
    )
    final_source_sql = projected_rows_sql
    if mode == "append":
        final_source_sql = (
            f"SELECT * FROM ({projected_rows_sql}) src "
            f"WHERE NOT EXISTS ("
            f"SELECT 1 FROM {fact_sql} e WHERE {duplicate_predicate}"
            f")"
        )

    with engine.begin() as conn:
        if mode == "replace":
            conn.execute(text(f"DROP TABLE IF EXISTS {fact_sql}"))
            fact_exists = False
        if not fact_exists:
            conn.execute(text(f"CREATE TABLE {fact_sql} ({create_columns_sql})"))
        elif mode == "append":
            existing_fact_columns = _get_table_columns(
                engine,
                schema=fact_schema,
                table=fact_table,
            )
            existing_fact_column_lookup = _case_insensitive_column_lookup(
                existing_fact_columns
            )
            added_fact_columns: set[str] = set()
            for column_name, column_type, _ in column_definitions:
                if column_name.casefold() in existing_fact_column_lookup:
                    continue
                column_key = column_name.casefold()
                has_data_backfill = column_key in data_column_backfills
                _alter_table_add_column(
                    conn,
                    engine,
                    schema=fact_schema,
                    table=fact_table,
                    column_name=column_name,
                    column_type=column_type,
                    nullable=(
                        True
                        if has_data_backfill
                        else data_column_nullability.get(column_key, True)
                    ),
                )
                added_fact_columns.add(column_name)
            for column_name in added_fact_columns:
                column_key = column_name.casefold()
                if column_key not in data_column_backfills:
                    continue
                conn.execute(
                    text(
                        f"""
                        UPDATE {fact_sql}
                        SET {_quote_identifier(engine, column_name)} = :backfill_value
                        WHERE {_quote_identifier(engine, column_name)} IS NULL
                        """
                    ),
                    {"backfill_value": data_column_backfills[column_key]},
                )
                if not data_column_nullability.get(column_key, True):
                    _alter_column_not_null(
                        conn,
                        engine,
                        schema=fact_schema,
                        table=fact_table,
                        column_name=column_name,
                        column_type=data_column_types[column_key],
                    )
            for active_lookup in active_lookups:
                target_column = active_lookup["config"]["target"]["column"]
                if target_column not in added_fact_columns:
                    continue
                backfill_value = (
                    active_lookup["config"].get("fallbacks") or {}
                ).get("backfill_existing_rows")
                if backfill_value is None:
                    continue
                source = active_lookup["config"]["source"]
                resolved_value = _resolve_lookup_value(
                    conn,
                    engine,
                    dim_schema=source.get("schema"),
                    dim_table=source["table"],
                    dim_lookup_column=source["lookup_column"],
                    required_column=source["value_column"],
                    lookup_value=backfill_value,
                )
                conn.execute(
                    text(
                        f"""
                        UPDATE {fact_sql}
                        SET {_quote_identifier(engine, target_column)} = :resolved_value
                        WHERE {_quote_identifier(engine, target_column)} IS NULL
                        """
                    ),
                    {"resolved_value": resolved_value},
                )
            if (
                batch_metadata_column
                and "batch_id" in runtime_values
                and _batch_already_loaded(
                    conn,
                    engine,
                    schema=fact_schema,
                    table=fact_table,
                    batch_column=batch_metadata_column,
                    batch_id=runtime_values["batch_id"],
                )
            ):
                return
        for column_name in unique_data_columns:
            _create_unique_index(
                conn,
                engine,
                schema=fact_schema,
                table=fact_table,
                column=column_name,
            )
        conn.execute(
            text(
                f"""
                WITH base_rows AS (
                    SELECT {base_select_sql}
                    FROM {staging_sql} s
                    {join_sql}
                )
                INSERT INTO {fact_sql} ({insert_columns_sql})
                {final_source_sql}
                """
            ),
            {
                **{
                    key: value
                    for active_lookup in active_lookups
                    for key, value in active_lookup["lookup_params"].items()
                },
                **metadata_params,
            },
        )

    _apply_comments(
        engine,
        schema=fact_schema,
        table=fact_table,
        table_comment=table_comment,
        column_comments=column_comments,
    )
