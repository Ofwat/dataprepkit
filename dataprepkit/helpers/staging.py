import re
import shutil
from typing import Literal, Sequence
from pathlib import Path
import uuid

import pandas as pd
from pandas.api.types import (
    is_datetime64_any_dtype,
    is_datetime64tz_dtype,
    is_object_dtype,
    is_string_dtype,
)
from sqlalchemy import Engine, inspect, text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.dialects.mssql import DATETIME2, NVARCHAR, VARCHAR
from dataprepkit.fact_loader import (
    HashMismatchError as _HashMismatchError,
    MissingStageFileError as _MissingStageFileError,
    StageFileSpec as _StageFileSpec,
    assert_columns_have_single_distinct_row as _assert_single_distinct_row,
    assert_columns_not_null as _assert_columns_not_null,
    verify_stage_file_hashes as _verify_stage_file_hashes,
)
from dataprepkit.helpers.schema import ensure_schema_exists
from dataprepkit.storage import archive_dataframe_path


HashMismatchError = _HashMismatchError
MissingStageFileError = _MissingStageFileError
StageFileSpec = _StageFileSpec
assert_columns_have_single_distinct_row = _assert_single_distinct_row
assert_columns_not_null = _assert_columns_not_null
verify_stage_file_hashes = _verify_stage_file_hashes

_MSSQL_STAGING_STRING_LENGTH = 4000
_MSSQL_STAGING_STRING_COLLATION = "Latin1_General_100_BIN2"


def _quote_mssql_identifier(identifier: str) -> str:
    normalized = _normalize_bracket_identifier(identifier) or ""
    return f"[{normalized.replace(']', ']]')}]"


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


def _quote_identifier(engine: Engine, identifier: str) -> str:
    if engine.dialect.name == "mssql":
        return _quote_mssql_identifier(identifier)
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def _clean_sql_type(type_name: str) -> str:
    cleaned = str(type_name).replace('"', "")
    return re.sub(r"\s+COLLATE\s+\S+", "", cleaned, flags=re.IGNORECASE)


def _render_reflected_sql_type(
    column_type,
    engine: Engine,
    *,
    precision: int | None = None,
) -> str:
    if isinstance(column_type, str):
        rendered = column_type
    else:
        rendered = column_type.compile(dialect=engine.dialect)
    cleaned = _clean_sql_type(rendered)
    if engine.dialect.name != "mssql" or precision is None:
        return cleaned
    base_type = cleaned.split("(", 1)[0].strip().upper()
    if base_type in {"DATETIME2", "DATETIMEOFFSET", "TIME"}:
        return f"{base_type}({precision})"
    return cleaned


def _get_mssql_column_precisions(
    engine: Engine,
    table_name: str,
    schema: str | None,
) -> dict[str, int]:
    if engine.dialect.name != "mssql":
        return {}
    query = text(
        """
        SELECT
            c.name AS column_name,
            c.scale AS column_scale
        FROM sys.columns c
        JOIN sys.objects o
            ON o.object_id = c.object_id
        WHERE o.name = :table_name
          AND SCHEMA_NAME(o.schema_id) = :schema_name
        """
    )
    with engine.connect() as conn:
        result = conn.execute(
            query,
            {
                "table_name": table_name,
                "schema_name": schema or "dbo",
            },
        )
        return {
            row._mapping["column_name"].lower(): int(row._mapping["column_scale"])
            for row in result
            if row._mapping["column_scale"] is not None
        }


def _get_schema_max_dates(engine: Engine, schema: str) -> pd.DataFrame:
    if engine.dialect.name != "mssql":
        raise ValueError("schema max date sync is only supported for MSSQL engines.")

    meta_sql = text(
        """
        SELECT
            s.name AS schema_name,
            t.name AS table_name
        FROM sys.tables t
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = :schema
        """
    )
    tables = pd.read_sql(meta_sql, engine, params={"schema": schema})

    results = []
    with engine.connect() as conn:
        for _, row in tables.iterrows():
            schema_name = row["schema_name"]
            table_name = row["table_name"]
            full_table = f"{schema_name}.{table_name}"
            col_check = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM sys.columns
                    WHERE object_id = OBJECT_ID(:tbl)
                      AND name IN ('insert_date', 'update_date')
                    """
                ),
                {"tbl": full_table},
            ).scalar()
            if not col_check or col_check < 1:
                continue

            query = text(
                f"""
                SELECT
                    '{schema_name}' AS schema_name,
                    '{table_name}' AS table_name,
                    MAX(insert_date) AS max_insert_date,
                    MAX(update_date) AS max_update_date
                FROM {_render_table_name(engine, schema_name, table_name)}
                """
            )
            df = pd.read_sql(query, conn)
            results.append(df)

    clean_results = []
    for df in results:
        df = df.dropna(axis=1, how="all")
        if df.shape[1] == 0 or df.empty:
            continue
        clean_results.append(df)

    if clean_results:
        return pd.concat(clean_results, ignore_index=True)

    return pd.DataFrame(
        columns=[
            "schema_name",
            "table_name",
            "max_insert_date",
            "max_update_date",
        ]
    )


def _compare_schema_max_dates(
    df_source: pd.DataFrame,
    df_target: pd.DataFrame,
) -> pd.DataFrame:
    key_cols = ["schema_name", "table_name"]
    df_source = df_source.copy()
    df_target = df_target.copy()

    df_source.columns = df_source.columns.str.lower()
    df_target.columns = df_target.columns.str.lower()

    merged = df_source.merge(
        df_target,
        on=key_cols,
        how="outer",
        suffixes=("_source", "_target"),
        indicator=True,
    )

    insert_mismatch = ~(
        (merged["max_insert_date_source"].isna() & merged["max_insert_date_target"].isna())
        | (merged["max_insert_date_source"] == merged["max_insert_date_target"])
    )
    update_mismatch = ~(
        (merged["max_update_date_source"].isna() & merged["max_update_date_target"].isna())
        | (merged["max_update_date_source"] == merged["max_update_date_target"])
    )

    result = merged[
        (merged["_merge"] != "both") | insert_mismatch | update_mismatch
    ].copy()

    def classify(row) -> str:
        if row["_merge"] == "left_only":
            return "missing_in_target"
        if row["_merge"] == "right_only":
            return "missing_in_source"
        return "data_mismatch"

    result["status"] = result.apply(classify, axis=1)

    return result.sort_values(["schema_name", "table_name"])


def _render_table_name(engine: Engine, schema: str | None, table: str) -> str:
    if engine.dialect.name == "mssql":
        table_sql = _quote_mssql_identifier(table)
        if schema:
            return f"{_quote_mssql_identifier(schema)}.{table_sql}"
        return table_sql
    if schema and schema.lower() not in {"main", "temp"}:
        return f"{_quote_identifier(engine, schema)}.{_quote_identifier(engine, table)}"
    return _quote_identifier(engine, table)


def _get_unique_constraints(
    engine: Engine,
    table_name: str,
    schema: str | None,
) -> dict[str, list[str]]:
    if engine.dialect.name == "mssql":
        full_name = (
            f"{_quote_mssql_identifier(schema)}.{_quote_mssql_identifier(table_name)}"
            if schema
            else _quote_mssql_identifier(table_name)
        )
        with engine.connect() as conn:
            unique_rows = conn.execute(
                text(
                    """
                    SELECT
                        i.name AS constraint_name,
                        c.name AS column_name
                    FROM sys.indexes i
                    JOIN sys.index_columns ic
                        ON i.object_id = ic.object_id
                        AND i.index_id = ic.index_id
                    JOIN sys.columns c
                        ON c.object_id = ic.object_id
                        AND c.column_id = ic.column_id
                    WHERE i.is_unique = 1
                      AND i.is_primary_key = 0
                      AND i.object_id = OBJECT_ID(:tbl)
                    ORDER BY i.name, ic.key_ordinal
                    """
                ),
                {"tbl": full_name},
            ).fetchall()
        uniques: dict[str, list[str]] = {}
        for row in unique_rows:
            uniques.setdefault(row.constraint_name, []).append(row.column_name)
        return uniques

    inspector = inspect(engine)
    uniques = {}
    for idx, constraint in enumerate(
        inspector.get_unique_constraints(table_name, schema=schema)
    ):
        columns = constraint.get("column_names") or []
        if not columns:
            continue
        name = constraint.get("name") or f"UQ_{table_name}_{idx}"
        uniques[name] = list(columns)
    return uniques


def _insert_rows(
    engine: Engine,
    table_sql: str,
    columns: list[dict[str, object]],
    rows: list[dict[str, object]],
    *,
    identity_cols: list[str] | None = None,
) -> None:
    if not rows:
        return

    column_list = ", ".join(
        _quote_identifier(engine, str(column["name"])) for column in columns
    )
    bind_list = ", ".join(f":{column['name']}" for column in columns)
    insert_sql = text(
        f"INSERT INTO {table_sql} ({column_list}) VALUES ({bind_list})"
    )

    with engine.begin() as conn:
        identity_enabled = False
        if identity_cols and engine.dialect.name == "mssql":
            conn.execute(text(f"SET IDENTITY_INSERT {table_sql} ON"))
            identity_enabled = True
        try:
            conn.execute(insert_sql, rows)
        finally:
            if identity_enabled:
                conn.execute(text(f"SET IDENTITY_INSERT {table_sql} OFF"))


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
                else str(int(value))
                if isinstance(value, float) and value.is_integer()
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
    fabric_warehouse_types: bool = False,
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
    fabric_warehouse_types
        Use Fabric Warehouse-compatible VARCHAR columns without a collation.
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
        dtype_overrides = {}
        for col, pandas_dtype in df.dtypes.items():
            if is_datetime64_any_dtype(pandas_dtype) or is_datetime64tz_dtype(
                pandas_dtype
            ):
                dtype_overrides[col] = DATETIME2(precision=3)
            elif is_object_dtype(pandas_dtype) or is_string_dtype(pandas_dtype):
                if fabric_warehouse_types:
                    dtype_overrides[col] = VARCHAR(_MSSQL_STAGING_STRING_LENGTH)
                else:
                    dtype_overrides[col] = NVARCHAR(
                        _MSSQL_STAGING_STRING_LENGTH,
                        collation=_MSSQL_STAGING_STRING_COLLATION,
                    )

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
            try:
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
                openrowset_options = (
                    options_sql
                    if fabric_warehouse_types
                    else f", FORMAT = 'PARQUET'{options_sql}"
                )

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
                        BULK '{escaped_source}'{openrowset_options}
                    ) AS src
                    """
                    )
                    conn.execute(openrowset_sql)
            finally:
                shutil.rmtree(part_dir, ignore_errors=True)
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


def clone_table(
    source_engine: Engine,
    target_engine: Engine,
    schema_name: str,
    table_name: str,
    *,
    staging_use_openrowset_parquet: bool = False,
    staging_parquet_base_dir: str | None = None,
    staging_copy_source_base_url: str | None = None,
    staging_copy_into_options: str = "",
    staging_openrowset_max_rows_per_file: int = 1_000_000,
) -> None:
    """
    Clone a table schema and data from one engine into another.
    """
    if not schema_name:
        raise ValueError("schema_name must be provided")
    if not table_name:
        raise ValueError("table_name must be provided")

    source_inspector = inspect(source_engine)
    target_inspector = inspect(target_engine)

    columns = source_inspector.get_columns(table_name, schema=schema_name)
    pk = source_inspector.get_pk_constraint(table_name, schema=schema_name)
    identity_cols = [column["name"] for column in columns if column.get("autoincrement")]
    source_precisions = _get_mssql_column_precisions(source_engine, table_name, schema_name)
    uniques = _get_unique_constraints(source_engine, table_name, schema_name)

    column_defs: list[str] = []
    for column in columns:
        name = str(column["name"])
        col_type = _render_reflected_sql_type(
            column["type"],
            target_engine,
            precision=source_precisions.get(name.lower()),
        )
        col_def = f"{_quote_identifier(target_engine, name)} {col_type}"
        col_def += " NULL" if column.get("nullable") else " NOT NULL"
        if column.get("autoincrement") and target_engine.dialect.name == "mssql":
            col_def += " IDENTITY(1,1)"
        column_defs.append(col_def)

    constraints: list[str] = []
    if pk and pk.get("constrained_columns"):
        pk_name = pk.get("name") or f"PK_{table_name}"
        pk_cols = ", ".join(
            _quote_identifier(target_engine, column_name)
            for column_name in pk["constrained_columns"]
        )
        constraints.append(
            f"CONSTRAINT {_quote_identifier(target_engine, pk_name)} PRIMARY KEY ({pk_cols})"
        )

    for unique_name, unique_columns in uniques.items():
        unique_cols_sql = ", ".join(
            _quote_identifier(target_engine, column_name) for column_name in unique_columns
        )
        constraints.append(
            f"CONSTRAINT {_quote_identifier(target_engine, unique_name)} "
            f"UNIQUE ({unique_cols_sql})"
        )

    target_table_sql = _render_table_name(target_engine, schema_name, table_name)
    ddl = (
        f"CREATE TABLE {target_table_sql} ("
        f"{', '.join(column_defs + constraints)}"
        f")"
    )

    rows = []
    select_columns = ", ".join(
        _quote_identifier(source_engine, str(column["name"])) for column in columns
    )
    select_sql = text(
        "SELECT "
        f"{select_columns} "
        f"FROM {_render_table_name(source_engine, schema_name, table_name)}"
    )
    with source_engine.connect() as source_conn:
        rows = [dict(row._mapping) for row in source_conn.execute(select_sql)]

    if target_inspector.has_table(table_name, schema=schema_name):
        with target_engine.begin() as target_conn:
            target_conn.execute(text(f"DROP TABLE IF EXISTS {target_table_sql}"))

    with target_engine.begin() as target_conn:
        target_conn.execute(text(ddl))

    if staging_use_openrowset_parquet:
        raw_table_name = f"{table_name}__raw"
        raw_table_sql = _render_table_name(target_engine, schema_name, raw_table_name)
        raw_table_dir = Path(staging_parquet_base_dir) / raw_table_name
        raw_df = pd.DataFrame(
            rows,
            columns=[str(column["name"]) for column in columns],
            dtype=object,
        )
        raw_df = raw_df.astype("string")
        with target_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {raw_table_sql}"))
        try:
            stage_dataframe(
                target_engine,
                raw_table_name,
                raw_df,
                if_exists="replace",
                index=False,
                schema=schema_name,
                use_copy_into_parquet=True,
                parquet_base_dir=staging_parquet_base_dir,
                copy_source_base_url=staging_copy_source_base_url,
                copy_into_options=staging_copy_into_options,
                openrowset_max_rows_per_file=staging_openrowset_max_rows_per_file,
            )

            insert_columns_sql = ", ".join(
                _quote_identifier(target_engine, str(column["name"])) for column in columns
            )
            select_sql = ", ".join(
                f"TRY_CAST(NULLIF(src.{_quote_identifier(target_engine, str(column['name']))}, '') AS {_render_reflected_sql_type(column['type'], target_engine, precision=source_precisions.get(str(column['name']).lower()))})"
                for column in columns
            )
            insert_sql = text(
                f"""
                INSERT INTO {target_table_sql} ({insert_columns_sql})
                SELECT {select_sql}
                FROM {raw_table_sql} src
                """
            )

            with target_engine.begin() as conn:
                if identity_cols and target_engine.dialect.name == "mssql":
                    conn.execute(text(f"SET IDENTITY_INSERT {target_table_sql} ON"))
                try:
                    conn.execute(insert_sql)
                finally:
                    if identity_cols and target_engine.dialect.name == "mssql":
                        conn.execute(text(f"SET IDENTITY_INSERT {target_table_sql} OFF"))
        finally:
            with target_engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {raw_table_sql}"))
            shutil.rmtree(raw_table_dir, ignore_errors=True)
        return

    _insert_rows(
        target_engine,
        target_table_sql,
        columns,
        rows,
        identity_cols=identity_cols,
    )


def sync_mssql_tables(
    source_engine: Engine,
    target_engine: Engine,
    schema_name: str,
    *,
    staging_use_openrowset_parquet: bool = False,
    staging_parquet_base_dir: str | None = None,
    staging_copy_source_base_url: str | None = None,
    staging_copy_into_options: str = "",
    staging_openrowset_max_rows_per_file: int = 1_000_000,
    accepted_statuses: Sequence[str] | None = None,
) -> pd.DataFrame:
    """
    Compare max insert/update dates across two MSSQL schemas and sync accepted tables.
    """
    if not schema_name:
        raise ValueError("schema_name must be provided")

    accepted = {
        status.casefold()
        for status in (
            accepted_statuses
            if accepted_statuses is not None
            else ("missing_in_target", "data_mismatch")
        )
    }
    source_dates = _get_schema_max_dates(source_engine, schema_name)
    target_dates = _get_schema_max_dates(target_engine, schema_name)
    diffs = _compare_schema_max_dates(source_dates, target_dates)

    for row in diffs.itertuples(index=False):
        status = str(row.status).casefold()
        if status not in accepted or status == "missing_in_source":
            continue
        clone_table(
            source_engine,
            target_engine,
            schema_name=row.schema_name,
            table_name=row.table_name,
            staging_use_openrowset_parquet=staging_use_openrowset_parquet,
            staging_parquet_base_dir=staging_parquet_base_dir,
            staging_copy_source_base_url=staging_copy_source_base_url,
            staging_copy_into_options=staging_copy_into_options,
            staging_openrowset_max_rows_per_file=staging_openrowset_max_rows_per_file,
        )

    return diffs


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
