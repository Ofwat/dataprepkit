import logging
from sqlalchemy import Table, MetaData, select, update, text, func
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

class LoadResult:
    def __init__(self, rows_updated=0, rows_inserted=0):
        self.rows_updated = rows_updated
        self.rows_inserted = rows_inserted


def _get_max_surrogate_key(conn, target_table, surrogate_key_col):
    max_key = conn.execute(
        select(func.max(target_table.c[surrogate_key_col]))
    ).scalar()
    return max_key


def _drop_table_if_exists(conn, schema, table_name):
    qualified_name = _qualify_table_name(schema, table_name)
    drop_sql = text(f"DROP TABLE IF EXISTS {qualified_name}")
    conn.execute(drop_sql)


def _validate_identifiers(*identifiers):
    for identifier in identifiers:
        if identifier is None:
            continue  # Allow None (no schema)
        if "-" in identifier or " " in identifier:
            raise ValueError(f"Invalid identifier: {identifier}")


def _verify_columns(table, required_cols):
    missing = set(required_cols) - set(table.columns.keys())
    if missing:
        raise ValueError(f"Missing columns in table '{table.name}': {missing}")


def _build_match_conditions(target_table, source_table, match_keys):
    conditions = []
    for k in match_keys:
        conditions.append(target_table.c[k] == source_table.c[k])
    return conditions


def build_update_statement(target_table, source_table, update_columns, match_conditions, surrogate_key_col):
    return (
        update(target_table)
        .values({col: source_table.c[col] for col in update_columns})
        .where(*match_conditions)
        .where(target_table.c[surrogate_key_col].isnot(None))
        .execution_options(synchronize_session=False)
    )


def _qualify_table_name(schema, table_name):
    if schema:
        return f"{schema}.{table_name}"
    else:
        return table_name


def _build_insert_sql(
    target_schema, target_table_name, source_schema, source_table_name,
    match_keys, surrogate_key_col, insert_columns, max_surrogate_key
):
    insert_cols = [surrogate_key_col] + insert_columns
    insert_cols_str = ", ".join(insert_cols)

    target_qualified = _qualify_table_name(target_schema, target_table_name)
    source_qualified = _qualify_table_name(source_schema, source_table_name)

    join_conditions = " AND ".join(
        [f"src.{k} = tgt.{k}" for k in match_keys]
    )
    missing_match_conditions = " AND ".join(
        [f"tgt.{k} IS NULL" for k in match_keys]
    )

    insert_sql = f"""
        WITH numbered_rows AS (
            SELECT
                src.*,
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
            FROM {source_qualified} src
            LEFT JOIN {target_qualified} tgt
            ON {join_conditions}
            WHERE {missing_match_conditions}
        )
        INSERT INTO {target_qualified} ({insert_cols_str})
        SELECT
            {max_surrogate_key} + rn AS {surrogate_key_col},
            {', '.join([f'nr.{col}' for col in insert_columns])}
        FROM numbered_rows nr
    """
    return insert_sql.strip()


def _build_select_new_rows_sql(
    source_schema, source_table_name,
    target_schema, target_table_name,
    match_keys
):
    target_qualified = _qualify_table_name(target_schema, target_table_name)
    source_qualified = _qualify_table_name(source_schema, source_table_name)

    join_conditions = " AND ".join(
        [f"src.{k} = tgt.{k}" for k in match_keys]
    )
    missing_match_conditions = " AND ".join(
        [f"tgt.{k} IS NULL" for k in match_keys]
    )

    select_sql = f"""
        SELECT
            src.*,
            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
        FROM {source_qualified} src
        LEFT JOIN {target_qualified} tgt
        ON {join_conditions}
        WHERE {missing_match_conditions}
    """
    return select_sql.strip()


def populate_table_from_source(
    engine,
    target_schema,
    target_table_name,
    source_schema,
    source_table_name,
    match_keys,
    surrogate_key_col,
    update_columns,
    insert_columns,
    drop_source=False,
):
    """
    Update and insert rows from source table into target table with surrogate keys.
    Optionally drops the source table.
    """
    _validate_identifiers(target_schema, target_table_name, source_schema, source_table_name)

    metadata = MetaData()
    target_table = Table(target_table_name, metadata, autoload_with=engine, schema=target_schema)
    source_table = Table(source_table_name, metadata, autoload_with=engine, schema=source_schema)

    # Check required columns exist
    _verify_columns(target_table, match_keys + update_columns + [surrogate_key_col])
    _verify_columns(source_table, match_keys + update_columns + insert_columns)

    try:
        with engine.connect() as conn:
            trans = conn.begin()
            try:
                logger.info("Starting load: %s.%s → %s.%s", source_schema, source_table_name, target_schema, target_table_name)

                match_conditions = _build_match_conditions(target_table, source_table, match_keys)

                update_stmt = build_update_statement(target_table, source_table, update_columns, match_conditions, surrogate_key_col)
                update_result = conn.execute(update_stmt)
                rows_updated = update_result.rowcount or 0
                logger.info("Updated %s record(s).", rows_updated)

                max_surrogate_key = _get_max_surrogate_key(conn, target_table, surrogate_key_col)
                if max_surrogate_key is None:
                    max_surrogate_key = 100

                insert_sql = _build_insert_sql(
                    target_schema, target_table_name, source_schema, source_table_name,
                    match_keys, surrogate_key_col, insert_columns, max_surrogate_key
                )

                select_sql = _build_select_new_rows_sql(
                    source_schema, source_table_name,
                    target_schema, target_table_name,
                    match_keys
                )

                # Preview rows to be inserted
                result = conn.execute(text(select_sql)).fetchall()
                logger.info("Rows that would be inserted: %s", len(result))

                insert_result = conn.execute(text(insert_sql))

                if insert_result.rowcount in (-1, 0, None):
                    try:
                        # Use qualified names in fallback count SQL as well
                        target_qualified = _qualify_table_name(target_schema, target_table_name)
                        source_qualified = _qualify_table_name(source_schema, source_table_name)
                        join_conditions = " AND ".join(
                            [f"src.{k} = tgt.{k}" for k in match_keys]
                        )
                        missing_match_conditions = " AND ".join(
                            [f"tgt.{k} IS NULL" for k in match_keys]
                        )
                        count_sql = f"""
                            WITH numbered_rows AS (
                                SELECT
                                    src.*,
                                    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
                                FROM {source_qualified} src
                                LEFT JOIN {target_qualified} tgt
                                ON {join_conditions}
                                WHERE {missing_match_conditions}
                            )
                            SELECT COUNT(*) FROM numbered_rows
                        """
                        rows_inserted = conn.execute(text(count_sql)).scalar()
                    except SQLAlchemyError as e:
                        logger.warning("Could not determine row count from fallback count SQL", exc_info=e)
                        rows_inserted = 0
                else:
                    rows_inserted = insert_result.rowcount or 0

                if drop_source:
                    _drop_table_if_exists(conn, source_schema, source_table_name)
                    logger.info("Dropped source table %s.%s", source_schema, source_table_name)

                trans.commit()
                return LoadResult(rows_updated=rows_updated, rows_inserted=rows_inserted)

            except Exception:
                trans.rollback()
                raise

    except SQLAlchemyError:
        logger.exception("Database error occurred during populate_table_from_source")
        raise
