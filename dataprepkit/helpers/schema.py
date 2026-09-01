from sqlalchemy import Engine, text


def _quote_identifier(identifier: str) -> str:
    if not identifier or not isinstance(identifier, str):
        raise ValueError("SQL identifiers must be non-empty strings")
    return f"[{identifier.replace(']', ']]')}]"


def ensure_schema_exists(engine: Engine, schema: str | None) -> None:
    """Create schema in MSSQL if it does not already exist."""
    if not schema:
        return
    if engine.dialect.name != "mssql":
        return
    schema_safe = schema.replace("]", "]]")
    create_sql = text(
        f"""
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = :schema)
            EXEC('CREATE SCHEMA [{schema_safe}]')
        """
    )
    with engine.begin() as conn:
        conn.execute(create_sql, {"schema": schema})


def create_validation_run_summary_view(
    engine: Engine,
    *,
    source_schema: str = "hello",
    source_table: str = "validation_event",
    view_schema: str = "hello",
    view_name: str = "validation_run_summary",
    lookback_months: int = 1,
) -> None:
    """Create or replace a validation-run summary view in MSSQL."""
    if engine.dialect.name != "mssql":
        raise ValueError(
            "create_validation_run_summary_view requires an MSSQL engine"
        )
    if lookback_months < 1:
        raise ValueError("lookback_months must be at least 1")

    source = f"{_quote_identifier(source_schema)}.{_quote_identifier(source_table)}"
    view = f"{_quote_identifier(view_schema)}.{_quote_identifier(view_name)}"
    create_sql = text(
        f"""
        CREATE OR ALTER VIEW {view}
        AS
        WITH filtered_events AS (
            SELECT *
            FROM {source}
            WHERE insert_date >= DATEADD(
                month,
                -{lookback_months},
                SYSUTCDATETIME()
            )
        ),
        run_summary AS (
            SELECT
                run_id,
                organisation_cd,
                MAX(process_cd) AS process_cd,
                MAX(candidate_filename) AS candidate_filename,
                MAX(profile_name) AS profile_name,
                MAX(config_version) AS config_version,
                MAX(submission_period_cd) AS submission_period_cd,
                MAX(status) AS status,
                MAX(process_stage_cd) AS process_stage_cd,
                MIN(pipeline_start_date) AS pipeline_start_date,
                MIN(insert_date) AS insert_date,
                MAX(user_id) AS user_id,
                MAX(user_name) AS user_name,
                CAST(MIN(CAST(is_valid AS INT)) AS BIT) AS is_valid,
                CAST(MIN(CAST(complete AS INT)) AS BIT) AS complete,
                COALESCE(SUM(processed_count), 0) AS processed_count
            FROM filtered_events
            WHERE event_type = 'summary'
            GROUP BY run_id, organisation_cd
        ),
        run_issues AS (
            SELECT
                run_id,
                organisation_cd,
                SUM(CASE WHEN event_type = 'error' THEN 1 ELSE 0 END)
                    AS error_count,
                SUM(CASE WHEN event_type = 'warning' THEN 1 ELSE 0 END)
                    AS warning_count
            FROM filtered_events
            WHERE event_type IN ('error', 'warning')
            GROUP BY run_id, organisation_cd
        )
        SELECT
            s.*,
            COALESCE(i.error_count, 0) AS error_count,
            COALESCE(i.warning_count, 0) AS warning_count
        FROM run_summary AS s
        LEFT JOIN run_issues AS i
            ON i.run_id = s.run_id
           AND i.organisation_cd = s.organisation_cd
        """
    )
    ensure_schema_exists(engine, view_schema)
    with engine.begin() as conn:
        conn.execute(create_sql)
