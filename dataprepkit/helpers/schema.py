from sqlalchemy import Engine, text


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
