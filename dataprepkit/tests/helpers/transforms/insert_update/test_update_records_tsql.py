import pytest
from unittest.mock import MagicMock
from dataprepkit.helpers.transforms.insert_update import update_records_tsql

def _get_executed_sql(mock_conn):
    # Helper to extract the SQL string from a mock call
    return mock_conn.execute.call_args[0][0].text


def test_update_records_basic():
    engine = MagicMock()
    conn = MagicMock()
    result = MagicMock(rowcount=3)

    engine.begin.return_value.__enter__.return_value = conn
    conn.execute.return_value = result

    rowcount = update_records_tsql(
        engine=engine,
        target_table="target",
        source_table="source",
        join_keys="id",
        surrogate_key="id",
        columns_to_update=["value"]
    )

    sql = _get_executed_sql(conn)
    assert "UPDATE tgt" in sql
    assert "FROM [target] tgt" in sql
    assert "INNER JOIN [source] src" in sql
    assert "tgt.[value] = src.[value]" in sql
    assert "tgt.[id] = src.[id]" in sql
    assert "WHERE tgt.[id] IS NOT NULL" in sql
    assert rowcount == 3


def test_update_multiple_join_keys():
    engine = MagicMock()
    conn = MagicMock()
    result = MagicMock(rowcount=1)

    engine.begin.return_value.__enter__.return_value = conn
    conn.execute.return_value = result

    rowcount = update_records_tsql(
        engine=engine,
        target_table="target",
        source_table="source",
        join_keys=["id", "code"],
        surrogate_key="id",
        columns_to_update=["name"]
    )

    sql = _get_executed_sql(conn)
    assert "tgt.[id] = src.[id]" in sql
    assert "tgt.[code] = src.[code]" in sql
    assert "tgt.[name] = src.[name]" in sql
    assert rowcount == 1


def test_update_with_schema_qualified_names():
    engine = MagicMock()
    conn = MagicMock()
    result = MagicMock(rowcount=2)

    engine.begin.return_value.__enter__.return_value = conn
    conn.execute.return_value = result

    rowcount = update_records_tsql(
        engine=engine,
        target_table="[dbo].[target_table]",
        source_table="[dbo].[source_table]",
        join_keys="Assurance_Cd",
        surrogate_key="Assurance_Id",
        columns_to_update=["Name", "Status"]
    )

    sql = _get_executed_sql(conn)
    assert "FROM [dbo].[target_table] tgt" in sql
    assert "INNER JOIN [dbo].[source_table] src" in sql
    assert "tgt.[Name] = src.[Name]" in sql
    assert "tgt.[Status] = src.[Status]" in sql
    assert rowcount == 2


def test_update_raises_if_columns_empty():
    engine = MagicMock()

    with pytest.raises(ValueError, match="`columns_to_update` must be a non-empty list"):
        update_records_tsql(
            engine=engine,
            target_table="target",
            source_table="source",
            join_keys="id",
            surrogate_key="id",
            columns_to_update=[]
        )


def test_update_with_dot_notation_tables():
    """Ensure dot notation without brackets is handled (e.g., dbo.table)"""
    engine = MagicMock()
    conn = MagicMock()
    result = MagicMock(rowcount=4)

    engine.begin.return_value.__enter__.return_value = conn
    conn.execute.return_value = result

    rowcount = update_records_tsql(
        engine=engine,
        target_table="[dbo].[target]",
        source_table="[dbo].[source]",
        join_keys="user_id",
        surrogate_key="user_id",
        columns_to_update=["email"]
    )

    sql = _get_executed_sql(conn)
    assert "FROM [dbo].[target] tgt" in sql
    assert "INNER JOIN [dbo].[source] src" in sql
    assert "tgt.[email] = src.[email]" in sql
    assert "tgt.[user_id] = src.[user_id]" in sql
    assert rowcount == 4
