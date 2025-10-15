import pytest
from unittest.mock import patch, MagicMock
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime
from sqlalchemy.exc import SQLAlchemyError

from dataprepkit.helpers.transforms.insert_update import populate_table_from_source, LoadResult
from dataprepkit.helpers.transforms.insert_update import _get_max_surrogate_key


@pytest.fixture
def sqlite_engine():
    # Create an in-memory SQLite engine
    return create_engine("sqlite:///:memory:")

@pytest.fixture
def setup_tables(sqlite_engine):
    metadata = MetaData()

    # Define target table schema
    target = Table(
        "tbl_d_business_unit", metadata,
        Column("Business_Unit_Id", Integer, primary_key=True),
        Column("Business_Unit_Cd", String, unique=True),
        Column("Business_Unit", String),
        Column("Business_Unit_Desc", String),
        Column("Batch_Id", Integer),
        Column("Insert_Date", DateTime),
        Column("Update_Date", DateTime),
    )

    # Define source table schema (staging)
    source = Table(
        "stg_d_business_unit", metadata,
        Column("Business_Unit_Cd", String, unique=True),
        Column("Business_Unit", String),
        Column("Business_Unit_Desc", String),
        Column("Batch_Id", Integer),
        Column("Insert_Date", DateTime),
        Column("Update_Date", DateTime),
    )

    metadata.create_all(sqlite_engine)

    # Insert initial data into target table
    with sqlite_engine.connect() as conn:
        conn.execute(target.insert(), [
            {
                "Business_Unit_Id": 101,
                "Business_Unit_Cd": "BU1",
                "Business_Unit": "Unit 1",
                "Business_Unit_Desc": "Description 1",
                "Batch_Id": 1,
                "Insert_Date": None,
                "Update_Date": None,
            }
        ])

        # Insert some data into source table
        conn.execute(source.insert(), [
            # Existing BU (should update)
            {
                "Business_Unit_Cd": "BU1",
                "Business_Unit": "Unit 1 Updated",
                "Business_Unit_Desc": "Description 1 Updated",
                "Batch_Id": 2,
                "Insert_Date": None,
                "Update_Date": None,
            },
            # New BU (should insert)
            {
                "Business_Unit_Cd": "BU2",
                "Business_Unit": "Unit 2",
                "Business_Unit_Desc": "Description 2",
                "Batch_Id": 2,
                "Insert_Date": None,
                "Update_Date": None,
            }
        ])

    return target, source

# Factory to create an execute_side_effect that captures SQL strings in a dictionary
def make_execute_side_effect(captured_sql, *, raise_on_count_sql=False, insert_rowcount=None):
    def execute_side_effect(conn_self, sql, *args, **kwargs):
        sql_str = str(sql).lower()

        if "update" in sql_str:
            mock_result = MagicMock()
            mock_result.rowcount = 1
            return mock_result

        if "insert into" in sql_str:
            captured_sql["insert_sql"] = str(sql)  # capture insert SQL
            mock_result = MagicMock()
            # Use provided insert_rowcount or default to None to trigger fallback
            mock_result.rowcount = insert_rowcount
            return mock_result

        if "select" in sql_str:
            if raise_on_count_sql and "select count(*) from numbered_rows" in sql_str:
                raise SQLAlchemyError("Simulated fallback count failure")

            mock_result = MagicMock()
            mock_result.fetchall.return_value = [("dummy",)]
            mock_result.scalar.return_value = 1
            mock_result.rowcount = 0
            return mock_result

        return MagicMock(rowcount=0)  # DROP, COMMIT, etc.

    return execute_side_effect


def test_populate_updates_and_inserts(sqlite_engine, setup_tables):
    target_table, source_table = setup_tables

    with patch("dataprepkit.helpers.transforms.insert_update._get_max_surrogate_key", return_value=101):

        captured_sql = {}

        with patch("sqlalchemy.engine.Connection.execute", autospec=True) as mock_execute:
            mock_execute.side_effect = make_execute_side_effect(captured_sql)

            result = populate_table_from_source(
                engine=sqlite_engine,
                target_schema=None,
                target_table_name="tbl_d_business_unit",
                source_schema=None,
                source_table_name="stg_d_business_unit",
                match_keys=["Business_Unit_Cd"],
                surrogate_key_col="Business_Unit_Id",
                update_columns=["Business_Unit", "Business_Unit_Desc", "Batch_Id", "Update_Date"],
                insert_columns=["Business_Unit_Cd", "Business_Unit", "Business_Unit_Desc", "Batch_Id", "Insert_Date"],
                drop_source=True,  # Force `_drop_table_if_exists` to run, triggering non-matching SQL
            )

    assert isinstance(result, LoadResult)
    assert result.rows_updated == 1
    assert result.rows_inserted == 1


def test_drop_source_table(sqlite_engine, setup_tables):
    _target_table, source_table = setup_tables

    with patch("dataprepkit.helpers.transforms.insert_update._get_max_surrogate_key", return_value=101):

        captured_sql = {}

        _result = populate_table_from_source(
            engine=sqlite_engine,
            target_schema=None,
            target_table_name="tbl_d_business_unit",
            source_schema=None,
            source_table_name="stg_d_business_unit",
            match_keys=["Business_Unit_Cd"],
            surrogate_key_col="Business_Unit_Id",
            update_columns=["Business_Unit", "Business_Unit_Desc", "Batch_Id", "Update_Date"],
            insert_columns=["Business_Unit_Cd", "Business_Unit", "Business_Unit_Desc", "Batch_Id", "Insert_Date"],
            drop_source=True
        )

    with pytest.raises(Exception):
        with sqlite_engine.connect() as conn:
            conn.execute(source_table.select()).fetchall()


def test_validate_identifiers_rejects_invalid():
    from dataprepkit.helpers.transforms.insert_update import _validate_identifiers

    with pytest.raises(ValueError):
        _validate_identifiers("valid_schema", "invalid-table-name")

    with pytest.raises(ValueError):
        _validate_identifiers("valid schema", "table")


def test_verify_columns_raises_on_missing():
    from sqlalchemy import Table, MetaData, Column, Integer
    from dataprepkit.helpers.transforms.insert_update import _verify_columns

    metadata = MetaData()
    table = Table("my_table", metadata, Column("col1", Integer), Column("col2", Integer))

    with pytest.raises(ValueError):
        _verify_columns(table, ["col1", "col3"])  # col3 missing


def test_qualify_table_name_branches():
    from dataprepkit.helpers.transforms.insert_update import _qualify_table_name

    # Test when schema is provided
    qualified = _qualify_table_name("my_schema", "my_table")
    assert qualified == "my_schema.my_table"

    # Test when schema is None
    qualified = _qualify_table_name(None, "my_table")
    assert qualified == "my_table"

    # Optional: test when schema is empty string (should behave like None)
    qualified = _qualify_table_name("", "my_table")
    assert qualified == "my_table"


def test_get_max_surrogate_key_none_fallback(sqlite_engine, setup_tables):
    target_table, source_table = setup_tables

    with patch("dataprepkit.helpers.transforms.insert_update._get_max_surrogate_key", return_value=None), \
         patch("sqlalchemy.engine.Connection.execute", autospec=True) as mock_execute:

        captured_sql = {}

        mock_execute.side_effect = make_execute_side_effect(captured_sql)

        result = populate_table_from_source(
            engine=sqlite_engine,
            target_schema=None,
            target_table_name="tbl_d_business_unit",
            source_schema=None,
            source_table_name="stg_d_business_unit",
            match_keys=["Business_Unit_Cd"],
            surrogate_key_col="Business_Unit_Id",
            update_columns=["Business_Unit", "Business_Unit_Desc", "Batch_Id", "Update_Date"],
            insert_columns=["Business_Unit_Cd", "Business_Unit", "Business_Unit_Desc", "Batch_Id", "Insert_Date"],
            drop_source=False,
        )

        assert "100 + rn as business_unit_id" in captured_sql["insert_sql"].lower()
        assert isinstance(result, LoadResult)
        assert result.rows_inserted == 1
        assert result.rows_updated == 1

def test_get_max_surrogate_key_called(sqlite_engine):
    metadata = MetaData()
    target = Table(
        "tbl_d_business_unit", metadata,
        Column("Business_Unit_Id", Integer, primary_key=True),
        Column("Business_Unit_Cd", String),
    )
    metadata.create_all(sqlite_engine)

    with sqlite_engine.connect() as conn:
        result = _get_max_surrogate_key(conn, target, "Business_Unit_Id")
        # Initially no rows, max should be None
        assert result is None

        # Insert a row
        conn.execute(target.insert().values(Business_Unit_Id=123, Business_Unit_Cd="BU1"))

        result2 = _get_max_surrogate_key(conn, target, "Business_Unit_Id")
        assert result2 == 123

def test_populate_table_logs_and_raises_on_sqlalchemy_error(sqlite_engine, setup_tables):
    target_table, source_table = setup_tables

    # Patch the first call to conn.execute to raise SQLAlchemyError
    with patch("sqlalchemy.engine.Connection.execute", side_effect=SQLAlchemyError("Mock execute failure")), \
         patch("dataprepkit.helpers.transforms.insert_update._get_max_surrogate_key", return_value=100), \
         patch("dataprepkit.helpers.transforms.insert_update.logger") as mock_logger, \
         pytest.raises(SQLAlchemyError, match="Mock execute failure"):

        populate_table_from_source(
            engine=sqlite_engine,
            target_schema=None,
            target_table_name="tbl_d_business_unit",
            source_schema=None,
            source_table_name="stg_d_business_unit",
            match_keys=["Business_Unit_Cd"],
            surrogate_key_col="Business_Unit_Id",
            update_columns=["Business_Unit", "Business_Unit_Desc", "Batch_Id", "Update_Date"],
            insert_columns=["Business_Unit_Cd", "Business_Unit", "Business_Unit_Desc", "Batch_Id", "Insert_Date"],
            drop_source=False,
        )

    # Confirm the final exception block was hit
    mock_logger.exception.assert_called_once()
    assert "Database error occurred during populate_table_from_source" in mock_logger.exception.call_args[0][0]

def test_fallback_row_count_logs_warning_and_sets_rows_inserted_to_zero(sqlite_engine, setup_tables):
    _target_table, _source_table = setup_tables

    with patch("dataprepkit.helpers.transforms.insert_update._get_max_surrogate_key", return_value=100), \
         patch("dataprepkit.helpers.transforms.insert_update.logger") as mock_logger, \
         patch("sqlalchemy.engine.Connection.execute", autospec=True) as mock_execute:

        captured_sql = {}
        mock_execute.side_effect = make_execute_side_effect(
            captured_sql, raise_on_count_sql=True  # Force the fallback to fail
        )

        result = populate_table_from_source(
            engine=sqlite_engine,
            target_schema=None,
            target_table_name="tbl_d_business_unit",
            source_schema=None,
            source_table_name="stg_d_business_unit",
            match_keys=["Business_Unit_Cd"],
            surrogate_key_col="Business_Unit_Id",
            update_columns=["Business_Unit", "Business_Unit_Desc", "Batch_Id", "Update_Date"],
            insert_columns=["Business_Unit_Cd", "Business_Unit", "Business_Unit_Desc", "Batch_Id", "Insert_Date"],
            drop_source=False,
        )

    # Asserts
    assert isinstance(result, LoadResult)
    assert result.rows_updated == 1
    assert result.rows_inserted == 0  # Fallback failed, so should be 0

    # Confirm fallback log was hit
    mock_logger.warning.assert_called_once()
    assert "Could not determine row count from fallback count SQL" in mock_logger.warning.call_args[0][0]

def test_insert_rowcount_directly_used(sqlite_engine, setup_tables):
    _target_table, _source_table = setup_tables

    with patch("dataprepkit.helpers.transforms.insert_update._get_max_surrogate_key", return_value=100), \
         patch("sqlalchemy.engine.Connection.execute", autospec=True) as mock_execute:

        captured_sql = {}

        # Pass insert_rowcount=2 to hit the direct rowcount use branch
        mock_execute.side_effect = make_execute_side_effect(captured_sql, insert_rowcount=2)

        result = populate_table_from_source(
            engine=sqlite_engine,
            target_schema=None,
            target_table_name="tbl_d_business_unit",
            source_schema=None,
            source_table_name="stg_d_business_unit",
            match_keys=["Business_Unit_Cd"],
            surrogate_key_col="Business_Unit_Id",
            update_columns=["Business_Unit", "Business_Unit_Desc", "Batch_Id", "Update_Date"],
            insert_columns=["Business_Unit_Cd", "Business_Unit", "Business_Unit_Desc", "Batch_Id", "Insert_Date"],
            drop_source=False,
        )

    assert isinstance(result, LoadResult)
    assert result.rows_updated == 1
    assert result.rows_inserted == 2  # direct from insert_result.rowcount
