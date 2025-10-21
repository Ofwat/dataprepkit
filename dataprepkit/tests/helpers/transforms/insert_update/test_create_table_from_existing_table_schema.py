import pytest
from unittest.mock import patch
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, inspect
from dataprepkit.helpers.transforms.insert_update import create_table_from_existing_table_schema


@pytest.fixture
def engine():
    engine = create_engine("sqlite:///:memory:")
    yield engine
    engine.dispose()


@pytest.fixture
def source_table(engine):
    metadata = MetaData()
    table = Table(
        "source_table", metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
    )
    metadata.create_all(engine)
    return table


def test_create_table_when_target_does_not_exist(engine, source_table):
    result = create_table_from_existing_table_schema(
        engine=engine,
        source_table="source_table",
        target_table="target_table",
        surrogate_key=""
    )
    assert result["created_table"] is True
    assert result["added_surrogate_key"] is False

    inspector = inspect(engine)
    assert inspector.has_table("target_table")


def test_create_table_with_surrogate_key_added(engine):
    metadata = MetaData()
    _source_table_with_sk = Table(
        "source_table_sk", metadata,
        Column("Assurance_Id", Integer),
        Column("id", Integer),
        Column("name", String),
    )
    metadata.create_all(engine)

    result = create_table_from_existing_table_schema(
        engine=engine,
        source_table="source_table_sk",
        target_table="target_table_sk",
        surrogate_key="Assurance_Id"
    )
    assert result["created_table"] is True
    assert result["added_surrogate_key"] is False


def test_create_table_with_surrogate_key_not_in_source(engine, source_table):
    result = create_table_from_existing_table_schema(
        engine=engine,
        source_table="source_table",
        target_table="target_table_sk2",
        surrogate_key="Assurance_Id"
    )
    assert result["created_table"] is True
    assert result["added_surrogate_key"] is True

def test_target_table_exists_no_surrogate_key_addition_if_not_empty(engine, source_table):
    metadata = MetaData()
    target_table = Table(
        "existing_target", metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String)
    )
    metadata.create_all(engine)

    # Use a transaction block that commits automatically
    with engine.begin() as conn:
        conn.execute(target_table.insert().values(id=1, name="test"))

    result = create_table_from_existing_table_schema(
        engine=engine,
        source_table="[source_table]",
        target_table="[existing_target]",
        surrogate_key="Assurance_Id"
    )

    assert result["created_table"] is False
    assert result["added_surrogate_key"] is False

def test_add_surrogate_key_to_existing_empty_target_table(engine, source_table):
    # Create a target table that is empty and missing the surrogate key
    metadata = MetaData()
    target_table = Table(
        "empty_target", metadata,
        Column("id", Integer),
        Column("name", String)
    )
    metadata.create_all(engine)

    # Ensure target table is empty and missing surrogate key
    result = create_table_from_existing_table_schema(
        engine=engine,
        source_table="source_table",
        target_table="empty_target",
        surrogate_key="Assurance_Id"
    )

    assert result["created_table"] is False
    assert result["added_surrogate_key"] is True

    # Verify the surrogate key column was added
    inspector = inspect(engine)
    columns = [col["name"] for col in inspector.get_columns("empty_target")]
    assert "Assurance_Id" in columns

def test_create_table_raises_exception_and_logs_error(engine):
    # Patch the inspect function used in your code to simulate exception
    with patch("dataprepkit.helpers.transforms.insert_update.inspect") as mocked_inspect:
        mocked_inspect.side_effect = Exception("Simulated failure")

        with pytest.raises(Exception, match="Simulated failure"):
            create_table_from_existing_table_schema(
                engine=engine,
                source_table="source_table",
                target_table="target_table",
                surrogate_key="Assurance_Id"
            )
