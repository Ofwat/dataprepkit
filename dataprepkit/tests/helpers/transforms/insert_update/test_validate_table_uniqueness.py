import pytest
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy.exc import OperationalError
from dataprepkit.helpers.transforms.insert_update import validate_table_uniqueness

@pytest.fixture
def engine():
    engine = create_engine("sqlite:///:memory:")
    yield engine
    engine.dispose()

@pytest.fixture
def sample_table(engine):
    metadata = MetaData()
    table = Table(
        "sample_table", metadata,
        Column("id", Integer),
        Column("code", String),
        Column("category", String)
    )
    metadata.create_all(engine)
    return table

def test_no_duplicates(engine, sample_table):
    # Insert unique rows
    with engine.begin() as conn:
        conn.execute(sample_table.insert(), [
            {"id": 1, "code": "A", "category": "X"},
            {"id": 2, "code": "B", "category": "Y"},
            {"id": 3, "code": "C", "category": "X"},
        ])
    qualified_table = "[sample_table]"
    # Should not raise
    validate_table_uniqueness(engine, qualified_table, ["code"])

def test_duplicates_single_key(engine, sample_table):
    with engine.begin() as conn:
        conn.execute(sample_table.insert(), [
            {"id": 1, "code": "A", "category": "X"},
            {"id": 2, "code": "A", "category": "Y"},
        ])
    qualified_table = "[sample_table]"
    with pytest.raises(ValueError, match="Duplicate business keys found"):
        validate_table_uniqueness(engine, qualified_table, ["code"])

def test_duplicates_multiple_keys(engine, sample_table):
    with engine.begin() as conn:
        conn.execute(sample_table.insert(), [
            {"id": 1, "code": "A", "category": "X"},
            {"id": 2, "code": "A", "category": "X"},
            {"id": 3, "code": "B", "category": "Y"},
        ])
    qualified_table = "[sample_table]"
    # Duplicate on composite key (code, category)
    with pytest.raises(ValueError, match="Duplicate business keys found"):
        validate_table_uniqueness(engine, qualified_table, ["code", "category"])

def test_empty_table_no_duplicates(engine, sample_table):
    qualified_table = "[sample_table]"
    # No rows inserted, should pass
    validate_table_uniqueness(engine, qualified_table, ["code"])
