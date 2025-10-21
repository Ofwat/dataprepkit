import pytest
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from dataprepkit.helpers.transforms.insert_update import validate_table_no_nulls

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

def test_no_nulls(engine, sample_table):
    with engine.begin() as conn:
        conn.execute(sample_table.insert(), [
            {"id": 1, "code": "A", "category": "X"},
            {"id": 2, "code": "B", "category": "Y"},
        ])
    qualified_table = "[sample_table]"
    # Should not raise
    validate_table_no_nulls(engine, qualified_table, ["code"])

def test_null_in_single_key(engine, sample_table):
    with engine.begin() as conn:
        conn.execute(sample_table.insert(), [
            {"id": 1, "code": None, "category": "X"},
            {"id": 2, "code": "B", "category": "Y"},
        ])
    qualified_table = "[sample_table]"
    with pytest.raises(ValueError, match="Found 1 rows with NULL values"):
        validate_table_no_nulls(engine, qualified_table, ["code"])

def test_null_in_multiple_keys(engine, sample_table):
    with engine.begin() as conn:
        conn.execute(sample_table.insert(), [
            {"id": 1, "code": None, "category": None},
            {"id": 2, "code": "B", "category": "Y"},
        ])
    qualified_table = "[sample_table]"
    with pytest.raises(ValueError, match="Found 1 rows with NULL values"):
        validate_table_no_nulls(engine, qualified_table, ["code", "category"])

def test_empty_table_no_nulls(engine, sample_table):
    qualified_table = "[sample_table]"
    # No rows inserted, should pass
    validate_table_no_nulls(engine, qualified_table, ["code"])
