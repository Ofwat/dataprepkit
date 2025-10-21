import pytest
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, select
from sqlalchemy.exc import ProgrammingError
from dataprepkit.helpers.transforms.insert_update import insert_new_records_dynamic  # adjust import path


@pytest.fixture
def engine():
    engine = create_engine("sqlite:///:memory:")
    yield engine
    engine.dispose()

def _create_table(engine, name, columns, schema=None):
    metadata = MetaData()
    cols = [Column(colname, coltype) for colname, coltype in columns.items()]
    table = Table(name, metadata, *cols, schema=schema)
    metadata.create_all(engine)
    return table

def test_invalid_business_key_type(engine):
    with pytest.raises(ValueError, match="`business_key` must be a string or a list of strings"):
        insert_new_records_dynamic(engine, "source", "target", business_key=123)

def test_source_table_missing(engine):
    _create_table(engine, "target", {"id": Integer, "Assurance_Cd": String})
    with pytest.raises(ValueError, match="Source table"):
        insert_new_records_dynamic(engine, "missing_source", "target")

def test_target_table_missing(engine):
    _create_table(engine, "source", {"id": Integer, "Assurance_Cd": String})
    with pytest.raises(ValueError, match="Target table"):
        insert_new_records_dynamic(engine, "source", "missing_target")

def test_no_common_columns(engine):
    # Both tables share only the surrogate key column 'Assurance_Id'
    _create_table(engine, "source", {"Assurance_Id": Integer, "colA": String})
    _create_table(engine, "target", {"Assurance_Id": Integer, "colB": String})

    # This will exclude 'Assurance_Id' surrogate key, leaving no common columns
    with pytest.raises(ValueError, match="No common columns"):
        insert_new_records_dynamic(engine, "source", "target", surrogate_key="Assurance_Id", business_key="Assurance_Cd")


def test_missing_business_key_column(engine):
    # Both tables share column 'colA', but business key 'Assurance_Cd' is missing
    _create_table(engine, "source", {"id": Integer, "colA": String})
    _create_table(engine, "target", {"id": Integer, "colA": String})

    with pytest.raises(ValueError, match="Business key\\(s\\) missing"):
        insert_new_records_dynamic(engine, "source", "target", business_key="Assurance_Cd")


def test_successful_insert(engine):
    source = _create_table(engine, "source", {
        "Assurance_Id": Integer,
        "Assurance_Cd": String,
        "Name": String
    })
    target = _create_table(engine, "target", {
        "Assurance_Id": Integer,
        "Assurance_Cd": String,
        "Name": String
    })

    # Insert some data in source
    with engine.begin() as conn:
        conn.execute(source.insert(), [
            {"Assurance_Id": 1, "Assurance_Cd": "A1", "Name": "Alpha"},
            {"Assurance_Id": 2, "Assurance_Cd": "B2", "Name": "Beta"},
        ])

    # Insert one row in target with surrogate key to simulate max_id = 2
    with engine.begin() as conn:
        conn.execute(target.insert(), [
            {"Assurance_Id": 2, "Assurance_Cd": "B2", "Name": "Beta"}
        ])

    # Run insert_new_records_dynamic
    insert_new_records_dynamic(
        engine,
        source_table="source",
        target_table="target",
        surrogate_key="Assurance_Id",
        business_key="Assurance_Cd",
        default_start_id=100
    )

    with engine.connect() as conn:
        rows = conn.execute(target.select()).fetchall()
        # Should contain original and new insert (Alpha record)
        assert any(row.Assurance_Cd == "A1" for row in rows)
        assert any(row.Assurance_Cd == "B2" for row in rows)

def test_default_start_id_used_when_no_max_id(engine):
    source = _create_table(engine, "source", {
        "Assurance_Id": Integer,
        "Assurance_Cd": String,
        "Name": String
    })
    target = _create_table(engine, "target", {
        "Assurance_Id": Integer,
        "Assurance_Cd": String,
        "Name": String
    })

    # Insert data into source only
    with engine.begin() as conn:
        conn.execute(source.insert(), [
            {"Assurance_Id": 10, "Assurance_Cd": "C3", "Name": "Gamma"},
        ])

    # Target is empty (no max surrogate key)
    insert_new_records_dynamic(
        engine,
        source_table="source",
        target_table="target",
        surrogate_key="Assurance_Id",
        business_key="Assurance_Cd",
        default_start_id=1000
    )

    with engine.connect() as conn:
        rows = conn.execute(target.select()).fetchall()
        assert any(row.Assurance_Cd == "C3" for row in rows)

def test_multiple_column_business_key_insert(engine):
    source = _create_table(engine, "source", {
        "Assurance_Id": Integer,
        "Assurance_Cd": String,
        "Region": String,
        "Name": String
    })
    target = _create_table(engine, "target", {
        "Assurance_Id": Integer,
        "Assurance_Cd": String,
        "Region": String,
        "Name": String
    })

    # Insert data into source
    with engine.begin() as conn:
        conn.execute(source.insert(), [
            {"Assurance_Id": 1, "Assurance_Cd": "A1", "Region": "East", "Name": "Alpha"},
            {"Assurance_Id": 2, "Assurance_Cd": "B2", "Region": "West", "Name": "Beta"},
        ])

    # Insert one row into target with a matching business key to simulate an existing row
    with engine.begin() as conn:
        conn.execute(target.insert(), [
            {"Assurance_Id": 2, "Assurance_Cd": "B2", "Region": "West", "Name": "Beta"}
        ])

    # Use a composite business key
    insert_new_records_dynamic(
        engine,
        source_table="source",
        target_table="target",
        surrogate_key="Assurance_Id",
        business_key=["Assurance_Cd", "Region"],
        default_start_id=500
    )

    # Fetch all records from target
    with engine.connect() as conn:
        rows = conn.execute(target.select()).fetchall()

    # Should contain original and new insert (Alpha record)
    assurance_codes = {row.Assurance_Cd for row in rows}
    assert "A1" in assurance_codes  # inserted
    assert "B2" in assurance_codes  # already existed
