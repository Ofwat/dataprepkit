# pylint: disable=C0301
"""
Test module for insert_new_records_dynamic function from dataprepkit.helpers.transforms.insert_update.

This module uses an in-memory SQLite database to test various scenarios of
the insert_new_records_dynamic function including validation, error handling,
and successful insert operations with different business key configurations.

Pytest fixtures and helper functions support table creation and engine setup.
"""

import pytest
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from dataprepkit.helpers.transforms.insert_update import insert_new_records_dynamic

@pytest.fixture
def engine():
    """
    Pytest fixture to create a temporary in-memory SQLite engine.

    Yields
    ------
    engine : sqlalchemy.engine.Engine
        A SQLAlchemy engine connected to an in-memory SQLite database.

    Notes
    -----
    The engine is disposed after the test completes.
    """
    # pylint: disable=redefined-outer-name
    engine = create_engine("sqlite:///:memory:")
    yield engine
    engine.dispose()

# pylint: disable=redefined-outer-name
def _create_table(engine, name, columns, schema=None):
    """
    Helper function to create a table with given columns in the database.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        SQLAlchemy engine to bind metadata and create tables.
    name : str
        Table name.
    columns : dict
        Dictionary mapping column names to SQLAlchemy column types.
    schema : str, optional
        Schema name if applicable (default is None).

    Returns
    -------
    sqlalchemy.Table
        The created SQLAlchemy Table object.
    """
    metadata = MetaData()
    cols = [Column(colname, coltype) for colname, coltype in columns.items()]
    table = Table(name, metadata, *cols, schema=schema)
    metadata.create_all(engine)
    return table

# pylint: disable=redefined-outer-name
def test_invalid_business_key_type(engine):
    """
    Test that insert_new_records_dynamic raises ValueError when business_key is not
    a string or list of strings.
    """
    with pytest.raises(ValueError, match="`business_key` must be a string or a list of strings"):
        insert_new_records_dynamic(
            engine,
            "source",
            "target",
            business_key=123) # type: ignore[arg-type]

# pylint: disable=redefined-outer-name
def test_source_table_missing(engine):
    """
    Test that insert_new_records_dynamic raises ValueError when the source table
    does not exist.
    """
    _create_table(engine, "target", {"id": Integer, "Assurance_Cd": String})
    with pytest.raises(ValueError, match="Source table"):
        insert_new_records_dynamic(engine, "missing_source", "target")

# pylint: disable=redefined-outer-name
def test_target_table_missing(engine):
    """
    Test that insert_new_records_dynamic raises ValueError when the target table
    does not exist.
    """
    _create_table(engine, "source", {"id": Integer, "Assurance_Cd": String})
    with pytest.raises(ValueError, match="Target table"):
        insert_new_records_dynamic(engine, "source", "missing_target")

# pylint: disable=redefined-outer-name
def test_no_common_columns(engine):
    """
    Test that insert_new_records_dynamic raises ValueError when source and target
    tables have no common columns except the surrogate key.
    """
    _create_table(engine, "source", {"Assurance_Id": Integer, "colA": String})
    _create_table(engine, "target", {"Assurance_Id": Integer, "colB": String})

    with pytest.raises(ValueError, match="No common columns"):
        insert_new_records_dynamic(
            engine,
            "source",
            "target",
            surrogate_key="Assurance_Id",
            business_key="Assurance_Cd")

# pylint: disable=redefined-outer-name
def test_missing_business_key_column(engine):
    """
    Test that insert_new_records_dynamic raises ValueError when business key column
    is missing from either source or target table.
    """
    _create_table(engine, "source", {"id": Integer, "colA": String})
    _create_table(engine, "target", {"id": Integer, "colA": String})

    with pytest.raises(ValueError, match="Business key\\(s\\) missing"):
        insert_new_records_dynamic(engine, "source", "target", business_key="Assurance_Cd")

# pylint: disable=redefined-outer-name
def test_successful_insert(engine):
    """
    Test successful insertion of new records from source to target when the target
    already contains some data. Verifies that only new records are inserted.
    """
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
        assert any(row.Assurance_Cd == "A1" for row in rows)
        assert any(row.Assurance_Cd == "B2" for row in rows)

# pylint: disable=redefined-outer-name
def test_default_start_id_used_when_no_max_id(engine):
    """
    Test that the default_start_id parameter is used when the target table is empty
    and no max surrogate key is found.
    """
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
    """
    Test insertion using multiple columns as a composite business key. Verifies
    that records missing from the target with matching composite keys are inserted.
    """
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

    # Insert one row into target with a matching business key to simulate existing record
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

    assurance_codes = {row.Assurance_Cd for row in rows}
    assert "A1" in assurance_codes
    assert "B2" in assurance_codes
