"""
Unit tests for `process_dim_dataframe` function in the dataprepkit package.

These tests cover various scenarios including:
- Successful processing with correct input.
- Handling of missing expected columns.
- Handling of unexpected extra columns.
- Behavior when no columns need renaming.
- Behavior when all columns are renamed.

Uses pytest and pandas for testing.
"""

import pytest 
import pandas as pd
from dataprepkit.processors.dimensions.dim_common import process_dim_dataframe

def test_process_dim_dataframe_basic_success():
    """
    Test that `process_dim_dataframe` successfully processes a DataFrame
    when given correct expected columns and rename mappings.
    
    Checks that:
    - Columns are renamed correctly.
    - Metadata columns ('Batch_Id', 'Insert_Date', 'Update_Date') are added.
    - Batch_Id column has the correct value.
    - Insert_Date and Update_Date are timestamp types and equal.
    """
    df = pd.DataFrame({
        "id": [1, 2],
        "name": ["Alice", "Bob"],
    })
    expected_columns = {"id", "name"}
    renames = {"name": "full_name"}
    batch_id = "batch-123"

    result = process_dim_dataframe(df, expected_columns, renames, batch_id)

    assert "full_name" in result.columns
    assert "name" not in result.columns
    assert "id" in result.columns

    for col in ["Batch_Id", "Insert_Date", "Update_Date"]:
        assert col in result.columns

    assert (result["Batch_Id"] == batch_id).all()
    assert pd.api.types.is_datetime64_any_dtype(result["Insert_Date"])
    assert pd.api.types.is_datetime64_any_dtype(result["Update_Date"])
    assert (result["Insert_Date"] == result["Update_Date"]).all()


def test_process_dim_dataframe_missing_columns_raises():
    """
    Test that `process_dim_dataframe` raises a ValueError when
    the input DataFrame is missing expected columns.
    """
    df = pd.DataFrame({
        "id": [1, 2]
    })
    expected_columns = {"id", "name"}  # 'name' is missing
    renames = {}
    batch_id = "batch-123"

    with pytest.raises(ValueError, match="Missing expected columns"):
        process_dim_dataframe(df, expected_columns, renames, batch_id)


def test_process_dim_dataframe_extra_columns_raises():
    """
    Test that `process_dim_dataframe` raises a ValueError when
    the input DataFrame contains unexpected extra columns.
    """
    df = pd.DataFrame({
        "id": [1, 2],
        "name": ["Alice", "Bob"],
        "extra_col": [9, 9]
    })
    expected_columns = {"id", "name"}  # 'extra_col' is unexpected
    renames = {}
    batch_id = "batch-123"

    with pytest.raises(ValueError, match="Unexpected extra columns"):
        process_dim_dataframe(df, expected_columns, renames, batch_id)


def test_process_dim_dataframe_no_renames():
    """
    Test that `process_dim_dataframe` works correctly when no columns
    need to be renamed.
    """
    df = pd.DataFrame({
        "id": [1],
        "name": ["Alice"]
    })
    expected_columns = {"id", "name"}
    renames = {}
    batch_id = "batch-123"

    result = process_dim_dataframe(df, expected_columns, renames, batch_id)
    expected_cols = expected_columns | {"Batch_Id", "Insert_Date", "Update_Date"}
    assert set(result.columns) >= expected_cols


def test_process_dim_dataframe_renames_all_columns():
    """
    Test that `process_dim_dataframe` correctly renames all specified columns.
    """
    df = pd.DataFrame({
        "old1": [1, 2],
        "old2": ["x", "y"]
    })
    expected_columns = {"old1", "old2"}
    renames = {"old1": "new1", "old2": "new2"}
    batch_id = "batch-123"

    result = process_dim_dataframe(df, expected_columns, renames, batch_id)
    assert "new1" in result.columns
    assert "new2" in result.columns
    assert "old1" not in result.columns
    assert "old2" not in result.columns
