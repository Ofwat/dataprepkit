"""
DataFrame transformation utility for dimensional ETL pipelines.

This module provides a function `process_dim_dataframe` that validates the schema of a
Pandas DataFrame, applies column renames, and adds standard metadata columns
such as `Batch_Id`, `Insert_Date`, and `Update_Date`.

Intended to be used in batch ETL pipelines where dimensional data is staged or
transformed before loading into a data warehouse.

Dependencies:
    - pandas
    - typing
    - logging

Example usage:
    >>> from your_module import process_dim_dataframe
    >>> df = pd.DataFrame({"id": [1], "name": ["Alice"]})
    >>> processed = process_dim_dataframe(
    ...     df,
    ...     expected_columns={"id", "name"},
    ...     renames={"name": "full_name"},
    ...     batch_id="20251014-001"
    ... )

"""

import logging as _logging
from typing import Dict as _Dict, Set as _Set
import pandas as _pd

logger = _logging.getLogger(__name__)

def process_dim_dataframe(
    df: _pd.DataFrame,
    expected_columns: _Set[str],
    renames: _Dict[str, str],
    batch_id: str
) -> _pd.DataFrame:
    """
    Process and enrich a dimensional DataFrame for ETL pipelines.

    This function performs the following steps:
    1. Validates that the DataFrame's columns exactly match the expected schema.
    2. Renames columns according to the provided mapping.
    3. Adds standard metadata columns: `Batch_Id`, `Insert_Date`, and `Update_Date`.

    :param df: The input pandas DataFrame to transform.
    :type df: pd.DataFrame
    :param expected_columns: A set of column names that are required in the DataFrame.
    :type expected_columns: Set[str]
    :param renames: A dictionary mapping old column names to new column names.
    :type renames: Dict[str, str]
    :param batch_id: A string identifier for the ETL batch run.
    :type batch_id: str

    :raises ValueError: If any expected column is missing or if there are unexpected extra columns.

    :return: A transformed DataFrame with renamed columns and added metadata.
    :rtype: pd.DataFrame

    :Example:

    >>> import pandas as pd
    >>> df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    >>> expected_cols = {"id", "name"}
    >>> renames = {"name": "full_name"}
    >>> batch_id = "20251014-001"
    >>> result = process_dim_dataframe(df, expected_cols, renames, batch_id)
    >>> result.columns
    Index(['id', 'full_name', 'Batch_Id', 'Insert_Date', 'Update_Date'], dtype='object')
    """
    actual_columns = set(df.columns)

    missing_cols = expected_columns - actual_columns
    extra_cols = actual_columns - expected_columns

    if missing_cols:
        raise ValueError(f"Missing expected columns: {missing_cols}")
    if extra_cols:
        raise ValueError(f"Unexpected extra columns: {extra_cols}")

    logger.debug("Renaming columns:%s", renames)
    df = df.rename(columns=renames)

    now = _pd.Timestamp.now()
    df["Batch_Id"] = batch_id
    df["Insert_Date"] = now
    df["Update_Date"] = now

    logger.info("Processed DataFrame with %d rows for batch_id %s", len(df), batch_id)

    return df
