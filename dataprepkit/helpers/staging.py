from typing import Literal

import pandas as pd
from sqlalchemy import Engine


def stage_dataframe(
    engine: Engine,
    table_name: str,
    df: pd.DataFrame,
    *,
    if_exists: Literal["fail", "replace", "append"] = "replace",
    index: bool = False,
    schema: str | None = None,
) -> None:
    """
    Write a DataFrame into a staging table (generic helper).

    Parameters
    ----------
    engine
        SQLAlchemy engine.
    table_name
        Target table name (schema qualified if needed).
    df
        DataFrame to persist.
    if_exists
        pandas strategy when the table already exists.
    index
        Whether to write DataFrame index.
    schema
        Optional schema name.
    """
    df.to_sql(
        table_name,
        engine,
        if_exists=if_exists,
        index=index,
        schema=schema,
    )
