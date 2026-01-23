import hashlib
from datetime import datetime

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from dataprepkit.scd2 import apply_changes


SYSTEM_COLUMNS = {
    "surrogate_key": "surrogate_key",
    "join_numeric_key": "join_numeric_key",
    "row_hash": "row_hash",
    "insert_date": "Insert_Date",
    "update_date": "Update_Date",
    "current_ind": "Current_Ind",
    "deleted_ind": "Deleted_Ind",
}


def _hash_value(value: str) -> str:
    return hashlib.sha256(f"data_column={value}".encode("utf-8")).hexdigest()


def _bootstrap_table(engine, rows):
    ddl = """
    CREATE TABLE dimension (
        surrogate_key INTEGER PRIMARY KEY AUTOINCREMENT,
        join_key TEXT NOT NULL,
        join_numeric_key INTEGER NOT NULL,
        data_column TEXT,
        row_hash TEXT NOT NULL,
        Insert_Date TEXT NOT NULL,
        Update_Date TEXT,
        Current_Ind INTEGER NOT NULL,
        Deleted_Ind INTEGER NOT NULL
    )
    """
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS dimension"))
        conn.execute(text(ddl))
        for row in rows:
            conn.execute(
                text(
                    """
                    INSERT INTO dimension (
                        join_key,
                        join_numeric_key,
                        data_column,
                        row_hash,
                        Insert_Date,
                        Update_Date,
                        Current_Ind,
                        Deleted_Ind
                    )
                    VALUES (
                        :join_key,
                        :join_numeric_key,
                        :data_column,
                        :row_hash,
                        :Insert_Date,
                        :Update_Date,
                        :Current_Ind,
                        :Deleted_Ind
                    )
                    """
                ),
                row,
            )


def _read_table(engine):
    return pd.read_sql_table("dimension", con=engine)


def _build_initial_row(join_key, join_numeric_key, data_column, current_ind=1, deleted_ind=0, insert_ts=None, update_ts=None):
    insert_ts = insert_ts or datetime(2026, 1, 8, 14, 44, 55)
    return {
        "join_key": join_key,
        "join_numeric_key": join_numeric_key,
        "data_column": data_column,
        "row_hash": _hash_value(data_column),
        "Insert_Date": insert_ts.isoformat(),
        "Update_Date": update_ts.isoformat() if update_ts else None,
        "Current_Ind": current_ind,
        "Deleted_Ind": deleted_ind,
    }


def _validate_insert(result):
    current = set(result.loc[result.Current_Ind == 1, "join_key"])
    assert current == {"a1", "b1", "c1"}
    assert result.shape[0] == 3
    assert not result.loc[result.join_key == "c1", "Deleted_Ind"].any()


def _validate_delete(result):
    deleted = result.loc[(result.join_key == "d1") & (result.Deleted_Ind == 1)]
    assert not deleted.empty
    assert deleted.iloc[0]["Current_Ind"] == 0
    assert deleted.iloc[0]["Update_Date"] is not None
    current = set(result.loc[result.Current_Ind == 1, "join_key"])
    assert current == {"a1", "b1", "c1"}


def _validate_update(result):
    current_rows = result.loc[(result.join_key == "c1") & (result.Current_Ind == 1)]
    history = result.loc[(result.join_key == "c1") & (result.Current_Ind == 0)]
    assert len(current_rows) == 1
    assert len(history) == 1
    assert history.iloc[0]["Deleted_Ind"] == 0
    assert current_rows.iloc[0]["data_column"] == "c2222"


def _validate_reinsert(result):
    current = set(result.loc[result.Current_Ind == 1, "join_key"])
    assert current == {"a1", "b1", "c1", "d1"}
    deleted = result.loc[(result.join_key == "d1") & (result.Deleted_Ind == 1)]
    assert len(deleted) == 1


SCENARIOS = [
    {
        "name": "insert",
        "initial": [
            _build_initial_row("a1", 1, "a2"),
            _build_initial_row("b1", 2, "b2"),
        ],
        "incoming": pd.DataFrame(
            [{"join_key": "a1", "data_column": "a2"}, {"join_key": "b1", "data_column": "b2"}, {"join_key": "c1", "data_column": "c2"}]
        ),
        "validator": _validate_insert,
    },
    {
        "name": "delete",
        "initial": [
            _build_initial_row("a1", 1, "a2"),
            _build_initial_row("b1", 2, "b2"),
            _build_initial_row("c1", 3, "c2"),
            _build_initial_row("d1", 4, "d2"),
        ],
        "incoming": pd.DataFrame(
            [{"join_key": "a1", "data_column": "a2"}, {"join_key": "b1", "data_column": "b2"}, {"join_key": "c1", "data_column": "c2"}]
        ),
        "validator": _validate_delete,
    },
    {
        "name": "update",
        "initial": [
            _build_initial_row("a1", 1, "a2"),
            _build_initial_row("b1", 2, "b2"),
            _build_initial_row("c1", 3, "c2"),
        ],
        "incoming": pd.DataFrame(
            [{"join_key": "a1", "data_column": "a2"}, {"join_key": "b1", "data_column": "b2"}, {"join_key": "c1", "data_column": "c2222"}]
        ),
        "validator": _validate_update,
    },
    {
        "name": "reinsertion",
        "initial": [
            _build_initial_row("a1", 1, "a2"),
            _build_initial_row("b1", 2, "b2"),
            _build_initial_row("c1", 3, "c2"),
            _build_initial_row("d1", 4, "d2", current_ind=0, deleted_ind=1, update_ts=datetime(2026, 1, 8, 14, 56, 55)),
        ],
        "incoming": pd.DataFrame(
            [
                {"join_key": "a1", "data_column": "a2"},
                {"join_key": "b1", "data_column": "b2"},
                {"join_key": "c1", "data_column": "c2"},
                {"join_key": "d1", "data_column": "d2"},
            ]
        ),
        "validator": _validate_reinsert,
    },
]


@pytest.mark.parametrize("scenario", SCENARIOS, ids=[s["name"] for s in SCENARIOS])
def test_apply_changes_scenarios(scenario):
    engine = create_engine("sqlite:///:memory:")
    _bootstrap_table(engine, scenario["initial"])

    apply_changes(
        engine=engine,
        target_table="dimension",
        incoming=scenario["incoming"],
        natural_key_cols=["join_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
    )

    final = _read_table(engine)
    scenario["validator"](final)
