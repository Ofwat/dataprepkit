import hashlib
from datetime import datetime

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from dataprepkit.scd2 import SCD2ValidationError

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


def test_apply_changes_repeated_insert_is_idempotent():
    engine = create_engine("sqlite:///:memory:")
    initial_rows = [
        _build_initial_row("a1", 1, "a2"),
        _build_initial_row("b1", 2, "b2"),
    ]
    _bootstrap_table(engine, initial_rows)

    incoming = pd.DataFrame(
        [
            {"join_key": "a1", "join_numeric_key": 1, "data_column": "a2"},
            {"join_key": "b1", "join_numeric_key": 2, "data_column": "b2"},
        ]
    )

    target = "dimension"
    apply_changes(
        engine=engine,
        target_table=target,
        incoming=incoming,
        natural_key_cols=["join_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
    )

    first_count = len(_read_table(engine))

    apply_changes(
        engine=engine,
        target_table=target,
        incoming=incoming,
        natural_key_cols=["join_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
    )

    second_count = len(_read_table(engine))
    assert first_count == second_count == len(initial_rows), "Repeated inserts should not add rows"


def test_delete_marks_row_as_historical():
    engine = create_engine("sqlite:///:memory:")
    initial_rows = [
        _build_initial_row("d1", 4, "d2"),
    ]
    _bootstrap_table(engine, initial_rows)

    incoming = pd.DataFrame([{"join_key": "other", "join_numeric_key": 5, "data_column": "a2"}])
    apply_changes(
        engine=engine,
        target_table="dimension",
        incoming=incoming,
        natural_key_cols=["join_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
    )

    final = _read_table(engine)
    deleted = final.loc[final.join_key == "d1"]
    assert not deleted.empty
    assert deleted.iloc[0]["Current_Ind"] == 0
    assert deleted.iloc[0]["Deleted_Ind"] == 1
    assert pd.notna(deleted.iloc[0]["Update_Date"])


def test_update_preserves_delete_flag_off():
    engine = create_engine("sqlite:///:memory:")
    initial_rows = [
        _build_initial_row("b1", 2, "b2"),
    ]
    _bootstrap_table(engine, initial_rows)

    incoming = pd.DataFrame([{"join_key": "b1", "join_numeric_key": 2, "data_column": "modified"}])
    apply_changes(
        engine=engine,
        target_table="dimension",
        incoming=incoming,
        natural_key_cols=["join_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
    )

    final = _read_table(engine)
    previous = final.loc[(final.join_key == "b1") & (final.Current_Ind == 0)]
    current = final.loc[(final.join_key == "b1") & (final.Current_Ind == 1)]
    assert len(current) == 1
    assert previous.iloc[0]["Deleted_Ind"] == 0
    assert pd.notna(previous.iloc[0]["Update_Date"])


def test_join_numeric_reused_for_existing_key():
    engine = create_engine("sqlite:///:memory:")
    initial_rows = [
        _build_initial_row("x1", 10, "initial"),
    ]
    _bootstrap_table(engine, initial_rows)

    incoming = pd.DataFrame([{"join_key": "x1", "data_column": "updated"}])
    apply_changes(
        engine=engine,
        target_table="dimension",
        incoming=incoming,
        natural_key_cols=["join_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
    )

    final = _read_table(engine)
    current = final.loc[(final.join_key == "x1") & (final.Current_Ind == 1)].iloc[0]
    assert current["join_numeric_key"] == 10


def test_reinsert_gets_new_join_numeric():
    engine = create_engine("sqlite:///:memory:")
    initial_rows = [
        _build_initial_row("z1", 1, "old"),
    ]
    _bootstrap_table(engine, initial_rows)

    # delete row by sending only another key
    apply_changes(
        engine=engine,
        target_table="dimension",
        incoming=pd.DataFrame(
            [{"join_key": "other", "join_numeric_key": 5, "data_column": "placeholder"}]
        ),
        natural_key_cols=["join_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
    )

    # reinsert with new data
    new_row = pd.DataFrame(
        [{"join_key": "z1", "join_numeric_key": 1, "data_column": "new"}]
    )
    apply_changes(
        engine=engine,
        target_table="dimension",
        incoming=new_row,
        natural_key_cols=["join_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
    )

    final = _read_table(engine)
    reinserted = final.loc[(final.join_key == "z1") & (final.Current_Ind == 1)]
    assert not reinserted.empty
    # Join numeric should have advanced from previous max
    assert reinserted.iloc[0]["join_numeric_key"] > 1


def test_reject_duplicate_natural_keys():
    engine = create_engine("sqlite:///:memory:")
    _bootstrap_table(engine, [])

    incoming = pd.DataFrame(
        [
            {"join_key": "a1", "join_numeric_key": 1, "data_column": "a"},
            {"join_key": "a1", "join_numeric_key": 2, "data_column": "b"},
        ]
    )

    with pytest.raises(SCD2ValidationError):
        apply_changes(
            engine=engine,
            target_table="dimension",
            incoming=incoming,
            natural_key_cols=["join_key"],
            data_cols=["data_column"],
            join_numeric_key_col="join_numeric_key",
            surrogate_key_col="surrogate_key",
            system_columns=SYSTEM_COLUMNS,
        )


def test_execution_time_is_consistent(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    _bootstrap_table(
        engine,
        [
            _build_initial_row("a1", 1, "a2"),
            _build_initial_row("b1", 2, "b2"),
        ],
    )

    incoming = pd.DataFrame(
        [
            {"join_key": "a1", "data_column": "a2"},
            {"join_key": "b1", "data_column": "b22"},
        ]
    )

    constant_ts = "2026-01-08T12:00:00.000+00:00"
    monkeypatch.setattr("dataprepkit.scd2._execution_timestamp", lambda: constant_ts)

    apply_changes(
        engine=engine,
        target_table="dimension",
        incoming=incoming,
        natural_key_cols=["join_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
    )

    df = _read_table(engine)
    historical = df.loc[(df.join_key == "b1") & (df.Current_Ind == 0)].iloc[0]
    current = df.loc[(df.join_key == "b1") & (df.Current_Ind == 1)].iloc[0]
    assert historical["Update_Date"] == constant_ts
    assert current["Insert_Date"] == constant_ts


def test_join_numeric_increases_for_each_insert():
    engine = create_engine("sqlite:///:memory:")
    _bootstrap_table(engine, [_build_initial_row("a1", 1, "a2")])

    incoming = pd.DataFrame(
        [
            {"join_key": "a1", "data_column": "a2"},
            {"join_key": "b1", "data_column": "b2"},
            {"join_key": "c1", "data_column": "c3"},
            {"join_key": "d1", "data_column": "d4"},
        ]
    )

    apply_changes(
        engine=engine,
        target_table="dimension",
        incoming=incoming,
        natural_key_cols=["join_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
    )

    df = _read_table(engine)
    inserted = df.loc[
        df.join_key.isin(["b1", "c1", "d1"]) & (df.Current_Ind == 1), :
    ].sort_values("join_key")
    assert inserted["join_numeric_key"].tolist() == [2, 3, 4]
