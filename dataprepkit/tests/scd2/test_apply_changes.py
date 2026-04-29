import hashlib
from datetime import datetime

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

from dataprepkit.scd2 import SCD2ValidationError
from dataprepkit.scd2 import (
    _build_natural_key_match_condition,
    EFFECTIVE_DATE_MAX,
    EFFECTIVE_DATE_MIN,
    _create_staging_table,
    _insert_snapshot_rows,
    _insert_snapshot_rows_from_raw,
    _normalize_existing_join_numeric_for_raw,
    apply_changes,
    synchronize_current_row_hashes,
)


SYSTEM_COLUMNS = {
    "surrogate_key": "surrogate_key",
    "join_numeric_key": "join_numeric_key",
    "row_hash": "row_hash",
    "insert_date": "Insert_Date",
    "update_date": "Update_Date",
    "effective_date_start": "Effective_Date_Start",
    "effective_date_end": "Effective_Date_End",
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
        Effective_Date_Start TEXT NOT NULL,
        Effective_Date_End TEXT NOT NULL,
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
                        Effective_Date_Start,
                        Effective_Date_End,
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
                        :Effective_Date_Start,
                        :Effective_Date_End,
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
        "Effective_Date_Start": insert_ts.isoformat(),
        "Effective_Date_End": EFFECTIVE_DATE_MAX if current_ind else (update_ts.isoformat() if update_ts else EFFECTIVE_DATE_MAX),
        "Current_Ind": current_ind,
        "Deleted_Ind": deleted_ind,
    }


def _validate_insert(result):
    current = set(result.loc[result.Current_Ind == 1, "join_key"])
    assert current == {"a1", "b1", "c1"}
    assert result.shape[0] == 3
    assert not result.loc[result.join_key == "c1", "Deleted_Ind"].any()
    assert result.loc[result.join_key == "c1", "Effective_Date_Start"].iloc[0] == EFFECTIVE_DATE_MIN
    assert result.loc[result.join_key == "c1", "Effective_Date_End"].iloc[0] == EFFECTIVE_DATE_MAX


def _validate_delete(result):
    deleted = result.loc[(result.join_key == "d1") & (result.Deleted_Ind == 1)]
    assert not deleted.empty
    assert deleted.iloc[0]["Current_Ind"] == 1
    assert deleted.iloc[0]["Update_Date"] is not None
    assert deleted.iloc[0]["Effective_Date_End"] == deleted.iloc[0]["Update_Date"]
    current = set(result.loc[result.Current_Ind == 1, "join_key"])
    assert current == {"a1", "b1", "c1", "d1"}


def _validate_update(result):
    current_rows = result.loc[(result.join_key == "c1") & (result.Current_Ind == 1)]
    history = result.loc[(result.join_key == "c1") & (result.Current_Ind == 0)]
    assert len(current_rows) == 1
    assert len(history) == 1
    assert history.iloc[0]["Deleted_Ind"] == 0
    assert current_rows.iloc[0]["data_column"] == "c2222"
    assert current_rows.iloc[0]["Effective_Date_End"] == EFFECTIVE_DATE_MAX
    assert history.iloc[0]["Effective_Date_End"] == history.iloc[0]["Update_Date"]


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
    assert deleted.iloc[0]["Current_Ind"] == 1
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


def test_case_insensitive_business_key_match_marks_original_row_deleted():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS dimension"))
        conn.execute(
            text(
                """
                CREATE TABLE dimension (
                    surrogate_key INTEGER PRIMARY KEY AUTOINCREMENT,
                    join_key TEXT COLLATE NOCASE NOT NULL,
                    join_numeric_key INTEGER NOT NULL,
                    data_column TEXT,
                    row_hash TEXT NOT NULL,
                    Insert_Date TEXT NOT NULL,
                    Update_Date TEXT,
                    Effective_Date_Start TEXT NOT NULL,
                    Effective_Date_End TEXT NOT NULL,
                    Current_Ind INTEGER NOT NULL,
                    Deleted_Ind INTEGER NOT NULL
                )
                """
            )
        )
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
                    Effective_Date_Start,
                    Effective_Date_End,
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
                    :Effective_Date_Start,
                    :Effective_Date_End,
                    :Current_Ind,
                    :Deleted_Ind
                )
                """
            ),
            _build_initial_row("Bio", 2, "before"),
        )

    apply_changes(
        engine=engine,
        target_table="dimension",
        incoming=pd.DataFrame([{"join_key": "BIO", "data_column": "after"}]),
        natural_key_cols=["join_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
    )

    final = _read_table(engine)
    deleted = final.loc[(final.join_key == "Bio") & (final.Deleted_Ind == 1)]
    current = final.loc[(final.join_key == "BIO") & (final.Current_Ind == 1)]

    assert len(deleted) == 1
    assert deleted.iloc[0]["Current_Ind"] == 1
    assert pd.notna(deleted.iloc[0]["Update_Date"])
    assert len(current) == 1
    assert current.iloc[0]["Deleted_Ind"] == 0
    assert current.iloc[0]["join_numeric_key"] == 3


def test_mssql_text_key_match_requires_binary_value_and_length_match():
    class _FakeDialect:
        name = "mssql"

    class _FakeEngine:
        dialect = _FakeDialect()

    condition = _build_natural_key_match_condition(
        _FakeEngine(),
        "t",
        "s",
        ["Site_Cd"],
        {"site_cd": "NVARCHAR(4000)"},
    )

    assert "t.[Site_Cd] COLLATE Latin1_General_100_BIN2" in condition
    assert "s.[Site_Cd] COLLATE Latin1_General_100_BIN2" in condition
    assert "DATALENGTH(t.[Site_Cd]) = DATALENGTH(s.[Site_Cd])" in condition


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


def test_synchronize_current_row_hashes_enables_repair_of_poisoned_rows():
    engine = create_engine("sqlite:///:memory:")
    poisoned_row = _build_initial_row("b1", 11, None)
    poisoned_row["row_hash"] = _hash_value("after")
    _bootstrap_table(engine, [poisoned_row])

    updated = synchronize_current_row_hashes(
        engine=engine,
        target_table="dimension",
        data_cols=["data_column"],
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
    )

    assert updated == 1

    changed = apply_changes(
        engine=engine,
        target_table="dimension",
        incoming=pd.DataFrame([{"join_key": "b1", "data_column": "after"}]),
        natural_key_cols=["join_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
        nullable_columns=["data_column"],
    )

    assert changed is True
    final = _read_table(engine)
    historical = final.loc[(final.join_key == "b1") & (final.Current_Ind == 0)]
    current = final.loc[(final.join_key == "b1") & (final.Current_Ind == 1)]
    assert len(historical) == 1
    assert len(current) == 1
    assert pd.isna(historical.iloc[0]["data_column"])
    assert current.iloc[0]["data_column"] == "after"


def test_reinsert_reuses_deleted_current_join_numeric_even_if_lower_than_max():
    engine = create_engine("sqlite:///:memory:")
    initial_rows = [
        _build_initial_row("z1", 10, "old"),
        _build_initial_row("other", 20, "placeholder"),
    ]
    _bootstrap_table(engine, initial_rows)

    # Remove z1 (delete scenario)
    apply_changes(
        engine=engine,
        target_table="dimension",
        incoming=pd.DataFrame([{"join_key": "other", "join_numeric_key": 20, "data_column": "placeholder"}]),
        natural_key_cols=["join_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
    )

    # Reinsert z1 and expect the original join numeric to be reused.
    reinsert = pd.DataFrame([{"join_key": "z1", "join_numeric_key": 10, "data_column": "new"}])
    apply_changes(
        engine=engine,
        target_table="dimension",
        incoming=reinsert,
        natural_key_cols=["join_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
    )

    final = _read_table(engine)
    current = final.loc[(final.join_key == "z1") & (final.Current_Ind == 1)]
    assert not current.empty
    assert current.iloc[0]["join_numeric_key"] == 10


def test_reinsert_reuses_deleted_current_join_numeric():
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
    assert reinserted.iloc[0]["join_numeric_key"] == 1


def test_reinsert_after_delete_reuses_existing_join_numeric():
    engine = create_engine("sqlite:///:memory:")
    initial_rows = [
        _build_initial_row("a1", 1, "a2"),
        _build_initial_row("b1", 2, "b2"),
    ]
    _bootstrap_table(engine, initial_rows)

    # Force an update to create a new join_numeric_key beyond the placeholder values.
    apply_changes(
        engine=engine,
        target_table="dimension",
        incoming=pd.DataFrame(
            [
                {"join_key": "a1", "data_column": "a2"},
                {"join_key": "b1", "data_column": "b2-modified"},
            ]
        ),
        natural_key_cols=["join_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
    )

    # Delete the current b1 so we can reinsert it.
    apply_changes(
        engine=engine,
        target_table="dimension",
        incoming=pd.DataFrame([{"join_key": "a1", "data_column": "a2"}]),
        natural_key_cols=["join_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
    )

    reinsert = pd.DataFrame([{"join_key": "b1", "data_column": "b2-reinsert"}])
    apply_changes(
        engine=engine,
        target_table="dimension",
        incoming=reinsert,
        natural_key_cols=["join_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
    )

    final = _read_table(engine)
    current = final.loc[(final.join_key == "b1") & (final.Current_Ind == 1)].iloc[0]
    assert current["join_numeric_key"] == 2


def test_trailing_space_business_key_gets_distinct_current_join_numeric():
    engine = create_engine("sqlite:///:memory:")
    _bootstrap_table(engine, [])

    apply_changes(
        engine=engine,
        target_table="dimension",
        incoming=pd.DataFrame([{"join_key": "ABC", "data_column": "alpha"}]),
        natural_key_cols=["join_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
        execution_time="2026-04-28T10:00:00.000+00:00",
    )

    apply_changes(
        engine=engine,
        target_table="dimension",
        incoming=pd.DataFrame([{"join_key": "ABC ", "data_column": "alpha"}]),
        natural_key_cols=["join_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
        execution_time="2026-04-28T11:00:00.000+00:00",
    )

    final = _read_table(engine)
    current = final.loc[final.Current_Ind == 1]
    abc = current.loc[current.join_key == "ABC"].iloc[0]
    abc_space = current.loc[current.join_key == "ABC "].iloc[0]

    assert abc["Deleted_Ind"] == 1
    assert abc_space["Deleted_Ind"] == 0
    assert abc["join_numeric_key"] != abc_space["join_numeric_key"]
    assert current["join_numeric_key"].is_unique


def test_apply_changes_rejects_duplicate_current_join_numeric_keys():
    engine = create_engine("sqlite:///:memory:")
    deleted = _build_initial_row(
        "a1",
        1,
        "a",
        deleted_ind=1,
        update_ts=datetime(2026, 4, 28, 11, 0, 0),
    )
    deleted["Effective_Date_End"] = deleted["Update_Date"]
    _bootstrap_table(
        engine,
        [
            deleted,
            _build_initial_row("b1", 1, "b"),
        ],
    )

    with pytest.raises(
        SCD2ValidationError,
        match="Multiple current rows found for join numeric key column 'join_numeric_key'",
    ):
        apply_changes(
            engine=engine,
            target_table="dimension",
            incoming=pd.DataFrame([{"join_key": "b1", "data_column": "b"}]),
            natural_key_cols=["join_key"],
            data_cols=["data_column"],
            join_numeric_key_col="join_numeric_key",
            surrogate_key_col="surrogate_key",
            system_columns=SYSTEM_COLUMNS,
        )


def test_delete_then_reinsert_keeps_exactly_one_current_row_per_business_key():
    engine = create_engine("sqlite:///:memory:")
    _bootstrap_table(engine, [_build_initial_row("b1", 2, "b2")])

    apply_changes(
        engine=engine,
        target_table="dimension",
        incoming=pd.DataFrame([{"join_key": "other", "data_column": "placeholder"}]),
        natural_key_cols=["join_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
    )

    apply_changes(
        engine=engine,
        target_table="dimension",
        incoming=pd.DataFrame([{"join_key": "b1", "data_column": "b2-reinsert"}]),
        natural_key_cols=["join_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
    )

    final = _read_table(engine)
    current_rows = final.loc[(final.join_key == "b1") & (final.Current_Ind == 1)]
    assert len(current_rows) == 1
    assert current_rows.iloc[0]["Deleted_Ind"] == 0
    assert current_rows.iloc[0]["data_column"] == "b2-reinsert"


def test_apply_changes_returns_flag_for_no_delta():
    engine = create_engine("sqlite:///:memory:")
    _bootstrap_table(engine, [_build_initial_row("x1", 1, "a")])

    incoming = pd.DataFrame([{"join_key": "x1", "data_column": "a"}])

    changed = apply_changes(
        engine=engine,
        target_table="dimension",
        incoming=incoming,
        natural_key_cols=["join_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
    )

    assert not changed


def test_apply_changes_returns_true_when_data_changes():
    engine = create_engine("sqlite:///:memory:")
    _bootstrap_table(engine, [_build_initial_row("x1", 1, "a")])

    incoming = pd.DataFrame([{"join_key": "x1", "data_column": "b"}])

    changed = apply_changes(
        engine=engine,
        target_table="dimension",
        incoming=incoming,
        natural_key_cols=["join_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
    )

    assert changed


def test_apply_changes_can_return_change_summary():
    engine = create_engine("sqlite:///:memory:")
    deleted = _build_initial_row(
        "d1",
        4,
        "d-old",
        deleted_ind=1,
        update_ts=datetime(2026, 1, 8, 15, 0, 0),
    )
    deleted["Effective_Date_End"] = deleted["Update_Date"]
    _bootstrap_table(
        engine,
        [
            _build_initial_row("a1", 1, "a"),
            _build_initial_row("b1", 2, "b-old"),
            _build_initial_row("c1", 3, "c"),
            deleted,
        ],
    )

    summary = apply_changes(
        engine=engine,
        target_table="dimension",
        incoming=pd.DataFrame(
            [
                {"join_key": "a1", "data_column": "a"},
                {"join_key": "b1", "data_column": "b-new"},
                {"join_key": "d1", "data_column": "d-new"},
                {"join_key": "e1", "data_column": "e"},
            ]
        ),
        natural_key_cols=["join_key"],
        data_cols=["data_column"],
        join_numeric_key_col="join_numeric_key",
        surrogate_key_col="surrogate_key",
        system_columns=SYSTEM_COLUMNS,
        return_summary=True,
    )

    assert summary.incoming_rows == 4
    assert summary.target_rows_before == 4
    assert summary.target_rows_after == 7
    assert summary.new_rows == 1
    assert summary.inserted_rows == 3
    assert summary.new_natural_keys == [{"join_key": "e1"}]
    assert summary.edited_rows == 1
    assert summary.edited_rows_detail == [
        {"join_key": "b1", "changes": {"data_column": {"from": "b-old", "to": "b-new"}}}
    ]
    assert summary.edited_natural_keys == [{"join_key": "b1"}]
    assert summary.soft_deleted_rows == 1
    assert summary.soft_deleted_natural_keys == [{"join_key": "c1"}]
    assert summary.reactivated_rows == 1
    assert summary.reactivated_natural_keys == [{"join_key": "d1"}]
    assert summary.unchanged_rows == 1
    assert summary.changes_applied


def test_reject_duplicate_natural_keys():
    engine = create_engine("sqlite:///:memory:")
    _bootstrap_table(engine, [])

    incoming = pd.DataFrame(
        [
            {"join_key": "a1", "join_numeric_key": 1, "data_column": "a"},
            {"join_key": "a1", "join_numeric_key": 2, "data_column": "b"},
        ]
    )

    with pytest.raises(
        SCD2ValidationError,
        match=r"Incoming data contains duplicate natural keys.*Example duplicate keys",
    ) as exc_info:
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
    assert "{'join_key': 'a1', 'count': 2}" in str(exc_info.value)


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


def test_nullable_data_column_allows_null_staging():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE dimension (
                    surrogate_key INTEGER PRIMARY KEY AUTOINCREMENT,
                    join_key TEXT NOT NULL,
                    join_numeric_key INTEGER NOT NULL,
                    data_column TEXT,
                    row_hash TEXT NOT NULL,
                    Insert_Date TEXT NOT NULL,
                    Update_Date TEXT,
                    Effective_Date_Start TEXT NOT NULL,
                    Effective_Date_End TEXT NOT NULL,
                    Current_Ind INTEGER NOT NULL,
                    Deleted_Ind INTEGER NOT NULL
                )
                """
            )
        )

    incoming = pd.DataFrame(
        [
            {"join_key": "a", "data_column": None},
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
        nullable_columns=["data_column"],
    )

    result = pd.read_sql_table("dimension", con=engine)
    assert result.shape[0] == 1
    assert pd.isna(result.iloc[0]["data_column"])


def test_insert_snapshot_rows_sanitizes_nan():
    engine = create_engine("sqlite:///:memory:")
    staging_table = "stage_nan"
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE stage_nan (
                    natural_key TEXT NOT NULL,
                    data_column TEXT,
                    row_hash TEXT NOT NULL,
                    existing_join_numeric REAL
                )
                """
            )
        )
        incoming = pd.DataFrame(
            {
                "natural_key": ["nan-key"],
                "data_column": ["payload"],
                "row_hash": ["hash-payload"],
                "existing_join_numeric": [float("nan")],
            }
        )
        _insert_snapshot_rows(
            conn,
            staging_table,
            incoming,
            natural_key_cols=["natural_key"],
            data_cols=["data_column"],
            hash_col="row_hash",
            extra_columns=["existing_join_numeric"],
        )
        result = pd.read_sql_table(staging_table, conn)
        assert pd.isna(result.loc[0, "existing_join_numeric"])


def test_insert_snapshot_rows_normalizes_integer_like_float_values():
    class _FakeConn:
        def __init__(self):
            self.records = None

        def execute(self, _statement, params=None):
            self.records = list(params or [])

    conn = _FakeConn()
    incoming = pd.DataFrame(
        {
            "Measure_Cd": ["OUT4_19"],
            "Measure_Instance_Id": [350.0],
            "row_hash": ["hash-payload"],
        }
    )

    _insert_snapshot_rows(
        conn,
        "stage_measure",
        incoming,
        natural_key_cols=["Measure_Cd"],
        data_cols=["Measure_Instance_Id"],
        hash_col="row_hash",
    )

    assert conn.records is not None
    assert conn.records[0]["Measure_Cd"] == "OUT4_19"
    assert conn.records[0]["Measure_Instance_Id"] == 350
    assert type(conn.records[0]["Measure_Instance_Id"]) is int
    assert conn.records[0]["row_hash"] == "hash-payload"


def test_insert_snapshot_rows_from_raw_uses_try_cast_for_mssql():
    class _FakeDialect:
        name = "mssql"

    class _FakeEngine:
        dialect = _FakeDialect()

    class _FakeConn:
        engine = _FakeEngine()

        def __init__(self):
            self.calls = []

        def execute(self, statement, params=None):
            self.calls.append((str(statement), dict(params or {})))

    conn = _FakeConn()
    _insert_snapshot_rows_from_raw(
        conn,
        raw_table="stg_raw",
        target_table="stg_typed",
        columns=["Interval_Start_Date", "existing_join_numeric"],
        column_types={
            "interval_start_date": "DATETIME2(3)",
            "existing_join_numeric": "BIGINT",
        },
    )

    sql, params = conn.calls[0]
    assert "INSERT INTO stg_typed ([Interval_Start_Date], [existing_join_numeric])" in sql
    assert "TRY_CAST(NULLIF(src.[Interval_Start_Date], '') AS DATETIME2(3))" in sql
    assert "TRY_CAST(NULLIF(src.[existing_join_numeric], '') AS BIGINT)" in sql
    assert "FROM stg_raw src" in sql
    assert params == {}


def test_create_staging_table_uses_override_type_for_existing_join_numeric():
    class _FakeDialect:
        name = "mssql"

    class _FakeEngine:
        dialect = _FakeDialect()

    class _FakeConn:
        engine = _FakeEngine()

        def __init__(self):
            self.calls = []

        def execute(self, statement, params=None):
            self.calls.append((str(statement), dict(params or {})))

    conn = _FakeConn()
    _create_staging_table(
        conn,
        "temp_snapshot",
        natural_key_cols=["Interval_Cd"],
        data_cols=["Interval_Type"],
        hash_col="row_hash",
        column_types={
            "interval_cd": "NVARCHAR(4000)",
            "interval_type": "NVARCHAR(4000)",
            "row_hash": "NVARCHAR(4000)",
        },
        extra_columns=["existing_join_numeric"],
        extra_column_type_overrides={"existing_join_numeric": "BIGINT"},
        preserve_mssql_types=True,
    )

    sql, params = conn.calls[0]
    assert "[existing_join_numeric] BIGINT" in sql
    assert params == {}


def test_insert_snapshot_rows_from_raw_honors_type_overrides():
    class _FakeDialect:
        name = "mssql"

    class _FakeEngine:
        dialect = _FakeDialect()

    class _FakeConn:
        engine = _FakeEngine()

        def __init__(self):
            self.calls = []

        def execute(self, statement, params=None):
            self.calls.append((str(statement), dict(params or {})))

    conn = _FakeConn()
    _insert_snapshot_rows_from_raw(
        conn,
        raw_table="stg_raw",
        target_table="stg_typed",
        columns=["Site_Cd", "existing_join_numeric"],
        column_types={"site_cd": "NVARCHAR(4000)"},
        column_type_overrides={"existing_join_numeric": "BIGINT"},
    )

    sql, params = conn.calls[0]
    assert "TRY_CAST(NULLIF(src.[Site_Cd], '') AS NVARCHAR(4000))" in sql
    assert "TRY_CAST(NULLIF(src.[existing_join_numeric], '') AS BIGINT)" in sql
    assert params == {}


def test_insert_snapshot_rows_from_raw_uses_join_key_type_for_existing_join_numeric():
    class _FakeDialect:
        name = "mssql"

    class _FakeEngine:
        dialect = _FakeDialect()

    class _FakeConn:
        engine = _FakeEngine()

        def __init__(self):
            self.calls = []

        def execute(self, statement, params=None):
            self.calls.append((str(statement), dict(params or {})))

    conn = _FakeConn()
    join_key_type = "BIGINT"
    _insert_snapshot_rows_from_raw(
        conn,
        raw_table="stg_raw",
        target_table="stg_typed",
        columns=["Site_Cd", "existing_join_numeric"],
        # Simulates real target metadata where only actual join key exists.
        column_types={"site_cd": "NVARCHAR(4000)", "site_id": join_key_type},
        # This mapping is what apply_changes now provides.
        column_type_overrides={"existing_join_numeric": join_key_type},
    )

    sql, params = conn.calls[0]
    assert "TRY_CAST(NULLIF(src.[existing_join_numeric], '') AS BIGINT)" in sql
    assert "TRY_CAST(NULLIF(src.[Site_Cd], '') AS NVARCHAR(4000))" in sql
    assert params == {}


def test_insert_snapshot_rows_from_raw_maps_integrity_to_validation_error():
    class _FakeDialect:
        name = "mssql"

    class _FakeEngine:
        dialect = _FakeDialect()

    class _FakeConn:
        engine = _FakeEngine()

        def execute(self, _statement, _params=None):
            raise IntegrityError("INSERT", {}, Exception("dup"))

    source_df = pd.DataFrame([{"Site_Cd": "A "}, {"Site_Cd": "a"}])
    with pytest.raises(
        SCD2ValidationError,
        match=r"duplicate natural keys after staging normalization/collation",
    ) as exc_info:
        _insert_snapshot_rows_from_raw(
            _FakeConn(),
            raw_table="stg_raw",
            target_table="stg_typed",
            columns=["Site_Cd", "existing_join_numeric"],
            column_types={"site_cd": "NVARCHAR(4000)"},
            column_type_overrides={"existing_join_numeric": "BIGINT"},
            source_df=source_df,
            natural_key_cols=["Site_Cd"],
        )
    assert "{'Site_Cd': 'a', 'count': 2}" in str(exc_info.value)


def test_insert_snapshot_rows_maps_integrity_to_explicit_duplicate_key_error():
    class _FakeConn:
        engine = type("E", (), {"dialect": type("D", (), {"name": "sqlite"})()})()

        def execute(self, _statement, _params=None):
            raise IntegrityError("INSERT", {}, Exception("dup"))

    incoming = pd.DataFrame(
        [
            {"join_key": "A ", "data_column": "x", "row_hash": "h1"},
            {"join_key": "a", "data_column": "y", "row_hash": "h2"},
        ]
    )

    with pytest.raises(
        SCD2ValidationError,
        match=r"duplicate natural keys after staging normalization/collation",
    ) as exc_info:
        _insert_snapshot_rows(
            _FakeConn(),
            "stage_table",
            incoming,
            natural_key_cols=["join_key"],
            data_cols=["data_column"],
            hash_col="row_hash",
        )
    assert "{'join_key': 'a', 'count': 2}" in str(exc_info.value)


def test_normalize_existing_join_numeric_for_raw_keeps_integer_text():
    assert _normalize_existing_join_numeric_for_raw(7084) == "7084"
    assert _normalize_existing_join_numeric_for_raw(7084.0) == "7084"
    assert _normalize_existing_join_numeric_for_raw(None) is None


def test_apply_changes_openrowset_normalizes_existing_join_numeric_for_reuse(monkeypatch):
    class _FakeDialect:
        name = "mssql"

    class _FakeConn:
        def __init__(self, engine):
            self.engine = engine

        def execute(self, _statement, _params=None):
            class _Result:
                rowcount = 0

                @staticmethod
                def fetchall():
                    return [("k1", 7084)]

                @staticmethod
                def scalar():
                    return 0

            return _Result()

    class _FakeBegin:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self.conn

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeEngine:
        dialect = _FakeDialect()

        def __init__(self):
            self.conn = _FakeConn(self)

        def begin(self):
            return _FakeBegin(self.conn)

    captured = {}

    def fake_stage_dataframe(_engine, _table_name, df, **_kwargs):
        captured["raw_existing"] = df["existing_join_numeric"].tolist()

    monkeypatch.setattr("dataprepkit.scd2.stage_dataframe", fake_stage_dataframe)
    monkeypatch.setattr("dataprepkit.scd2._get_column_types", lambda *_: {"site_id": "BIGINT"})
    monkeypatch.setattr("dataprepkit.scd2._count_rows", lambda *_: 0)
    monkeypatch.setattr("dataprepkit.scd2._validate_row_growth", lambda *_: None)
    monkeypatch.setattr("dataprepkit.scd2._validate_current_join_numeric_unique", lambda *_: None)
    monkeypatch.setattr("dataprepkit.scd2._create_staging_table", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("dataprepkit.scd2._insert_snapshot_rows_from_raw", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("dataprepkit.scd2._apply_snapshot_to_target", lambda *_args, **_kwargs: False)

    engine = _FakeEngine()
    incoming = pd.DataFrame([{"Site_Cd": "k1", "Site_Name": "edited"}])
    apply_changes(
        engine=engine,
        target_table="Dimensions.dim_site",
        incoming=incoming,
        natural_key_cols=["Site_Cd"],
        data_cols=["Site_Name"],
        join_numeric_key_col="Site_Id",
        surrogate_key_col="Site_Instance_Id",
        system_columns=SYSTEM_COLUMNS,
        staging_use_openrowset_parquet=True,
        staging_parquet_base_dir="/tmp/stage",
    )

    assert captured["raw_existing"] == ["7084"]


def test_apply_changes_openrowset_reuses_join_numeric_when_natural_key_is_numeric_like_text(
    monkeypatch,
):
    class _FakeDialect:
        name = "mssql"

    class _FakeConn:
        def __init__(self, engine):
            self.engine = engine

        def execute(self, _statement, _params=None):
            class _Result:
                rowcount = 0

                @staticmethod
                def fetchall():
                    return [("1980", 5001)]

                @staticmethod
                def scalar():
                    return 0

            return _Result()

    class _FakeBegin:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self.conn

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeEngine:
        dialect = _FakeDialect()

        def __init__(self):
            self.conn = _FakeConn(self)

        def begin(self):
            return _FakeBegin(self.conn)

    captured = {}

    def fake_stage_dataframe(_engine, _table_name, df, **_kwargs):
        captured["interval_cd"] = df["Interval_Cd"].tolist()
        captured["raw_existing"] = df["existing_join_numeric"].tolist()

    monkeypatch.setattr("dataprepkit.scd2.stage_dataframe", fake_stage_dataframe)
    monkeypatch.setattr(
        "dataprepkit.scd2._get_column_types",
        lambda *_: {
            "interval_cd": "NVARCHAR(4000)",
            "interval_end_date": "DATETIME2(3)",
            "interval_id": "BIGINT",
            "row_hash": "NVARCHAR(4000)",
        },
    )
    monkeypatch.setattr("dataprepkit.scd2._count_rows", lambda *_: 0)
    monkeypatch.setattr("dataprepkit.scd2._validate_row_growth", lambda *_: None)
    monkeypatch.setattr("dataprepkit.scd2._validate_current_join_numeric_unique", lambda *_: None)
    monkeypatch.setattr("dataprepkit.scd2._create_staging_table", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("dataprepkit.scd2._insert_snapshot_rows_from_raw", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("dataprepkit.scd2._apply_snapshot_to_target", lambda *_args, **_kwargs: False)

    engine = _FakeEngine()
    incoming = pd.DataFrame(
        [{"Interval_Cd": 1980, "Interval_End_Date": pd.Timestamp("1980-12-31 00:00:00.000")}]
    )
    apply_changes(
        engine=engine,
        target_table="Dimensions.dim_interval",
        incoming=incoming,
        natural_key_cols=["Interval_Cd"],
        data_cols=["Interval_End_Date"],
        join_numeric_key_col="Interval_Id",
        surrogate_key_col="Interval_Instance_Id",
        system_columns=SYSTEM_COLUMNS,
        staging_use_openrowset_parquet=True,
        staging_parquet_base_dir="/tmp/stage",
    )

    assert captured["interval_cd"] == ["1980"]
    assert captured["raw_existing"] == ["5001"]


def test_apply_changes_openrowset_normalizes_integer_like_data_columns(monkeypatch):
    class _FakeDialect:
        name = "mssql"

    class _FakeConn:
        def __init__(self, engine):
            self.engine = engine

        def execute(self, _statement, _params=None):
            class _Result:
                rowcount = 0

                @staticmethod
                def fetchall():
                    return []

                @staticmethod
                def scalar():
                    return 0

            return _Result()

    class _FakeBegin:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self.conn

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeEngine:
        dialect = _FakeDialect()

        def __init__(self):
            self.conn = _FakeConn(self)

        def begin(self):
            return _FakeBegin(self.conn)

    captured = {}

    def fake_stage_dataframe(_engine, _table_name, df, **_kwargs):
        captured["raw_measure_instance"] = df["Measure_Instance_Id"].tolist()

    monkeypatch.setattr("dataprepkit.scd2.stage_dataframe", fake_stage_dataframe)
    monkeypatch.setattr(
        "dataprepkit.scd2._get_column_types",
        lambda *_: {
            "measure_cd": "NVARCHAR(4000)",
            "measure_instance_id": "BIGINT",
            "measure_id": "BIGINT",
            "row_hash": "NVARCHAR(4000)",
        },
    )
    monkeypatch.setattr("dataprepkit.scd2._count_rows", lambda *_: 0)
    monkeypatch.setattr("dataprepkit.scd2._validate_row_growth", lambda *_: None)
    monkeypatch.setattr("dataprepkit.scd2._validate_current_join_numeric_unique", lambda *_: None)
    monkeypatch.setattr("dataprepkit.scd2._create_staging_table", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("dataprepkit.scd2._insert_snapshot_rows_from_raw", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("dataprepkit.scd2._apply_snapshot_to_target", lambda *_args, **_kwargs: False)

    engine = _FakeEngine()
    incoming = pd.DataFrame(
        [{"Measure_Cd": "OUT4_19", "Measure_Instance_Id": 350.0}]
    )
    apply_changes(
        engine=engine,
        target_table="Dimensions.map_measure",
        incoming=incoming,
        natural_key_cols=["Measure_Cd"],
        data_cols=["Measure_Instance_Id"],
        join_numeric_key_col="Measure_Id",
        surrogate_key_col="Measure_Instance_Id",
        system_columns=SYSTEM_COLUMNS,
        staging_use_openrowset_parquet=True,
        staging_parquet_base_dir="/tmp/stage",
    )

    assert captured["raw_measure_instance"] == ["350"]
