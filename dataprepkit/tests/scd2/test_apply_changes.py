import hashlib
from datetime import datetime

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

from dataprepkit.scd2 import SCD2ValidationError
from dataprepkit.scd2 import (
    _create_staging_table,
    _insert_snapshot_rows,
    _insert_snapshot_rows_from_raw,
    _normalize_existing_join_numeric_for_raw,
    apply_changes,
)


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


def test_reinsert_grows_past_current_max():
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

    # Reinsert z1 and expect join numeric to exceed previous max (20)
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
    assert current.iloc[0]["join_numeric_key"] > 20


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


def test_reinsert_after_delete_uses_new_join_numeric():
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

    updated = _read_table(engine)
    max_join_after_update = updated["join_numeric_key"].max()

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
    assert current["join_numeric_key"] > max_join_after_update


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
    assert "existing_join_numeric BIGINT" in sql
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

    with pytest.raises(SCD2ValidationError, match="duplicate natural keys"):
        _insert_snapshot_rows_from_raw(
            _FakeConn(),
            raw_table="stg_raw",
            target_table="stg_typed",
            columns=["Site_Cd", "existing_join_numeric"],
            column_types={"site_cd": "NVARCHAR(4000)"},
            column_type_overrides={"existing_join_numeric": "BIGINT"},
        )


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
