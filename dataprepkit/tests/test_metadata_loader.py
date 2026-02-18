from dataprepkit import metadata_loader
from dataprepkit.metadata_loader import (
    ColumnSpec,
    DependencyJoin,
    DimensionMetadata,
    _apply_table_and_column_comments,
    _apply_system_column_comments,
)
from sqlalchemy import create_engine, text


def test_register_metadata_accepts_schema_alias():
    metadata_loader.METADATA_REGISTRY.pop("schema_test", None)

    metadata_loader.register_metadata(
        "schema_test",
        {
            "target_table": "dimtable",
            "natural_key_cols": ["id"],
            "data_columns": {"value": {"type": "TEXT"}},
            "surrogate_key": "surrogate",
            "join_numeric_key": "join_key",
            "filepath": "dummy",
            "schema": "myschema",
        },
    )

    entry = metadata_loader.get_metadata("schema_test")
    assert isinstance(entry, DimensionMetadata)
    assert entry.target_schema == "myschema"
    assert entry.target_table.startswith("myschema.")
    metadata_loader.METADATA_REGISTRY.pop("schema_test", None)


def test_register_metadata_targets_schema_precedence():
    metadata_loader.METADATA_REGISTRY.pop("schema_test", None)

    metadata_loader.register_metadata(
        "schema_test",
        {
            "target_table": "dimtable",
            "natural_key_cols": ["id"],
            "data_columns": {"value": {"type": "TEXT"}},
            "surrogate_key": "surrogate",
            "join_numeric_key": "join_key",
            "filepath": "dummy",
            "schema": "ignored",
            "target_schema": "preferred",
        },
    )

    entry = metadata_loader.get_metadata("schema_test")
    assert entry.target_schema == "preferred"
    assert entry.target_table.startswith("preferred.")
    metadata_loader.METADATA_REGISTRY.pop("schema_test", None)


def test_dependency_where_clause_filters_join():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE dim_service (
                    Service_Type_Cd TEXT NOT NULL,
                    Current_Ind INTEGER NOT NULL,
                    Policy_Flag TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_service (Service_Type_Cd, Current_Ind, Policy_Flag)
                VALUES
                    ('S1', 1, 'flag-yes'),
                    ('S2', 0, 'flag-no')
                """
            )
        )
    incoming = metadata_loader.pd.DataFrame({"Service_Type_Cd": ["S1", "S2"]})
    dependency = DependencyJoin(
        table="dim_service",
        on=[{"source": "Service_Type_Cd", "target": "Service_Type_Cd"}],
        select={"Policy_Flag": "Policy_Flag"},
        where={"target": ["Current_Ind == 1"]},
        on_missing="null",
    )

    joined = metadata_loader._apply_dependency_joins(
        incoming, [dependency], engine
    )
    assert joined.loc[joined.Service_Type_Cd == "S1", "Policy_Flag"].iloc[0] == "flag-yes"
    assert metadata_loader.pd.isna(
        joined.loc[joined.Service_Type_Cd == "S2", "Policy_Flag"]
    ).iloc[0]


def test_cast_data_columns_parses_datetime():
    metadata_loader.METADATA_REGISTRY.pop("cast_test", None)


def test_register_metadata_applies_archive_defaults(tmp_path):
    metadata_loader.METADATA_REGISTRY.pop("archive_default", None)
    metadata_loader.register_metadata(
        "archive_default",
        {
            "target_table": "dimtable",
            "natural_key_cols": ["id"],
            "data_columns": {"value": {"type": "TEXT"}},
            "surrogate_key": "surrogate",
            "join_numeric_key": "join_key",
            "filepath": "dummy",
        },
        archive_base_dir=str(tmp_path / "archive"),
        archive_batch_id="batch1",
    )

    entry = metadata_loader.get_metadata("archive_default")
    assert entry.archive_path is not None
    assert "dimtable__" in entry.archive_path
    assert entry.archive_path.startswith(str(tmp_path / "archive"))
    metadata_loader.METADATA_REGISTRY.pop("archive_default", None)
    metadata_loader.register_metadata(
        "cast_test",
        {
            "target_table": "dimtable",
            "natural_key_cols": ["id"],
            "data_columns": {
                "value": {
                    "type": "DATETIME2(3)",
                    "nullable": True,
                    "parse_format": "%Y-%m-%dT%H:%M:%S.%fZ",
                }
            },
            "surrogate_key": "surrogate",
            "join_numeric_key": "join_key",
            "filepath": "dummy",
        },
    )

    metadata = metadata_loader.get_metadata("cast_test")
    incoming = metadata_loader.pd.DataFrame({"value": ["2026-01-01T12:00:00.000Z"]})
    casted = metadata_loader._cast_data_columns(incoming, metadata)
    assert metadata_loader.pd.api.types.is_datetime64_any_dtype(casted["value"])
    metadata_loader.METADATA_REGISTRY.pop("cast_test", None)


def test_default_csv_reader_handles_csv(tmp_path):
    data = metadata_loader.pd.DataFrame({"col": ["a", "b"]})
    src = tmp_path / "data.csv"
    data.to_csv(src, index=False)

    result = metadata_loader._default_csv_reader(str(src))
    assert metadata_loader.pd.api.types.is_string_dtype(result["col"])
    assert result["col"].tolist() == ["a", "b"]


def test_default_csv_reader_handles_excel(tmp_path):
    try:
        import openpyxl  # noqa: F401
    except ImportError:
        pytest.skip("openpyxl is required to test Excel input")

    data = metadata_loader.pd.DataFrame({"col": ["x", "y"], "num": [1, 2]})
    src = tmp_path / "data.xlsx"
    data.to_excel(src, index=False)

    result = metadata_loader._default_csv_reader(str(src))
    assert list(result["col"]) == ["x", "y"]
    assert list(result["num"]) == [1, 2]
    result_with_missing = metadata_loader._default_csv_reader(str(src))
    assert metadata_loader.pd.isna(result_with_missing["col"]).sum() == 0


def test_default_csv_reader_handles_parquet(tmp_path, monkeypatch):
    src = tmp_path / "data.parquet"
    src.touch()
    expected = metadata_loader.pd.DataFrame({"col": ["p1", "p2"]})

    called = {"filepath": None}

    def fake_read_parquet(filepath):
        called["filepath"] = filepath
        return expected

    monkeypatch.setattr(metadata_loader.pd, "read_parquet", fake_read_parquet)

    result = metadata_loader._default_csv_reader(str(src))

    assert called["filepath"] == str(src)
    assert result.equals(expected)


def test_run_dimension_copy_into_writes_parquet_and_executes_copy(tmp_path, monkeypatch):
    class DummyConn:
        def __init__(self):
            self.calls = []

        def execute(self, statement, params=None):
            self.calls.append((str(statement), dict(params or {})))

    class DummyBegin:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self.conn

        def __exit__(self, exc_type, exc, tb):
            return False

    class DummyEngine:
        def __init__(self):
            self.conn = DummyConn()

        def begin(self):
            return DummyBegin(self.conn)

    written = {}

    def fake_to_parquet(self, path, index=False):
        written["path"] = str(path)
        written["index"] = index

    monkeypatch.setattr(metadata_loader.pd.DataFrame, "to_parquet", fake_to_parquet)

    engine = DummyEngine()
    incoming = metadata_loader.pd.DataFrame([{"natural_key": "k1", "data_column": "v1"}])
    source_url = metadata_loader.run_dimension_copy_into(
        engine,
        "dummy_dimension",
        destination_table="dbo.stage_dimension",
        copy_source_base_url="https://contoso.dfs.core.windows.net/raw",
        parquet_base_dir=str(tmp_path),
        override_df=incoming,
    )

    assert written["path"].endswith(".parquet")
    assert written["index"] is False
    assert "dimension" in written["path"]
    assert source_url.startswith("https://contoso.dfs.core.windows.net/raw/dimension/")
    assert len(engine.conn.calls) == 1
    sql, params = engine.conn.calls[0]
    assert "COPY INTO dbo.stage_dimension" in sql
    assert "FILE_TYPE = 'PARQUET'" in sql
    assert params["source_url"] == source_url


def test_run_dimension_copy_into_accepts_extra_options(tmp_path, monkeypatch):
    class DummyConn:
        def __init__(self):
            self.calls = []

        def execute(self, statement, params=None):
            self.calls.append((str(statement), dict(params or {})))

    class DummyBegin:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self.conn

        def __exit__(self, exc_type, exc, tb):
            return False

    class DummyEngine:
        def __init__(self):
            self.conn = DummyConn()

        def begin(self):
            return DummyBegin(self.conn)

    monkeypatch.setattr(metadata_loader.pd.DataFrame, "to_parquet", lambda self, path, index=False: None)
    engine = DummyEngine()
    incoming = metadata_loader.pd.DataFrame([{"natural_key": "k1", "data_column": "v1"}])

    metadata_loader.run_dimension_copy_into(
        engine,
        "dummy_dimension",
        destination_table="dbo.stage_dimension",
        copy_source_base_url="https://contoso.dfs.core.windows.net/raw",
        parquet_base_dir=str(tmp_path),
        override_df=incoming,
        copy_into_options=", MAXERRORS = 10",
    )

    sql, _ = engine.conn.calls[0]
    assert "MAXERRORS = 10" in sql


def test_cast_data_columns_uses_default_format(tmp_path):
    metadata_loader.METADATA_REGISTRY.pop("default_format_test", None)


def test_column_comments_include_data_column_comment():
    metadata_loader.METADATA_REGISTRY.pop("comment_test", None)
    metadata_loader.register_metadata(
        "comment_test",
        {
            "target_table": "dimtable",
            "natural_key_cols": ["id"],
            "data_columns": {
                "Asset_Class_Name": {
                    "type": "NVARCHAR(4000)",
                    "comment": "Name comment",
                }
            },
            "surrogate_key": "surrogate",
            "join_numeric_key": "join_key",
            "filepath": "dummy",
        },
    )
    metadata = metadata_loader.get_metadata("comment_test")
    comments = metadata_loader._column_comments(metadata)
    assert comments["Asset_Class_Name"] == "Name comment"
    metadata_loader.METADATA_REGISTRY.pop("comment_test", None)


def test_apply_system_column_comments_executes_script():
    class DummyConn:
        def __init__(self):
            self.calls = []

        def execute(self, statement, params=None):
            self.calls.append((str(statement), dict(params or {})))

    conn = DummyConn()
    column_comments = {"Batch_Id": "B", "Asset_Class_Name": "A"}
    _apply_system_column_comments(conn, "Dimensions", "tbl", column_comments)
    assert len(conn.calls) == 1
    script, params = conn.calls[0]
    assert "MS_Description" in script
    assert "Batch_Id" in script
    assert params["schema"] == "Dimensions"
    assert params["table"] == "tbl"
    assert params.get("comment_0") == "B"
    assert params.get("comment_1") == "A"


def test_apply_table_comments_includes_description():
    class DummyConn:
        def __init__(self):
            self.calls = []

        def execute(self, statement, params=None):
            self.calls.append((str(statement), dict(params or {})))

    conn = DummyConn()
    _apply_table_and_column_comments(
        conn,
        "Dimensions",
        "tbl",
        "Table desc",
        {"Batch_Id": "B"},
    )
    assert len(conn.calls) == 1
    script, params = conn.calls[0]
    assert "Table desc" in script
    assert params["description"] == "Table desc"
    metadata_loader.register_metadata(
        "default_format_test",
        {
            "target_table": "dimtable",
            "natural_key_cols": ["id"],
            "data_columns": {
                "started_at": {
                    "type": "DATETIME2(3)",
                    "nullable": True,
                }
            },
            "surrogate_key": "surrogate",
            "join_numeric_key": "join_key",
            "filepath": "dummy",
        },
    )

    metadata = metadata_loader.get_metadata("default_format_test")
    incoming = metadata_loader.pd.DataFrame(
        {"started_at": ["31/12/2023 14:35", None]}
    )
    casted = metadata_loader._cast_data_columns(incoming, metadata)
    assert metadata_loader.pd.notna(casted["started_at"]).sum() == 1
    metadata_loader.METADATA_REGISTRY.pop("default_format_test", None)
