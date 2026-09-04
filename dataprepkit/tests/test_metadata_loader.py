from dataprepkit import metadata_loader
from dataprepkit.metadata_loader import (
    CircularDimensionDependencyError,
    ColumnSpec,
    DimensionDependencyError,
    DependencyJoin,
    DimensionMetadata,
    _current_business_key_index_sql,
    _join_numeric_clause,
    _apply_table_and_column_comments,
    _apply_system_column_comments,
    _evolve_table_columns,
    _expected_column_names,
    _post_scd2_validation,
    _surrogate_column_clause,
    build_dimension_dependency_graph,
    build_dimension_dependency_edge_frame,
    resolve_dimension_execution_order,
    run_dimensions_in_dependency_order,
)
import pytest
from sqlalchemy import create_engine, text


def _register_dummy_dimension_metadata() -> None:
    metadata_loader.METADATA_REGISTRY.pop("dummy_dimension", None)
    metadata_loader.register_metadata(
        "dummy_dimension",
        {
            "target_table": "dimension",
            "natural_key_cols": ["natural_key"],
            "data_columns": {"data_column": {"type": "TEXT"}},
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": "unused.csv",
        },
    )


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


def test_register_and_resolve_metadata_with_explicit_registry():
    registry = {}

    metadata_loader.register_metadata(
        "schema_test",
        {
            "target_table": "dimtable",
            "target_schema": "myschema",
            "natural_key_cols": ["id"],
            "data_columns": {"value": {"type": "TEXT"}},
            "surrogate_key": "surrogate",
            "join_numeric_key": "join_key",
            "filepath": "dummy",
        },
        metadata_registry=registry,
    )

    entry = metadata_loader.get_metadata("schema_test", metadata_registry=registry)

    assert entry.target_table == "myschema.dimtable"
    assert "schema_test" not in metadata_loader.METADATA_REGISTRY


def test_register_metadata_target_schema_does_not_override_dependency_schema():
    registry = {}

    metadata_loader.register_metadata(
        "map_measure",
        {
            "target_table": "map_measure",
            "target_schema": "test",
            "natural_key_cols": ["Legacy_BonCode"],
            "data_columns": {
                "Measure_Cd": {"type": "TEXT"},
                "Measure_Instance_Id": {"type": "BIGINT"},
            },
            "surrogate_key": "Map_Measure_Instance_Id",
            "join_numeric_key": "Map_Measure_Id",
            "filepath": "dummy",
            "dependencies": [
                {
                    "table": "dim_measure",
                    "schema": "prod",
                    "on": [{"source": "Measure_Cd", "target": "Measure_Cd"}],
                    "select": {"Measure_Instance_Id": "Measure_Instance_Id"},
                }
            ],
        },
        metadata_registry=registry,
    )

    entry = metadata_loader.get_metadata("map_measure", metadata_registry=registry)

    assert entry.target_table == "test.map_measure"
    assert entry.target_schema == "test"
    assert entry.dependencies[0].schema_name == "prod"


def test_expected_column_names_uses_configured_key_columns():
    metadata = DimensionMetadata(
        name="wrmp_scheme_classification",
        target_table="Dimensions.dim_wrmp_scheme_classification",
        natural_key_cols=["WRMP_Scheme_Classification_Cd"],
        data_columns={
            "Classification": ColumnSpec(type="NVARCHAR(4000)", nullable=True)
        },
        surrogate_key="WRMP_Scheme_Classification_Instance_Id",
        join_numeric_key="WRMP_Scheme_Classification_Id",
        filepath="dummy.xlsx",
    )

    expected = _expected_column_names(metadata)

    assert "WRMP_Scheme_Classification_Instance_Id" in expected
    assert "WRMP_Scheme_Classification_Id" in expected
    assert "surrogate_key" not in expected
    assert "join_numeric_key" not in expected


def test_normalize_value_for_sql_maps_pandas_missing_to_none():
    assert metadata_loader._normalize_value_for_sql(metadata_loader.pd.NA) is None
    assert metadata_loader._normalize_value_for_sql(float("nan")) is None
    assert metadata_loader._normalize_value_for_sql("NA") == "NA"


def test_normalize_value_for_sql_maps_numpy_scalars_to_python_scalars():
    np = pytest.importorskip("numpy")

    assert metadata_loader._normalize_value_for_sql(np.int64(1)) == 1
    assert isinstance(metadata_loader._normalize_value_for_sql(np.int64(1)), int)
    assert metadata_loader._normalize_value_for_sql(np.float64(1.5)) == 1.5


def test_prepare_archive_snapshot_stringifies_mixed_object_columns():
    incoming = metadata_loader.pd.DataFrame(
        {
            "mixed": ["A", 1, None],
            "uniform_text": ["x", "y", None],
            "uniform_int": [1, 2, 3],
        }
    )

    archive_df = metadata_loader._prepare_archive_snapshot(incoming)

    assert archive_df["mixed"].tolist() == ["A", "1", None]
    assert archive_df["uniform_text"].tolist() == ["x", "y", None]
    assert archive_df["uniform_int"].tolist() == [1, 2, 3]


def test_mssql_key_column_clauses_default_to_int():
    class _FakeDialect:
        name = "mssql"

    class _FakeEngine:
        dialect = _FakeDialect()

    engine = _FakeEngine()

    assert _surrogate_column_clause(engine, "dim_id") == "[dim_id] INT IDENTITY(1,1) PRIMARY KEY"
    assert _join_numeric_clause(engine, "join_id") == "[join_id] INT NOT NULL"


def test_current_business_key_index_sql_uses_business_keys_and_current_ind():
    sql = _current_business_key_index_sql(
        "Dimensions.dim_measure",
        ["Measure_Cd", "Region_Cd"],
        "Current_Ind",
    )

    assert (
        sql
        == 'CREATE INDEX ix_dim_measure_Measure_Cd_Region_Cd_Current_Ind ON Dimensions.dim_measure ("Measure_Cd", "Region_Cd", "Current_Ind")'
    )


def test_build_dimension_dependency_graph_orders_registered_dimensions():
    upstream = DimensionMetadata(
        name="dim_region",
        target_table="Dimensions.dim_region",
        natural_key_cols=["Region_Cd"],
        data_columns={"Region_Name": ColumnSpec(type="TEXT", nullable=True)},
        surrogate_key="Region_Instance_Id",
        join_numeric_key="Region_Id",
        filepath="region.xlsx",
    )
    downstream = DimensionMetadata(
        name="dim_scheme",
        target_table="Dimensions.dim_scheme",
        natural_key_cols=["Scheme_Cd"],
        data_columns={"Region_Instance_Id": ColumnSpec(type="BIGINT", nullable=True)},
        surrogate_key="Scheme_Instance_Id",
        join_numeric_key="Scheme_Id",
        filepath="scheme.xlsx",
        dependencies=[
            DependencyJoin(
                table="dim_region",
                schema="Dimensions",
                on=[{"source": "Region_Cd", "target": "Region_Cd"}],
                select={"Region_Instance_Id": "Region_Instance_Id"},
            )
        ],
    )

    graph = build_dimension_dependency_graph(
        {"dim_scheme": downstream, "dim_region": upstream}
    )

    assert graph == {"dim_scheme": {"dim_region"}, "dim_region": set()}
    assert resolve_dimension_execution_order(
        {"dim_scheme": downstream, "dim_region": upstream}
    ) == ["dim_region", "dim_scheme"]


def test_build_dimension_dependency_edge_frame_returns_dependency_edges():
    isolated = DimensionMetadata(
        name="dim_isolated",
        target_table="Dimensions.dim_isolated",
        natural_key_cols=["Isolated_Cd"],
        data_columns={"Name": ColumnSpec(type="TEXT", nullable=True)},
        surrogate_key="Isolated_Instance_Id",
        join_numeric_key="Isolated_Id",
        filepath="isolated.xlsx",
    )
    upstream = DimensionMetadata(
        name="dim_region",
        target_table="Dimensions.dim_region",
        natural_key_cols=["Region_Cd"],
        data_columns={"Region_Name": ColumnSpec(type="TEXT", nullable=True)},
        surrogate_key="Region_Instance_Id",
        join_numeric_key="Region_Id",
        filepath="region.xlsx",
    )
    downstream = DimensionMetadata(
        name="dim_scheme",
        target_table="Dimensions.dim_scheme",
        natural_key_cols=["Scheme_Cd"],
        data_columns={"Region_Instance_Id": ColumnSpec(type="BIGINT", nullable=True)},
        surrogate_key="Scheme_Instance_Id",
        join_numeric_key="Scheme_Id",
        filepath="scheme.xlsx",
        dependencies=[
            DependencyJoin(
                table="dim_region",
                schema="Dimensions",
                on=[{"source": "Region_Cd", "target": "Region_Cd"}],
                select={"Region_Instance_Id": "Region_Instance_Id"},
            )
        ],
    )

    graph = build_dimension_dependency_edge_frame(
        {"dim_scheme": downstream, "dim_region": upstream, "dim_isolated": isolated}
    )

    assert list(graph.columns) == ["source", "target"]
    assert graph.to_dict(orient="records") == [
        {"source": "dim_region", "target": "dim_scheme"},
        {"source": None, "target": "dim_region"},
        {"source": None, "target": "dim_isolated"},
    ]


def test_resolve_dimension_execution_order_ignores_external_dependencies():
    metadata = DimensionMetadata(
        name="dim_scheme",
        target_table="Dimensions.dim_scheme",
        natural_key_cols=["Scheme_Cd"],
        data_columns={"Classification": ColumnSpec(type="TEXT", nullable=True)},
        surrogate_key="Scheme_Instance_Id",
        join_numeric_key="Scheme_Id",
        filepath="scheme.xlsx",
        dependencies=[
            DependencyJoin(
                table="dbo.reference_lookup",
                on=[{"source": "Scheme_Cd", "target": "Scheme_Cd"}],
                select={"Classification": "Classification"},
            )
        ],
    )

    graph = build_dimension_dependency_graph({"dim_scheme": metadata})

    assert graph == {"dim_scheme": set()}
    assert resolve_dimension_execution_order({"dim_scheme": metadata}) == ["dim_scheme"]


def test_resolve_dimension_execution_order_detects_circular_dependencies():
    dim_a = DimensionMetadata(
        name="dim_a",
        target_table="Dimensions.dim_a",
        natural_key_cols=["A_Cd"],
        data_columns={"B_Id": ColumnSpec(type="BIGINT", nullable=True)},
        surrogate_key="A_Instance_Id",
        join_numeric_key="A_Id",
        filepath="a.xlsx",
        dependencies=[
            DependencyJoin(
                table="dim_b",
                schema="Dimensions",
                on=[{"source": "B_Cd", "target": "B_Cd"}],
                select={"B_Id": "B_Id"},
            )
        ],
    )
    dim_b = DimensionMetadata(
        name="dim_b",
        target_table="Dimensions.dim_b",
        natural_key_cols=["B_Cd"],
        data_columns={"A_Id": ColumnSpec(type="BIGINT", nullable=True)},
        surrogate_key="B_Instance_Id",
        join_numeric_key="B_Id",
        filepath="b.xlsx",
        dependencies=[
            DependencyJoin(
                table="dim_a",
                schema="Dimensions",
                on=[{"source": "A_Cd", "target": "A_Cd"}],
                select={"A_Id": "A_Id"},
            )
        ],
    )

    with pytest.raises(CircularDimensionDependencyError, match="dim_a -> dim_b -> dim_a"):
        resolve_dimension_execution_order({"dim_a": dim_a, "dim_b": dim_b})


def test_build_dimension_dependency_graph_rejects_ambiguous_dependency():
    dim_east = DimensionMetadata(
        name="dim_region_east",
        target_table="East.dim_region",
        natural_key_cols=["Region_Cd"],
        data_columns={"Region_Name": ColumnSpec(type="TEXT", nullable=True)},
        surrogate_key="Region_Instance_Id",
        join_numeric_key="Region_Id",
        filepath="east.xlsx",
    )
    dim_west = DimensionMetadata(
        name="dim_region_west",
        target_table="West.dim_region",
        natural_key_cols=["Region_Cd"],
        data_columns={"Region_Name": ColumnSpec(type="TEXT", nullable=True)},
        surrogate_key="Region_Instance_Id",
        join_numeric_key="Region_Id",
        filepath="west.xlsx",
    )
    dim_scheme = DimensionMetadata(
        name="dim_scheme",
        target_table="Dimensions.dim_scheme",
        natural_key_cols=["Scheme_Cd"],
        data_columns={"Region_Instance_Id": ColumnSpec(type="BIGINT", nullable=True)},
        surrogate_key="Scheme_Instance_Id",
        join_numeric_key="Scheme_Id",
        filepath="scheme.xlsx",
        dependencies=[
            DependencyJoin(
                table="dim_region",
                on=[{"source": "Region_Cd", "target": "Region_Cd"}],
                select={"Region_Instance_Id": "Region_Instance_Id"},
            )
        ],
    )

    with pytest.raises(DimensionDependencyError, match="ambiguous"):
        build_dimension_dependency_graph(
            {
                "dim_scheme": dim_scheme,
                "dim_region_east": dim_east,
                "dim_region_west": dim_west,
            }
        )


def test_run_dimensions_in_dependency_order_runs_in_resolved_order(monkeypatch):
    upstream = DimensionMetadata(
        name="dim_region",
        target_table="Dimensions.dim_region",
        natural_key_cols=["Region_Cd"],
        data_columns={"Region_Name": ColumnSpec(type="TEXT", nullable=True)},
        surrogate_key="Region_Instance_Id",
        join_numeric_key="Region_Id",
        filepath="region.xlsx",
    )
    downstream = DimensionMetadata(
        name="dim_scheme",
        target_table="Dimensions.dim_scheme",
        natural_key_cols=["Scheme_Cd"],
        data_columns={"Region_Instance_Id": ColumnSpec(type="BIGINT", nullable=True)},
        surrogate_key="Scheme_Instance_Id",
        join_numeric_key="Scheme_Id",
        filepath="scheme.xlsx",
        dependencies=[
            DependencyJoin(
                table="dim_region",
                schema="Dimensions",
                on=[{"source": "Region_Cd", "target": "Region_Cd"}],
                select={"Region_Instance_Id": "Region_Instance_Id"},
            )
        ],
    )
    engine = object()
    calls = []

    monkeypatch.setattr(
        metadata_loader,
        "run_dimension",
        lambda actual_engine, name, **kwargs: calls.append((actual_engine, name, kwargs)) or name,
    )

    result = run_dimensions_in_dependency_order(
        engine,
        names=["dim_scheme", "dim_region"],
        metadata_registry={"dim_scheme": downstream, "dim_region": upstream},
        staging_use_openrowset_parquet=True,
    )

    assert result == ["dim_region", "dim_scheme"]
    expected_registry = {"dim_scheme": downstream, "dim_region": upstream}
    assert calls == [
        (
            engine,
            "dim_region",
            {
                "metadata_registry": expected_registry,
                "staging_use_openrowset_parquet": True,
            },
        ),
        (
            engine,
            "dim_scheme",
            {
                "metadata_registry": expected_registry,
                "staging_use_openrowset_parquet": True,
            },
        ),
    ]


def test_run_dimensions_in_dependency_order_uses_selected_names(monkeypatch):
    dim_a = DimensionMetadata(
        name="dim_a",
        target_table="Dimensions.dim_a",
        natural_key_cols=["A_Cd"],
        data_columns={"A_Name": ColumnSpec(type="TEXT", nullable=True)},
        surrogate_key="A_Instance_Id",
        join_numeric_key="A_Id",
        filepath="a.xlsx",
    )
    dim_b = DimensionMetadata(
        name="dim_b",
        target_table="Dimensions.dim_b",
        natural_key_cols=["B_Cd"],
        data_columns={"B_Name": ColumnSpec(type="TEXT", nullable=True)},
        surrogate_key="B_Instance_Id",
        join_numeric_key="B_Id",
        filepath="b.xlsx",
    )
    executed = []

    monkeypatch.setattr(
        metadata_loader,
        "run_dimension",
        lambda _engine, name, **_kwargs: executed.append(name) or name,
    )

    result = run_dimensions_in_dependency_order(
        object(),
        names=["dim_b"],
        metadata_registry={"dim_a": dim_a, "dim_b": dim_b},
    )

    assert result == ["dim_b"]
    assert executed == ["dim_b"]


def test_run_dimensions_in_dependency_order_passes_explicit_registry_to_run_dimension(monkeypatch):
    registry = {}
    metadata_loader.register_metadata(
        "dim_a",
        {
            "target_table": "Dimensions.dim_a",
            "natural_key_cols": ["A_Cd"],
            "data_columns": {"A_Name": {"type": "TEXT", "nullable": True}},
            "surrogate_key": "A_Instance_Id",
            "join_numeric_key": "A_Id",
            "filepath": "a.xlsx",
        },
        metadata_registry=registry,
    )

    captured = []
    monkeypatch.setattr(
        metadata_loader,
        "run_dimension",
        lambda _engine, name, **kwargs: captured.append(kwargs["metadata_registry"]) or name,
    )

    result = run_dimensions_in_dependency_order(
        object(),
        metadata_registry=registry,
    )

    assert result == ["dim_a"]
    assert captured == [registry]


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


def test_dependency_join_inner_filters_unmatched_rows():
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
                VALUES ('S1', 1, 'flag-yes')
                """
            )
        )

    incoming = metadata_loader.pd.DataFrame({"Service_Type_Cd": ["S1", "S2"]})
    dependency = DependencyJoin(
        table="dim_service",
        how="inner",
        on=[{"source": "Service_Type_Cd", "target": "Service_Type_Cd"}],
        select={"Policy_Flag": "Policy_Flag"},
        on_missing="null",
    )

    joined = metadata_loader._apply_dependency_joins(incoming, [dependency], engine)

    assert joined.to_dict("records") == [
        {"Service_Type_Cd": "S1", "Policy_Flag": "flag-yes"}
    ]


def test_dependency_join_rejects_duplicate_matches():
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
                    ('S1', 1, 'flag-a'),
                    ('S1', 1, 'flag-b')
                """
            )
        )

    incoming = metadata_loader.pd.DataFrame({"Service_Type_Cd": ["S1"]})
    dependency = DependencyJoin(
        table="dim_service",
        on=[{"source": "Service_Type_Cd", "target": "Service_Type_Cd"}],
        select={"Policy_Flag": "Policy_Flag"},
        on_missing="error",
    )

    with pytest.raises(RuntimeError, match="duplicate matches"):
        metadata_loader._apply_dependency_joins(incoming, [dependency], engine)


def test_dependency_join_parses_schema_qualified_table_names(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    captured = {}

    def fake_read_sql_query(query, con):
        captured["query"] = str(query)
        return metadata_loader.pd.DataFrame(
            [{"Scheme_Cd": "S1", "Classification": "flag-yes"}]
        )

    monkeypatch.setattr(metadata_loader.pd, "read_sql_query", fake_read_sql_query)

    incoming = metadata_loader.pd.DataFrame({"Scheme_Cd": ["S1"]})
    dependency = DependencyJoin(
        table="dbo.reference_lookup",
        on=[{"source": "Scheme_Cd", "target": "Scheme_Cd"}],
        select={"Classification": "Classification"},
        on_missing="null",
    )

    metadata_loader._apply_dependency_joins(incoming, [dependency], engine)

    assert "FROM [dbo].[reference_lookup]" in captured["query"]


def test_dependency_join_error_allows_matched_null_selected_values():
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
                VALUES ('S1', 1, NULL)
                """
            )
        )

    incoming = metadata_loader.pd.DataFrame({"Service_Type_Cd": ["S1"]})
    dependency = DependencyJoin(
        table="dim_service",
        on=[{"source": "Service_Type_Cd", "target": "Service_Type_Cd"}],
        select={"Policy_Flag": "Policy_Flag"},
        on_missing="error",
    )

    joined = metadata_loader._apply_dependency_joins(incoming, [dependency], engine)

    assert joined.to_dict("records") == [
        {"Service_Type_Cd": "S1", "Policy_Flag": None}
    ]


def test_dependency_join_error_reports_missing_source_keys():
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
                VALUES ('S1', 1, 'flag-yes')
                """
            )
        )

    incoming = metadata_loader.pd.DataFrame({"Service_Type_Cd": ["S2"]})
    dependency = DependencyJoin(
        table="dim_service",
        on=[{"source": "Service_Type_Cd", "target": "Service_Type_Cd"}],
        select={"Policy_Flag": "Policy_Flag"},
        on_missing="error",
    )

    with pytest.raises(
        RuntimeError,
        match=r"Missing dependency match in \[dim_service\] for source columns \['Service_Type_Cd'\]",
    ) as exc_info:
        metadata_loader._apply_dependency_joins(incoming, [dependency], engine)

    assert "Required columns ['Policy_Flag'] were null for 1 row(s)." in str(exc_info.value)
    assert "Example missing source keys: [{'Service_Type_Cd': 'S2'}]" in str(exc_info.value)


def test_dependency_join_inner_keeps_matched_rows_with_null_selected_values():
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
                VALUES ('S1', 1, NULL)
                """
            )
        )

    incoming = metadata_loader.pd.DataFrame({"Service_Type_Cd": ["S1", "S2"]})
    dependency = DependencyJoin(
        table="dim_service",
        how="inner",
        on=[{"source": "Service_Type_Cd", "target": "Service_Type_Cd"}],
        select={"Policy_Flag": "Policy_Flag"},
        on_missing="null",
    )

    joined = metadata_loader._apply_dependency_joins(incoming, [dependency], engine)

    assert joined.to_dict("records") == [
        {"Service_Type_Cd": "S1", "Policy_Flag": None}
    ]


def test_dependency_join_normalizes_numeric_like_source_keys_to_match_text_targets():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE dim_interval (
                    Interval_Cd TEXT NOT NULL,
                    Current_Ind INTEGER NOT NULL,
                    Interval_Instance_Id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_interval (Interval_Cd, Current_Ind, Interval_Instance_Id)
                VALUES ('1980', 1, 42)
                """
            )
        )

    incoming = metadata_loader.pd.DataFrame({"Interval_Cd": [1980, "NA"]})
    dependency = DependencyJoin(
        table="dim_interval",
        on=[{"source": "Interval_Cd", "target": "Interval_Cd"}],
        select={"Interval_Instance_Id": "Interval_Instance_Id"},
        on_missing="null",
    )

    joined = metadata_loader._apply_dependency_joins(incoming, [dependency], engine)

    assert joined.loc[0, "Interval_Cd"] == 1980
    assert joined.loc[0, "Interval_Instance_Id"] == 42.0
    assert joined.loc[1, "Interval_Cd"] == "NA"
    assert metadata_loader.pd.isna(joined.loc[1, "Interval_Instance_Id"])


def test_dependency_join_allows_deleted_current_rows():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE dim_service (
                    Service_Type_Cd TEXT NOT NULL,
                    Current_Ind INTEGER NOT NULL,
                    Deleted_Ind INTEGER NOT NULL,
                    Policy_Flag TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_service (Service_Type_Cd, Current_Ind, Deleted_Ind, Policy_Flag)
                VALUES ('S1', 1, 1, 'deleted-current')
                """
            )
        )

    incoming = metadata_loader.pd.DataFrame({"Service_Type_Cd": ["S1"]})
    dependency = DependencyJoin(
        table="dim_service",
        on=[{"source": "Service_Type_Cd", "target": "Service_Type_Cd"}],
        select={"Policy_Flag": "Policy_Flag"},
        on_missing="null",
    )

    joined = metadata_loader._apply_dependency_joins(incoming, [dependency], engine)

    assert joined.to_dict("records") == [
        {"Service_Type_Cd": "S1", "Policy_Flag": "deleted-current"}
    ]


def test_post_scd2_validation_rejects_multiple_current_rows_for_same_key():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE dim_service (
                    Service_Type_Cd TEXT NOT NULL,
                    Current_Ind INTEGER NOT NULL,
                    Deleted_Ind INTEGER NOT NULL,
                    Update_Date TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_service (Service_Type_Cd, Current_Ind, Deleted_Ind, Update_Date)
                VALUES
                    ('S1', 1, 1, '2026-03-18T10:00:00'),
                    ('S1', 1, 0, NULL)
                """
            )
        )

    with pytest.raises(
        RuntimeError,
        match=r"Multiple current rows found for natural key columns \['Service_Type_Cd'\]",
    ) as exc_info:
        _post_scd2_validation(engine, "dim_service", ["Service_Type_Cd"])

    assert "Example duplicate keys: [{'Service_Type_Cd': 'S1'}]" in str(exc_info.value)


def test_post_scd2_validation_rejects_multiple_current_rows_for_same_join_numeric():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE dim_service (
                    Service_Type_Cd TEXT NOT NULL,
                    join_numeric_key INTEGER NOT NULL,
                    Current_Ind INTEGER NOT NULL,
                    Deleted_Ind INTEGER NOT NULL,
                    Update_Date TEXT,
                    Effective_Date_End TEXT NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_service (
                    Service_Type_Cd,
                    join_numeric_key,
                    Current_Ind,
                    Deleted_Ind,
                    Update_Date,
                    Effective_Date_End
                )
                VALUES
                    (
                        'S1',
                        1,
                        1,
                        1,
                        '2026-03-18T10:00:00.000',
                        '2026-03-18T10:00:00.000'
                    ),
                    (
                        'S2',
                        1,
                        1,
                        0,
                        NULL,
                        '9999-12-31T23:59:59.999'
                    )
                """
            )
        )

    with pytest.raises(
        RuntimeError,
        match="Multiple current rows found for join numeric key column 'join_numeric_key'",
    ) as exc_info:
        _post_scd2_validation(
            engine,
            "dim_service",
            ["Service_Type_Cd"],
            "join_numeric_key",
        )

    assert "Example duplicate values: [{'join_numeric_key': 1}]" in str(exc_info.value)


def test_post_scd2_validation_allows_deleted_current_row_with_closed_end_date():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE dim_service (
                    Service_Type_Cd TEXT NOT NULL,
                    Current_Ind INTEGER NOT NULL,
                    Deleted_Ind INTEGER NOT NULL,
                    Update_Date TEXT,
                    Effective_Date_End TEXT NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_service (
                    Service_Type_Cd,
                    Current_Ind,
                    Deleted_Ind,
                    Update_Date,
                    Effective_Date_End
                )
                VALUES ('S1', 1, 1, '2026-03-18T10:00:00.000', '2026-03-18T10:00:00.000')
                """
            )
        )

    _post_scd2_validation(engine, "dim_service", ["Service_Type_Cd"])


def test_post_scd2_validation_treats_case_variants_as_distinct_keys():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE dim_service (
                    Service_Type_Cd TEXT COLLATE NOCASE NOT NULL,
                    Current_Ind INTEGER NOT NULL,
                    Deleted_Ind INTEGER NOT NULL,
                    Update_Date TEXT,
                    Effective_Date_End TEXT NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_service (
                    Service_Type_Cd,
                    Current_Ind,
                    Deleted_Ind,
                    Update_Date,
                    Effective_Date_End
                )
                VALUES
                    ('Ret', 1, 0, NULL, '9999-12-31T23:59:59.999'),
                    ('RET', 1, 0, NULL, '9999-12-31T23:59:59.999')
                """
            )
        )

    _post_scd2_validation(engine, "dim_service", ["Service_Type_Cd"])


def test_run_dimension_matches_system_columns_case_insensitively():
    engine = create_engine("sqlite:///:memory:")
    metadata_name = "case_system_dimension"
    metadata_loader.METADATA_REGISTRY.pop(metadata_name, None)
    metadata_loader.register_metadata(
        metadata_name,
        {
            "target_table": "dim_case_system",
            "natural_key_cols": ["natural_key"],
            "data_columns": {"data_column": {"type": "TEXT"}},
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": "unused.csv",
            "archive_batch_id": "batch1",
        },
    )
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE dim_case_system (
                    surrogate_key INTEGER PRIMARY KEY AUTOINCREMENT,
                    natural_key TEXT NOT NULL,
                    join_numeric_key INTEGER NOT NULL,
                    data_column TEXT,
                    row_hash TEXT NOT NULL,
                    Insert_Date TEXT NOT NULL,
                    Update_Date TEXT,
                    Effective_Date_Start TEXT NOT NULL,
                    Effective_Date_End TEXT NOT NULL,
                    Current_Ind INTEGER NOT NULL,
                    Deleted_Ind INTEGER NOT NULL,
                    batch_id TEXT NOT NULL DEFAULT '',
                    archive_filename TEXT NOT NULL DEFAULT ''
                )
                """
            )
        )

    try:
        metadata_loader.run_dimension(
            engine,
            metadata_name,
            override_df=metadata_loader.pd.DataFrame(
                [{"natural_key": "A1", "data_column": "Alpha"}]
            ),
        )

        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT batch_id, archive_filename
                    FROM dim_case_system
                    WHERE Current_Ind = 1
                    """
                )
            ).mappings().one()

        assert row["batch_id"] == "batch1"
        assert row["archive_filename"] == "unused.csv"
    finally:
        metadata_loader.METADATA_REGISTRY.pop(metadata_name, None)


def test_evolve_table_columns_backfills_effective_dates_for_existing_rows():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE dim_service (
                    Service_Type_Cd TEXT NOT NULL,
                    Service_Name TEXT,
                    surrogate_key INTEGER PRIMARY KEY,
                    join_numeric_key INTEGER NOT NULL,
                    row_hash TEXT NOT NULL,
                    Insert_Date TEXT NOT NULL,
                    Update_Date TEXT,
                    Current_Ind INTEGER NOT NULL,
                    Deleted_Ind INTEGER NOT NULL,
                    Batch_Id TEXT NOT NULL,
                    Archive_Filename TEXT NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_service (
                    Service_Type_Cd,
                    Service_Name,
                    surrogate_key,
                    join_numeric_key,
                    row_hash,
                    Insert_Date,
                    Update_Date,
                    Current_Ind,
                    Deleted_Ind,
                    Batch_Id,
                    Archive_Filename
                )
                VALUES
                    ('S1', 'Active', 1, 10, 'h1', '2026-03-18T10:00:00.000', NULL, 1, 0, 'b1', 'a1'),
                    ('S2', 'Historic', 2, 20, 'h2', '2026-03-17T10:00:00.000', '2026-03-18T09:00:00.000', 0, 0, 'b1', 'a1')
                """
            )
        )

    metadata = DimensionMetadata(
        name="dim_service",
        target_table="dim_service",
        natural_key_cols=["Service_Type_Cd"],
        data_columns={"Service_Name": ColumnSpec(type="TEXT", nullable=True)},
        surrogate_key="surrogate_key",
        join_numeric_key="join_numeric_key",
        filepath="dummy.csv",
    )

    _evolve_table_columns(
        engine,
        metadata,
        {
            metadata_loader.DEFAULT_SYSTEM_COLUMNS["effective_date_start"],
            metadata_loader.DEFAULT_SYSTEM_COLUMNS["effective_date_end"],
        },
    )

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT Service_Type_Cd, Effective_Date_Start, Effective_Date_End
                FROM dim_service
                ORDER BY Service_Type_Cd
                """
            )
        ).fetchall()

    assert rows == [
        ("S1", "2026-03-18T10:00:00.000", metadata_loader.EFFECTIVE_DATE_MAX),
        ("S2", "2026-03-17T10:00:00.000", "2026-03-18T09:00:00.000"),
    ]


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


def test_run_dimension_archives_raw_input_snapshot_before_dependency_joins(tmp_path, monkeypatch):
    registry = {}
    metadata_loader.register_metadata(
        "archive_snapshot_test",
        {
            "target_table": "dimension",
            "natural_key_cols": ["natural_key"],
            "data_columns": {"data_column": {"type": "TEXT"}},
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": "unused.csv",
            "dependencies": [
                {
                    "table": "dep_dim",
                    "on": [{"source": "dep_key", "target": "dep_key"}],
                    "select": {"joined_value": "joined_value"},
                    "on_missing": "null",
                }
            ],
        },
        metadata_registry=registry,
        archive_base_dir=str(tmp_path / "archive"),
        archive_batch_id="batch1",
    )

    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE dep_dim (
                    dep_key TEXT NOT NULL,
                    Current_Ind INTEGER NOT NULL,
                    joined_value TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dep_dim (dep_key, Current_Ind, joined_value)
                VALUES ('D1', 1, 'joined')
                """
            )
        )

    raw_input = metadata_loader.pd.DataFrame(
        [
            {
                "natural_key": "k1",
                "data_column": "v1",
                "dep_key": "D1",
                "raw_extra": "keep-me",
            }
        ]
    )
    captured = {}

    monkeypatch.setattr(metadata_loader, "_ensure_target_table", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(metadata_loader, "_apply_table_description", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(metadata_loader, "_post_scd2_validation", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        metadata_loader,
        "_get_target_columns",
        lambda *_args, **_kwargs: {
            "data_column",
            metadata_loader.DEFAULT_SYSTEM_COLUMNS["batch_id"],
            metadata_loader.DEFAULT_SYSTEM_COLUMNS["archive_filename"],
        },
    )
    monkeypatch.setattr(metadata_loader, "apply_changes", lambda **_kwargs: True)
    monkeypatch.setattr(
        metadata_loader,
        "_archive_snapshot",
        lambda incoming, _metadata, _path: captured.setdefault("archived", incoming.copy()),
    )

    metadata_loader.run_dimension(
        engine,
        "archive_snapshot_test",
        metadata_registry=registry,
        override_df=raw_input,
    )

    archived = captured["archived"]
    assert list(archived.columns) == ["natural_key", "data_column", "dep_key", "raw_extra"]
    assert archived.iloc[0].to_dict() == {
        "natural_key": "k1",
        "data_column": "v1",
        "dep_key": "D1",
        "raw_extra": "keep-me",
    }
    assert "joined_value" not in archived.columns


def test_cast_data_columns_parses_datetime():
    metadata_loader.METADATA_REGISTRY.pop("cast_test", None)
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


def test_default_csv_reader_trims_csv_column_names(tmp_path):
    src = tmp_path / "data_trimmed_headers.csv"
    src.write_text(" col , num \n a ,1\n b ,2\n", encoding="utf-8")

    result = metadata_loader._default_csv_reader(str(src))

    assert list(result.columns) == ["col", "num"]


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


def test_default_csv_reader_preserves_excel_missing_cells_as_nulls(tmp_path):
    try:
        import openpyxl  # noqa: F401
    except ImportError:
        pytest.skip("openpyxl is required to test Excel input")

    data = metadata_loader.pd.DataFrame({"col": ["x", None], "num": [1, 2]})
    src = tmp_path / "data_missing.xlsx"
    data.to_excel(src, index=False)

    result = metadata_loader._default_csv_reader(str(src))

    assert result.loc[0, "col"] == "x"
    assert metadata_loader.pd.isna(result.loc[1, "col"])


def test_default_csv_reader_trims_excel_column_names(tmp_path):
    try:
        import openpyxl  # noqa: F401
    except ImportError:
        pytest.skip("openpyxl is required to test Excel input")

    data = metadata_loader.pd.DataFrame({" col ": ["x"], " num ": [1]})
    src = tmp_path / "data_trimmed_headers.xlsx"
    data.to_excel(src, index=False)

    result = metadata_loader._default_csv_reader(str(src))

    assert list(result.columns) == ["col", "num"]


def test_default_csv_reader_drops_fully_blank_excel_rows(tmp_path):
    try:
        import openpyxl  # noqa: F401
    except ImportError:
        pytest.skip("openpyxl is required to test Excel input")

    data = metadata_loader.pd.DataFrame(
        {
            "col": ["x", None, None, "y"],
            "num": [1, None, None, 2],
        }
    )
    src = tmp_path / "data_with_blank_rows.xlsx"
    data.to_excel(src, index=False)

    result = metadata_loader._default_csv_reader(str(src))

    assert result.to_dict("records") == [
        {"col": "x", "num": 1},
        {"col": "y", "num": 2},
    ]


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


def test_default_csv_reader_trims_parquet_column_names(tmp_path, monkeypatch):
    src = tmp_path / "data_trimmed_headers.parquet"
    src.touch()
    expected = metadata_loader.pd.DataFrame({" col ": ["p1"], " num ": [2]})

    monkeypatch.setattr(metadata_loader.pd, "read_parquet", lambda _filepath: expected)

    result = metadata_loader._default_csv_reader(str(src))

    assert list(result.columns) == ["col", "num"]


def test_default_csv_reader_rejects_duplicate_column_names_after_trim(tmp_path):
    src = tmp_path / "duplicate_headers.csv"
    src.write_text("col, col \n1,2\n", encoding="utf-8")

    with pytest.raises(ValueError, match="duplicate column names after trimming"):
        metadata_loader._default_csv_reader(str(src))


def test_run_dimension_copy_into_writes_parquet_and_executes_copy(tmp_path, monkeypatch):
    _register_dummy_dimension_metadata()
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
    metadata_loader.METADATA_REGISTRY.pop("dummy_dimension", None)


def test_run_dimension_copy_into_accepts_extra_options(tmp_path, monkeypatch):
    _register_dummy_dimension_metadata()
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
    metadata_loader.METADATA_REGISTRY.pop("dummy_dimension", None)


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


def test_cast_data_columns_falls_back_to_iso_datetime_strings():
    metadata_loader.METADATA_REGISTRY.pop("iso_format_test", None)
    metadata_loader.register_metadata(
        "iso_format_test",
        {
            "target_table": "dimtable",
            "natural_key_cols": ["id"],
            "data_columns": {
                "ended_at": {
                    "type": "DATETIME2(3)",
                    "nullable": True,
                }
            },
            "surrogate_key": "surrogate",
            "join_numeric_key": "join_key",
            "filepath": "dummy",
        },
    )

    metadata = metadata_loader.get_metadata("iso_format_test")
    incoming = metadata_loader.pd.DataFrame(
        {"ended_at": ["1980-12-31 00:00:00.000", None]}
    )

    casted = metadata_loader._cast_data_columns(incoming, metadata)

    assert casted.loc[0, "ended_at"] == metadata_loader.pd.Timestamp("1980-12-31 00:00:00")
    assert metadata_loader.pd.isna(casted.loc[1, "ended_at"])
    metadata_loader.METADATA_REGISTRY.pop("iso_format_test", None)


def test_cast_data_columns_falls_back_to_additional_iso_datetime_strings():
    metadata_loader.METADATA_REGISTRY.pop("iso_format_test_2", None)
    metadata_loader.register_metadata(
        "iso_format_test_2",
        {
            "target_table": "dimtable",
            "natural_key_cols": ["id"],
            "data_columns": {
                "ended_at": {
                    "type": "DATETIME2(3)",
                    "nullable": True,
                }
            },
            "surrogate_key": "surrogate",
            "join_numeric_key": "join_key",
            "filepath": "dummy",
        },
    )

    metadata = metadata_loader.get_metadata("iso_format_test_2")
    incoming = metadata_loader.pd.DataFrame(
        {"ended_at": ["2014-04-01 00:00:00.000", None]}
    )

    casted = metadata_loader._cast_data_columns(incoming, metadata)

    assert casted.loc[0, "ended_at"] == metadata_loader.pd.Timestamp("2014-04-01 00:00:00")
    assert metadata_loader.pd.isna(casted.loc[1, "ended_at"])
    metadata_loader.METADATA_REGISTRY.pop("iso_format_test_2", None)


def test_cast_data_columns_accepts_out_of_bounds_datetime_strings():
    metadata_loader.METADATA_REGISTRY.pop("out_of_bounds_datetime_test", None)
    metadata_loader.register_metadata(
        "out_of_bounds_datetime_test",
        {
            "target_table": "dimtable",
            "natural_key_cols": ["id"],
            "data_columns": {
                "Interval_Start_Date": {
                    "type": "DATETIME2(3)",
                    "nullable": True,
                }
            },
            "surrogate_key": "surrogate",
            "join_numeric_key": "join_key",
            "filepath": "dummy",
        },
    )

    metadata = metadata_loader.get_metadata("out_of_bounds_datetime_test")
    incoming = metadata_loader.pd.DataFrame(
        {"Interval_Start_Date": ["31/12/2999 00:00", None]}
    )

    casted = metadata_loader._cast_data_columns(incoming, metadata)

    assert casted.loc[0, "Interval_Start_Date"] == "2999-12-31T00:00:00.000"
    assert casted.loc[1, "Interval_Start_Date"] is None
    metadata_loader.METADATA_REGISTRY.pop("out_of_bounds_datetime_test", None)


def test_cast_data_columns_accepts_out_of_bounds_datetime_strings_with_seconds():
    metadata_loader.METADATA_REGISTRY.pop("out_of_bounds_datetime_seconds_test", None)
    metadata_loader.register_metadata(
        "out_of_bounds_datetime_seconds_test",
        {
            "target_table": "dimtable",
            "natural_key_cols": ["id"],
            "data_columns": {
                "Interval_End_Date": {
                    "type": "DATETIME2(3)",
                    "nullable": True,
                }
            },
            "surrogate_key": "surrogate",
            "join_numeric_key": "join_key",
            "filepath": "dummy",
        },
    )

    metadata = metadata_loader.get_metadata("out_of_bounds_datetime_seconds_test")
    incoming = metadata_loader.pd.DataFrame(
        {"Interval_End_Date": ["31/12/2999 23:59:59", None]}
    )

    casted = metadata_loader._cast_data_columns(incoming, metadata)

    assert casted.loc[0, "Interval_End_Date"] == "2999-12-31T23:59:59.000"
    assert casted.loc[1, "Interval_End_Date"] is None
    metadata_loader.METADATA_REGISTRY.pop("out_of_bounds_datetime_seconds_test", None)


def test_cast_data_columns_raises_for_invalid_float_values():
    metadata_loader.METADATA_REGISTRY.pop("float_cast_test", None)
    metadata_loader.register_metadata(
        "float_cast_test",
        {
            "target_table": "dimtable",
            "natural_key_cols": ["id"],
            "data_columns": {
                "measure_value": {
                    "type": "REAL",
                    "nullable": True,
                }
            },
            "surrogate_key": "surrogate",
            "join_numeric_key": "join_key",
            "filepath": "dummy",
        },
    )

    metadata = metadata_loader.get_metadata("float_cast_test")
    incoming = metadata_loader.pd.DataFrame({"measure_value": ["not-a-float", None]})

    with pytest.raises(
        ValueError,
        match=r"Column 'measure_value' contains value\(s\) incompatible with target type REAL",
    ):
        metadata_loader._cast_data_columns(incoming, metadata)

    metadata_loader.METADATA_REGISTRY.pop("float_cast_test", None)


def test_cast_data_columns_raises_for_non_integral_integer_values():
    metadata_loader.METADATA_REGISTRY.pop("integer_cast_test", None)
    metadata_loader.register_metadata(
        "integer_cast_test",
        {
            "target_table": "dimtable",
            "natural_key_cols": ["id"],
            "data_columns": {
                "measure_id": {
                    "type": "BIGINT",
                    "nullable": True,
                }
            },
            "surrogate_key": "surrogate",
            "join_numeric_key": "join_key",
            "filepath": "dummy",
        },
    )

    metadata = metadata_loader.get_metadata("integer_cast_test")
    incoming = metadata_loader.pd.DataFrame({"measure_id": [1.5, None]})

    with pytest.raises(
        ValueError,
        match=r"Column 'measure_id' contains value\(s\) incompatible with target type BIGINT",
    ):
        metadata_loader._cast_data_columns(incoming, metadata)

    metadata_loader.METADATA_REGISTRY.pop("integer_cast_test", None)


def test_cast_data_columns_raises_for_invalid_boolean_values():
    metadata_loader.METADATA_REGISTRY.pop("boolean_cast_test", None)
    metadata_loader.register_metadata(
        "boolean_cast_test",
        {
            "target_table": "dimtable",
            "natural_key_cols": ["id"],
            "data_columns": {
                "is_active": {
                    "type": "BIT",
                    "nullable": True,
                }
            },
            "surrogate_key": "surrogate",
            "join_numeric_key": "join_key",
            "filepath": "dummy",
        },
    )

    metadata = metadata_loader.get_metadata("boolean_cast_test")
    incoming = metadata_loader.pd.DataFrame({"is_active": ["maybe", None]})

    with pytest.raises(
        ValueError,
        match=r"Column 'is_active' contains value\(s\) incompatible with target type BIT",
    ):
        metadata_loader._cast_data_columns(incoming, metadata)

    metadata_loader.METADATA_REGISTRY.pop("boolean_cast_test", None)


def test_cast_data_columns_accepts_pandas_boolean_values():
    metadata_loader.METADATA_REGISTRY.pop("boolean_cast_valid_test", None)
    metadata_loader.register_metadata(
        "boolean_cast_valid_test",
        {
            "target_table": "dimtable",
            "natural_key_cols": ["id"],
            "data_columns": {
                "is_active": {
                    "type": "BIT",
                    "nullable": True,
                }
            },
            "surrogate_key": "surrogate",
            "join_numeric_key": "join_key",
            "filepath": "dummy",
        },
    )

    metadata = metadata_loader.get_metadata("boolean_cast_valid_test")
    incoming = metadata_loader.pd.DataFrame(
        {
            "is_active": metadata_loader.pd.Series(
                [True, False, None],
                dtype="boolean",
            )
        }
    )

    casted = metadata_loader._cast_data_columns(incoming, metadata)

    assert casted["is_active"].tolist() == [True, False, metadata_loader.pd.NA]
    metadata_loader.METADATA_REGISTRY.pop("boolean_cast_valid_test", None)


def test_cast_data_columns_raises_for_invalid_uuid_values():
    metadata_loader.METADATA_REGISTRY.pop("uuid_cast_test", None)
    metadata_loader.register_metadata(
        "uuid_cast_test",
        {
            "target_table": "dimtable",
            "natural_key_cols": ["id"],
            "data_columns": {
                "entity_id": {
                    "type": "UNIQUEIDENTIFIER",
                    "nullable": True,
                }
            },
            "surrogate_key": "surrogate",
            "join_numeric_key": "join_key",
            "filepath": "dummy",
        },
    )

    metadata = metadata_loader.get_metadata("uuid_cast_test")
    incoming = metadata_loader.pd.DataFrame({"entity_id": ["not-a-guid", None]})

    with pytest.raises(
        ValueError,
        match=r"Column 'entity_id' contains value\(s\) incompatible with target type UNIQUEIDENTIFIER",
    ):
        metadata_loader._cast_data_columns(incoming, metadata)

    metadata_loader.METADATA_REGISTRY.pop("uuid_cast_test", None)


def test_run_dimension_raises_before_scd2_for_invalid_float_values(monkeypatch):
    registry = {}
    metadata_loader.register_metadata(
        "invalid_float_dimension",
        {
            "target_table": "dimension",
            "natural_key_cols": ["natural_key"],
            "data_columns": {
                "measure_value": {
                    "type": "REAL",
                    "nullable": True,
                }
            },
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": "unused.csv",
        },
        metadata_registry=registry,
    )

    engine = create_engine("sqlite:///:memory:")
    apply_changes_called = False

    def _unexpected_apply_changes(**_kwargs):
        nonlocal apply_changes_called
        apply_changes_called = True
        return True

    monkeypatch.setattr(metadata_loader, "apply_changes", _unexpected_apply_changes)

    with pytest.raises(
        ValueError,
        match=r"Column 'measure_value' contains value\(s\) incompatible with target type REAL",
    ):
        metadata_loader.run_dimension(
            engine,
            "invalid_float_dimension",
            metadata_registry=registry,
            override_df=metadata_loader.pd.DataFrame(
                [{"natural_key": "A1", "measure_value": "not-a-float"}]
            ),
        )

    assert apply_changes_called is False
