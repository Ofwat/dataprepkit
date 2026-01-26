from dataprepkit import metadata_loader
from dataprepkit.metadata_loader import DependencyJoin, DimensionMetadata
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
    )

    joined = metadata_loader._apply_dependency_joins(
        incoming, [dependency], engine
    )
    assert joined.loc[joined.Service_Type_Cd == "S1", "Policy_Flag"].iloc[0] == "flag-yes"
    assert metadata_loader.pd.isna(
        joined.loc[joined.Service_Type_Cd == "S2", "Policy_Flag"]
    ).iloc[0]
