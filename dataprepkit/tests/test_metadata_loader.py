from dataprepkit import metadata_loader
from dataprepkit.metadata_loader import DimensionMetadata


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
