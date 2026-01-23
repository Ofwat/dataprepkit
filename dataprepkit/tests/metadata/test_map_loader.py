import pytest

from dataprepkit.metadata_loader import METADATA_REGISTRY, get_metadata
from dataprepkit.metadata_map_loader import register_metadata_from_map


def test_register_metadata_from_map(tmp_path):
    sample_csv = tmp_path / "dim.csv"
    sample_csv.write_text("natural_key,data_column\n1,a\n")

    metadata_map = {
        "test_dimension": {
            "insert_update": {
                "join_keys": ["natural_key"],
                "join_numeric_key": "join_numeric_key",
                "surrogate_key": "surrogate_key",
                "data_columns": {"natural_key": {}, "data_column": {}},
            },
            "renames": {},
            "filepath": str(sample_csv),
        }
    }

    register_metadata_from_map(metadata_map)
    meta = get_metadata("test_dimension")

    assert meta.target_table == "test_dimension"
    assert list(meta.data_columns) == ["natural_key", "data_column"]

    # cleanup registry to avoid side effects
    METADATA_REGISTRY.pop("test_dimension", None)
