import pytest
from dataprepkit.helpers.transforms.insert_update import _parse_qualified_table

def test_parse_qualified_table_valid():
    assert _parse_qualified_table("[myschema].[mytable]") == ("myschema", "mytable")
    assert _parse_qualified_table("[mytable]") == (None, "mytable")
    assert _parse_qualified_table("mytable") == (None, "mytable")

def test_parse_qualified_table_invalid():
    with pytest.raises(ValueError, match="not in the format"):
        _parse_qualified_table("myschema.mytable")  # missing brackets
    with pytest.raises(ValueError, match="not in the format"):
        _parse_qualified_table("")  # empty string
