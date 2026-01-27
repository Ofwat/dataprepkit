import os
from pathlib import Path

import pytest

from dataprepkit.storage import ArchivePath, archive_dataframe_path


def test_archive_dataframe_path_creates_folder(tmp_path):
    result = archive_dataframe_path("tbl_d_region", "42", str(tmp_path))
    assert isinstance(result, ArchivePath)
    assert result.table == "tbl_d_region"
    assert "BATCH42" in result.file_path
    assert result.file_path.endswith(".parquet")
    assert os.path.exists(result.file_path) is False
    assert os.path.basename(Path(result.file_path).parent) == "tbl_d_region"


@pytest.mark.parametrize(
    ("table_name", "batch_id", "base_dir"),
    [
        ("", "1", "/tmp"),
        ("table", "", "/tmp"),
        ("table", "1", ""),
    ],
)
def test_archive_dataframe_path_validates_inputs(table_name, batch_id, base_dir):
    with pytest.raises(ValueError):
        archive_dataframe_path(table_name, batch_id, base_dir)
