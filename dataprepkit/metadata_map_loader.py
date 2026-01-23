"""Bridge between legacy metadata maps and the dataprepkit metadata registry."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from dataprepkit.metadata_loader import register_metadata


EXAMPLE_METADATA_MAP: Mapping[str, Any] = {
    "tbl_d_assurance": {
        "insert_update": {
            "join_keys": ["Assurance_Cd"],
            "join_numeric_key": "Assurance_Id",
            "surrogate_key": "Assurance_Instance_Id",
            "data_columns": {
                "Assurance_Cd": {"type": "TEXT", "unique": False, "nullable": False},
                "Assurance_Level": {"type": "TEXT"},
                "Assurance_Definition": {"type": "TEXT"},
                "Batch_Id": {"type": "TEXT", "nullable": False, "unique": False},
                "Effective_Start_Date": {"type": "DATETIME"},
                "Effective_End_Date": {"type": "DATETIME"},
            },
            "processing_class": None,
            "dependency_tables": [],
        },
        "expected_columns": {"Assurance_Cd", "Assurance_Level", "Assurance_Definition"},
        "renames": {},
        "filepath": "examples/assurance_dim.csv",
    }
}


def register_metadata_from_map(
    metadata_map: Mapping[str, Mapping[str, Any]],
    *,
    root_dir: str | Path | None = None,
) -> None:
    root = Path(root_dir) if root_dir else None
    for name, payload in metadata_map.items():
        insert_update = payload.get("insert_update") or {}
        data_columns = insert_update.get("data_columns", {})
        filepath = payload.get("filepath")
        if root and filepath:
            filepath = str((root / filepath).resolve())
        register_metadata(
            name,
            {
                "target_table": name,
                "natural_key_cols": insert_update.get("join_keys", []),
                "data_columns": data_columns,
                "surrogate_key": insert_update.get("surrogate_key"),
                "join_numeric_key": insert_update.get("join_numeric_key"),
                "filepath": filepath or "",
                "column_renames": payload.get("renames", {}),
            },
        )
