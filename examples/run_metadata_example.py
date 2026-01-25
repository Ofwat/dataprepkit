"""
Example that exercises the metadata-driven orchestrator end-to-end.

Create target tables, register metadata, and call `run_dimension` to invoke
the new SQL-backed SCD2 logic, schema evolution, run policies, dependency joins,
and snapshot archiving across multiple dimensions.
"""

from pathlib import Path
from typing import Iterable
import logging

import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine, text

from dataprepkit.metadata_loader import register_metadata, run_dimension


METADATA_MAP = {
    "tbl_d_assurance": {
        "natural_key_cols": ["Assurance_Cd"],
        "natural_key_defs": {
            "Assurance_Cd": {"type": "TEXT", "nullable": False},
        },
        "data_columns": {
            "Assurance_Level": {"type": "TEXT"},
            "Assurance_Definition": {"type": "TEXT"},
        },
        "surrogate_key": "Assurance_Instance_Id",
        "join_numeric_key": "Assurance_Id",
        "processing_class": lambda df: df.assign(
            Assurance_Level=df.Assurance_Level.str.upper()
        ),
        "filepath": "tbl_d_assurance.csv",
        "input_format": "csv",
    },
    "tbl_d_business_type": {
        "natural_key_cols": ["Business_Type_Cd"],
        "natural_key_defs": {
            "Business_Type_Cd": {"type": "TEXT", "nullable": False},
        },
        "data_columns": {
            "Business_Type": {"type": "TEXT"},
            "Business_Type_Desc": {"type": "TEXT"},
        },
        "surrogate_key": "Business_Type_Instance_Id",
        "join_numeric_key": "Business_Type_Id",
        "processing_class": lambda df: df.assign(
            Business_Type=df.Business_Type.str.title()
        ),
        "filepath": "tbl_d_business_type.csv",
        "input_format": "csv",
    },
}

SNAPSHOTS = {
    "tbl_d_assurance": [
        DataFrame(
            [
                {
                    "Assurance_Cd": "A1",
                    "Assurance_Level": "high",
                    "Assurance_Definition": "First level",
                },
                {
                    "Assurance_Cd": "A2",
                    "Assurance_Level": "medium",
                    "Assurance_Definition": "Second level",
                },
            ]
        ),
        DataFrame(
            [
                {
                    "Assurance_Cd": "A1",
                    "Assurance_Level": "high",
                    "Assurance_Definition": "First level updated",
                },
                {
                    "Assurance_Cd": "A3",
                    "Assurance_Level": "low",
                    "Assurance_Definition": "New tier",
                },
            ]
        ),
    ],
    "tbl_d_business_type": [
        DataFrame(
            [
                {
                    "Business_Type_Cd": "B1",
                    "Business_Type": "Corporate",
                    "Business_Type_Desc": "Mainline",
                },
                {
                    "Business_Type_Cd": "B2",
                    "Business_Type": "Wholesale",
                    "Business_Type_Desc": "Partners",
                },
            ]
        ),
        DataFrame(
            [
                {
                    "Business_Type_Cd": "B1",
                    "Business_Type": "Corporate",
                    "Business_Type_Desc": "Enterprise",
                },
                {
                    "Business_Type_Cd": "B3",
                    "Business_Type": "Retail",
                    "Business_Type_Desc": "Direct",
                },
            ]
        ),
    ],
}


def _register_metadata(name: str, entry: dict[str, object], base_dir: Path) -> None:
    register_metadata(
        name=name,
        metadata={
            "target_table": name,
            "natural_key_cols": entry["natural_key_cols"],
            "data_columns": entry["data_columns"],
            "surrogate_key": entry["surrogate_key"],
            "join_numeric_key": entry["join_numeric_key"],
            "natural_key_specs": entry.get("natural_key_defs", {}),
            "filepath": str(base_dir / "examples" / entry["filepath"]),
            "schema_handling": {"mode": "evolve"},
            "processing_class": entry["processing_class"],
            "run_policy": {"on_table_failure": "continue"},
            "archive_path": str(base_dir / "examples" / "archives"),
        },
    )


_LOGGER = logging.getLogger(__name__)


def _write_snapshot(entry: dict[str, object], snapshot: DataFrame, base_dir: Path) -> Path:
    input_format = entry.get("input_format", "csv")
    filepath = base_dir / "examples" / entry["filepath"]
    if input_format == "csv":
        _LOGGER.info("persisting snapshot for %s to %s", entry["filepath"], filepath)
        snapshot.to_csv(filepath, index=False)
        return filepath
    raise NotImplementedError(f"Input format {input_format!r} is not implemented yet.")


def _run_snapshots(
    engine,
    table_name: str,
    snapshots: Iterable[DataFrame],
    entry: dict[str, object],
    base_dir: Path,
) -> None:
    for idx, snapshot in enumerate(snapshots, 1):
        stage_label = "initial load" if idx == 1 else f"update {idx - 1}"
        print(f"\n=== {table_name} {stage_label} ===")
        _LOGGER.debug("running %s against %s from snapshot stage %s", table_name, entry["filepath"], stage_label)
        _write_snapshot(entry, snapshot, base_dir)
        run_dimension(engine, table_name)
        with engine.connect() as conn:
            count = conn.execute(text(f"SELECT COUNT(1) FROM {table_name}")).scalar()
        print(f"row count for {table_name}: {count}")


def main():
    engine = create_engine("sqlite:///:memory:")

    try:
        base_dir = Path(__file__).resolve().parents[1]
    except NameError:
        base_dir = Path.cwd()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    for table_name, metadata in METADATA_MAP.items():
        _LOGGER.info("registering metadata for %s", table_name)
        _register_metadata(table_name, metadata, base_dir)
        _run_snapshots(engine, table_name, SNAPSHOTS[table_name], metadata, base_dir)


if __name__ == "__main__":
    main()
