from __future__ import annotations

from pathlib import Path

from dataprepkit.helpers.connectors.fabric import create_engine_for_fabric, validate
from dataprepkit.metadata_loader import register_metadata, run_dimension


def _resolve_base_dir() -> Path:
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:
        return Path.cwd()


FABRIC_ENDPOINT = "byx2sqtktgzedbish3jdpk4dcm-qybek6cxp2yulbokgc3c6aie5u.database.fabric.microsoft.com"
FABRIC_DATABASE = "mydb-8be33c12-255a-43ff-bead-2fbe027bf1ed"
FABRIC_LAKEHOUSE_PATH = "/lakehouse/data/dimensions.csv"


METADATA_MAP = [
    {
        "name": "fabric_demo_dimension",
        "metadata": {
            "target_table": "fabric_dim_demo",
            "natural_key_cols": ["natural_key"],
            "data_columns": {
                "data_column": {"type": "NVARCHAR(4000)"},
                "source_system": {"type": "NVARCHAR(4000)", "nullable": True},
            },
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": FABRIC_LAKEHOUSE_PATH,
            "description": "Fabric demo dimension loaded purely from lakehouse CSV",
        },
    },
    {
        "name": "fabric_customer_dimension",
        "metadata": {
            "target_table": "fabric_dim_customer",
            "natural_key_cols": ["natural_key"],
            "natural_key_specs": {
                "natural_key": {"type": "NVARCHAR(4000)", "nullable": False},
            },
            "data_columns": {
                "customer_name": {"type": "NVARCHAR(4000)"},
                "source_system": {"type": "NVARCHAR(4000)", "nullable": True},
            },
            "surrogate_key": "surrogate_key",
            "join_numeric_key": "join_numeric_key",
            "filepath": FABRIC_LAKEHOUSE_PATH,
            "description": "Fabric customer dimension referenced from the same CSV",
        },
    },
]


def _build_engine():
    engine = create_engine_for_fabric(
        FABRIC_ENDPOINT,
        FABRIC_DATABASE,
        preferred_driver="ODBC Driver 18 for SQL Server",
    )
    if not validate(engine):
        raise RuntimeError("Fabric connection test failed.")
    return engine


def _register_metadata():
    for entry in METADATA_MAP:
        register_metadata(entry["name"], entry["metadata"])


def _lake_csv_reader(filepath: str) -> pd.DataFrame:
    return pd.read_csv(
        filepath,
        header=0,
        encoding="utf-8",
        dtype=str,
        keep_default_na=False,
        na_values=["NULL"],
    )


def main():
    engine = _build_engine()
    _register_metadata()
    for entry in METADATA_MAP:
        name = entry["name"]
        table = entry["metadata"]["target_table"]
        run_dimension(engine, name, csv_reader=_lake_csv_reader)
        print(f"Loaded dimension {name} into {table}")


if __name__ == "__main__":
    main()
