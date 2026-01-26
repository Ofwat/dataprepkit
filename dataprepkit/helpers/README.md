# Helpers

Utility modules under `dataprepkit.helpers` expose low-level plumbing reused by the metadata-driven loader.

## Connectors

### `helpers/connectors/fabric.py`

* Provides `create_engine_for_fabric(endpoint, database, preferred_driver=None, ...)`.
  * Dynamically chooses the installed ODBC driver.
  * Attaches an MSI token (`notebookutils.credentials.getToken`) for Fabric.
  * Builds the `pyodbc` connection string and wraps it in a SQLAlchemy engine, with pooling settings for long-lived workloads.
* `validate(engine)` runs a lightweight `SELECT 1` to confirm the Fabric connection works before running SCD2.

## Storage

### `helpers/storage.py`

* Integrates with Fabric lakehouses:
  * `mount_lakehouse(workspace, lakehouse, mount_point)`.
  * `LakehouseMount` helper encapsulates mount metadata (paths, URL).
* Examples use these helpers to resolve CSV paths that live inside the lakehouse filesystem before calling `register_metadata`.

## Usage

Examples in `examples/run_metadata_example_fabric.py` and `examples/run_scd2_fabric.py` show how to reuse these helpers when bootstrapping metadata-driven loads against Fabric.
