# dataprepkit metadata-driven SCD2 orchestrator

`dataprepkit` turns CSV snapshots into evolving dimensions by declaring the schema, keys, and dependency joins in metadata. The loader and `scd2.apply_changes` take care of schema evolution, dependency joins, normalizing datetimes, and the SCD2 workflow.

## Metadata format

Each metadata entry registered via `register_metadata(name, metadata)` must include:

| Key | Description |
| --- | --- |
| `target_table` | Table name (schemaless or schema-qualified; `target_schema` can fill in the schema). |
| `target_schema` | Optional schema name. The loader auto-prefixes it during registration and creates the schema if missing. |
| `natural_key_cols` | List of natural key columns—required. |
| `natural_key_specs` | Optional overrides (`ColumnSpec`) for natural keys (type, nullable, unique, default). |
| `data_columns` | Map of column names → `ColumnSpec`. Required. |
| `surrogate_key` / `join_numeric_key` | System column names. |
| `filepath` | CSV path (or lakehouse path) to read. |
| `schema_handling.mode` | `"suggest"` (default) or `"evolve"` (auto-add missing columns). |
| `processing_class` | Optional callable that transforms the incoming pandas DataFrame. |
| `dependencies` | Optional dependency definitions (see below). |
| `run_policy` | Determines failure handling (`continue` or `abort`). |
| `archive_path` | Optional location for parquet archives. |

`ColumnSpec` fields:
* `type`: SQL type (e.g., `NVARCHAR(4000)`, `DATETIME2(3)`).
* `nullable`: boolean.
* `unique`: boolean flag.
* `default`: SQL default expression.
* `parse_format`: optional format string for datetime parsing before staging.

Reserved `NA` convention:
* Any row whose natural key columns are all `"NA"` is treated as the reserved `NA` member automatically.
* The reserved `NA` member always uses `-1` for both the surrogate key and the join numeric key.
* If multiple `NA` rows appear for the same dimension snapshot, the load fails.
* The reserved `NA` member does not participate in SCD2 history; it is stored as a single latest-value row and overwritten in place.

Example:

```json
{
  "schema_handling": {"mode": "evolve"},
  "target_table": "tbl_d_company",
  "target_schema": "Dimensions",
  "natural_key_cols": ["Organisation_Cd"],
  "data_columns": {
    "Legacy_Company_Name": {"type": "NVARCHAR(4000)", "nullable": true},
    "New_Column_From_Join": {"type": "NVARCHAR(4000)", "nullable": true}
  },
  "surrogate_key": "surrogate_key",
  "join_numeric_key": "join_numeric_key",
  "filepath": "/lakehouse/.../company_dim.csv",
  "dependencies": [
    {
      "schema": "Dimensions",
      "table": "tbl_d_company_service",
      "on": [{"source": "Service_Type_Cd", "target": "Service_Type_Cd"}],
      "select": {"Service_Description": "New_Column_From_Join"},
      "where": {"target": ["Current_Ind == 1"]},
      "how": "left",
      "on_missing": "null"
    }
  ]
}
```

## Dependency joins

`DependencyJoin` entries support:
* `schema` / `table`: lookup table to join to.
* `on`: list of `{source, target}` mappings between incoming and lookup columns.
* `select`: mapping of lookup column → alias added to the incoming frame.
* `where`: optional SQL filters applied before the join.
* `filter_target_current`: restricts to `Current_Ind = 1` rows (default `True`).
* `how`: `"left"` or `"inner"`.
* `on_missing`: `"null"` or `"error"`.

Joins run inside SQL; pandas only reads the filtered result set, renames columns, and maps the selected values to the incoming `DataFrame`.

## Runtime flow

1. Register metadata (see `examples/run_metadata_example_fabric.py`).
2. Call `run_dimension(engine, metadata_name, csv_reader=...)`.
3. Loader steps:
   * read CSV (custom reader may handle lakehouse authentication);
   * cast datetimes using optional `parse_format`;
   * ensure schema/table exist (auto-create/evolve if needed);
   * run SQL dependency joins;
   * apply SCD2 history for normal rows and latest-only handling for the reserved `NA` row.

## Tests

* `python -m pytest dataprepkit/tests/test_metadata_loader.py` — metadata parsing, datetime casting, dependency joins, schema aliases.
* `python -m pytest dataprepkit/tests/scd2/test_apply_changes.py::test_nullable_data_column_allows_null_staging` — staging with nullable columns.

## Examples

* `examples/run_metadata_example_fabric.py` – registers multiple metadata objects and drives Fabric-based loading.
* `examples/run_scd2_fabric.py` – demo insert/update/delete/reinsert phases on Fabric tables.

## Notes

- Metadata entries are validated via Pydantic; missing keys raise errors.
- Update metadata before rerunning `run_dimension` to maintain the registry.
- The system is designed for declarative metadata so you can add dimensions by editing metadata, not code.

## Helpers modules

`dataprepkit.helpers` exposes reusable utilities:

* `helpers/connectors/fabric.py` – Fabric SQL connection builder that handles MSI tokens, driver selection, and pooling. Use `create_engine_for_fabric(endpoint, database, preferred_driver, ...)` plus `validate(engine)` before running any metadata loads.
* `helpers/storage.py` – Lakehouse/mount helpers (mounting, paths) used by the Fabric examples; consult it for paths when you need to read raw CSVs stored in your lakehouse.
* Additional helper modules (e.g., connector-specific tooling) are referenced by the examples so you can replicate the Fabric patterns on other platforms.
