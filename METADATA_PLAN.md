# Metadata-Driven Dimension Load Orchestrator Specification

## 1. Purpose and Scope

This specification defines a metadata-driven process (“the orchestrator”) that loads dimension tables from CSV snapshot files and invokes a pre-defined Snapshot SCD2 implementation.

This document does not define SCD2 semantics. It defines how to ingest, validate, manage schema drift/evolution, resolve dependencies, and safely invoke SCD2.

Key generation infrastructure (sequences/identities/allocators) is a black box owned by the SCD2 implementation.

---

## 2. Assumptions and External Contracts

The orchestrator assumes:

* A compliant Snapshot SCD2 implementation is available and callable.
* The target database supports transactional writes sufficient to guarantee atomicity at the table level (or an equivalent all-or-nothing pattern).
* Pandas is available for CSV ingestion.

The SCD2 implementation is responsible for:

* generating `surrogate_key` and `join_numeric_key` with its own internal mechanism
* ensuring keys are unique, non-null, monotonic, and never reused (per the SCD2 spec)
* initializing/validating any key-generation infrastructure it requires

The orchestrator must surface SCD2 failures (including missing key infrastructure) as table failures and must not attempt to remediate key infrastructure.

---

## 3. Inputs

### 3.1 Metadata (required)

Metadata must fully define table loading behavior and must separate concerns:

* source input definition (`source`)
* target schema of data columns only (`schema`)
* SCD2 invocation configuration (`scd2`)
* transforms and dependency lookups (`transform`, optional)
* schema drift handling behavior (`schema_handling`, global or per-table)

System-managed SCD2 columns are implicit and must not be included in metadata schema.

### 3.2 Snapshot input files (required)

Each table consumes exactly one CSV file per execution, representing a full snapshot of the domain’s current state.

Missing natural keys imply deletions.

---

## 4. Global Execution Guarantees

For each table load, the orchestrator guarantees:

* no database writes occur before successful CSV ingestion and validation
* a single captured execution timestamp is used consistently for that table execution
* atomicity: either the table load is fully applied or not applied at all
* schema drift is detected and logged; optional schema evolution is supported
* deterministic behavior within a deployment

---

## 5. Metadata Structure

### 5.1 Global configuration (required)

* `warehouse_name` (string)
* `target_schema_name` (string)
* `defaults` (optional, recommended)

  * `column.nullable`: True
  * `column.unique`: False
  * `source.mode`: "snapshot"
  * `source.duplicate_key_policy`: "error"
  * `source.null_key_policy`: "error"
  * `schema_handling.mode`: "suggest" | "evolve"
  * `run.on_table_failure`: "continue"
  * `run.on_dependency_failure`: "skip_dependents"
  * `row_hash.algorithm`: implementation-defined (must be stable within deployment)
  * `row_hash.encoding`: implementation-defined

### 5.2 Per-table configuration (required)

Each table entry must contain:

* `target`
* `source`
* `schema`
* `scd2`
* `transform` (optional)

---

## 6. Per-Table Metadata Contracts

### 6.1 `target` (required)

* `table_name` (string)
* optional overrides: `schema_name`, `warehouse_name`

### 6.2 `source` (required)

* `filepath` (string)
* `mode`: must be "snapshot"
* `expected_columns` (optional set[string])
* `renames` (optional map[string->string])
* `csv_options` (optional map passed to pandas.read_csv)
* `duplicate_key_policy`: must be "error"
* `null_key_policy`: must be "error"

If `expected_columns` is omitted, the orchestrator must infer required columns as:

* natural key columns
* all schema data columns
* all tracked columns
* all columns referenced by dependency joins

### 6.3 `schema` (required, data columns only)

* `data_columns`: map column_name → column_spec

`column_spec`:

* `type` (required)
* `nullable` (default True)
* `unique` (default False)
* `default` (optional)

Rules:

* Natural keys must be non-null (enforced by orchestrator even if metadata omits it).
* System-managed SCD2 columns must not be listed here.
* Columns not listed must not be written to target.

### 6.4 `scd2` (required)

* `natural_keys` (ordered list[string])
* `tracked_columns` (ordered list[string])
* `key_columns` (required):

  * `surrogate_key` (physical column name in target)
  * `join_numeric_key` (physical column name in target)
* optional `row_hash` override:

  * `algorithm`
  * `encoding`

Rules:

* `tracked_columns` must be a subset of `schema.data_columns`.
* The orchestrator computes `Row_Hash` deterministically within the deployment, using tracked columns.

### 6.5 `transform` (optional)

* `processing_class` (optional)
* `dependency_tables` (optional list[string])
* `dependency_joins` (optional list[dependency_join])

`dependency_join`:

* `table` (required string)
* `how` (required: "left" | "inner")
* `on` (required list of `{source:<col>, target:<col>}`)
* `filter_target_current` (default True)
* `select` (required map target_column → dependency_column)
* `on_missing` (required: "error" | "null")

Rules:

* Dependency-selected columns must exist in `schema.data_columns`.
* Dependency joins may only reference source columns that exist after renames.

---

## 7. Dependency Ordering and Run Policy

The orchestrator must derive a load order by topologically sorting the dependency graph defined by `dependency_tables`.

Default run policy:

* if a table fails: log error and continue loading unrelated tables
* if a dependency fails: skip dependent tables and log the skip

Policies may be overridden globally, but must be applied consistently across the run.

---

## 8. CSV Ingestion and Staging (Pandas)

### 8.1 Read CSV

For each table, the orchestrator must read the CSV at `source.filepath` into a pandas DataFrame using `pandas.read_csv`.

Default `read_csv` parameters:

* `header=0`
* `skipinitialspace=True`
* `dtype=None`

If `source.csv_options` is provided, those options must be passed to `read_csv` (overriding defaults where applicable).

### 8.2 Read failures

If `read_csv` fails:

* log error to stdout (table name, filepath, exception)
* fail the table
* apply run policy
* perform no database writes

### 8.3 Initial DataFrame validation

After reading:

* DataFrame column names must be unique (duplicate names => fail)
* DataFrame column names must be strings (otherwise fail)
* Empty DataFrame is allowed and is treated as a valid empty snapshot (implies deletes for all currently current keys)

### 8.4 Renames

Apply `source.renames` to DataFrame columns before further validation.

Rename collisions (two columns mapping to same name or mapping onto an existing name) must fail the table.

### 8.5 Expected column validation

After renames:

* If `expected_columns` is defined, all expected columns must exist, else fail.
* Extra columns are allowed and ignored unless referenced later.

---

## 9. Target Table Handling

### 9.1 Table existence check

Before invoking SCD2, the orchestrator must check whether the target table exists.

* If missing: create it (Section 9.2).
* If exists: validate schema and detect drift (Section 10).

### 9.2 Table creation (when missing)

If the target table does not exist, the orchestrator must create it with:

* all system-managed SCD2 columns (implicit set)
* all natural key columns (from metadata schema)
* all schema-defined data columns

The orchestrator must not create or manage key-generation infrastructure (black box of SCD2).

If table creation fails:

* log error to stdout (table, reason)
* fail the table and apply run policy

---

## 10. Schema Validation, Drift, and Optional Evolution

### 10.1 Schema comparison

If the table exists, the orchestrator must compare:

* expected columns = (system-managed SCD2 columns) + (schema.data_columns)
  against actual target schema.

Drift types:

* missing expected columns
* type incompatibility (actual not safely coercible to expected)
* nullability mismatch (actual nullable but expected non-null)
* extra columns (informational unless configured otherwise)

A drift report must always be logged to stdout.

### 10.2 Drift handling mode

Drift handling is controlled by `schema_handling.mode`:

* "suggest" (default)
* "evolve"

### 10.3 Safe write set (mandatory under drift)

When drift exists, the orchestrator must compute a safe write set:

* always include system-managed SCD2 columns
* include only data columns that:

  * exist in target table
  * are type-compatible with staged DataFrame values
* exclude missing or incompatible columns
* fail the table if any non-null target column in the safe write set would receive null after coercion/defaulting

The orchestrator must log:

* columns included in safe write set
* excluded columns and reasons

### 10.4 Suggest mode

In "suggest" mode:

* the orchestrator must not run DDL to change schema
* it must emit a schema evolution plan to stdout (Section 10.6)
* it must proceed with SCD2 using the safe write set

### 10.5 Evolve mode

In "evolve" mode:

* the orchestrator may apply only safe, non-breaking changes:

  * add missing nullable columns
  * safely widen types
  * add defaults where specified
* it must not apply:

  * dropping columns
  * renames
  * narrowing types
  * nullable → non-null without default/backfill

Schema evolution concerns only table columns/constraints. It must not attempt to manage SCD2 key infrastructure.

If evolution fails:

* log the failure
* rollback and fail the table

After evolution:

* re-check schema drift; if unsafe drift remains, proceed using safe write set (or fail if drift prevents required columns).

### 10.6 Schema evolution plan output (required when drift exists)

When drift exists, log an explicit plan to stdout including:

* columns to add (name, type, nullable, default)
* proposed type widenings
* items requiring manual backfill or unsupported changes (clearly labeled)

---

## 11. Pre-SCD2 Transform, Dependency Joins, and Type Coercion

All staging work must occur on the pandas DataFrame before any SCD2 call.

Order:

1. Apply optional `processing_class` transform (must return DataFrame).
2. Apply `dependency_joins`:

   * use dependency tables filtered to current, non-deleted rows by default
   * enforce `on_missing` policy
3. Apply defaults from schema (where specified).
4. Coerce DataFrame columns to schema types.

   * if coercion fails for any required column in safe write set: fail table
5. Validate snapshot key constraints:

   * natural keys non-null
   * natural keys unique (duplicates => fail)
6. Validate presence of tracked columns.
7. Compute `Row_Hash` deterministically from tracked columns.

---

## 12. Invocation of SCD2 (Black Box)

The orchestrator must invoke SCD2 with:

* target table identifier
* staged DataFrame (or a staged representation derived from it)
* natural keys
* tracked columns and computed Row_Hash
* safe write set
* a single captured `execution_time` (datetime(3))

The orchestrator must allow SCD2 to validate/initialize any key-generation infrastructure it requires.

If SCD2 raises an error (including key infrastructure errors):

* log error to stdout (table, error)
* rollback and fail the table
* apply run policy

---

## 13. Post-SCD2 Validation (Orchestrator-Level)

After SCD2 returns success, the orchestrator must validate (via queries) that the table satisfies:

* at most one current row per natural key
* no row has Current_Ind=1 and Deleted_Ind=1
* current rows have Update_Date IS NULL
* closed rows have Update_Date IS NOT NULL

If validation fails:

* log error
* rollback and fail the table

---

## 14. Atomicity and Idempotency

* All table changes must be applied within one transaction (or equivalent atomic pattern).
* Any failure must leave the table unchanged from before the table execution began.
* Re-running with the same input and same starting state must produce no net change.

---

## 15. Logging Requirements (stdout)

Per table, log:

* filepath and read status
* input row count
* schema drift summary (if any)
* safe write set summary (if drift)
* schema evolution plan (if drift)
* transform/dependency steps applied (high-level)
* SCD2 classification counts if SCD2 exposes them; otherwise log “not available”
* success/failure outcome and rollback confirmation on failure

---

## 16. Non-Goals

The orchestrator does not:

* redefine SCD2 behavior
* manage key-generation infrastructure
* rewrite historical rows
* support parallel writers
* guarantee reproducible keys across rebuilds
* support real-time ingestion

---

## 17. Minimum Metadata Checklist (Eligibility)

A table must not run unless metadata provides:

* source.filepath
* schema.data_columns (data columns only; may be empty only if truly no data columns exist)
* scd2.natural_keys
* scd2.tracked_columns
* scd2.key_columns
* (optional) transform dependencies and joins if used

If any required element is missing, log an error and skip/fail per run policy.
