# SCD2 implementation plan

## Objectives
- Publish a standalone `dataprepkit` PyPI release that Fabric consumers can `pip install`.
- Provide a single public API (`scd2.apply_changes`) that accepts a pandas DataFrame plus configuration and updates a target database in one atomic execution.
- Keep the logic deterministic, schema-aware, and compliant with the system-column, key-constraint, and execution-context requirements listed in `scd2 reqs.txt`.

## Milestones
1. **Packaging & entry points**
   - Add the new module under `dataprepkit/` and ensure `setup.py` or equivalent exposes it.
   - Document how Fabric installs the wheel and calls the entry point with pandas data/SQLAlchemy engine.
2. **SCD2 core logic**
   - Encode natural keys, join numeric key sequencing, row hash computation, and system columns (surrogate/join keys, timestamps, flags).
   - Implement insert/delete/update/reinsert detection by diffing ordered history and apply changes within a SQLAlchemy transaction to guarantee atomicity.
   - Treat surrogate keys as opaque, deterministic join_numeric_key, and enforce one current row per natural key.
3. **Validation, idempotency, and rebuildability**
   - Offer helpers to rebuild from full historical input while keeping logical identity constraints (excluding surrogate keys).
   - Validate row counts, current-indicator uniqueness, and constraint enforcement before applying writes.
4. **Testing & reuse strategy**
   - Use lightweight engines (SQLite/DuckDB) to exercise the public API through the inserted scenarios (insert/delete/update/reinsert).
   - Craft parameterized fixtures or unions describing each scenario so tests stay concise but cover all behaviors; avoid testing private helper branches separately.

## Notes
- All data movement and mutations must occur inside the same execution context; no intermediate writes outside the transaction are permitted.
- Align the implementation with the `scd2 reqs.txt` non-goals (e.g., no late-arriving corrections, non-deterministic ingestion).
