# Metadata Orchestrator Action Items

This checklist captures the concrete pieces we still need to implement so the metadata-driven loader fully matches the spec in `METADATA_PLAN.md`.

1. **Metadata ingestion & validation**
   - [ ] Enforce metadata-driven required fields (`source.filepath`, `schema.data_columns`, `scd2.natural_keys`, `scd2.tracked_columns`, `scd2.key_columns`) before any processing begins.
   - [ ] Log source row counts, read status, and validation results to stdout as soon as each CSV is ingested.
   - [ ] Apply renames safely and emit errors when collisions occur.

2. **Schema handling & safe write set**
   - [ ] Detect schema drift by comparing system columns + data columns against the existing target table schema.
   - [ ] Build a safe write set that only includes columns present and type-compatible with the target.
   - [ ] Support `schema_handling.mode` = `suggest` vs `evolve`, including logging an evolution plan in suggest mode and performing light evolution (adding nullable columns, widening types, defaults) when allowed.
   - [ ] Log the final safe write set and any excluded columns.

3. **Transform/dependency orchestration**
   - [ ] Apply optional `processing_class` transforms before dependency joins.
   - [ ] Execute dependency joins using the metadata configuration, honoring `on_missing`, `filter_target_current`, and `how`.
   - [ ] Coerce DataFrame columns to schema types and validate required columns after joins.

4. **SCD2 invocation & logging**
   - [ ] Capture a single execution timestamp per table and pass it along with the staged DataFrame to `apply_changes`.
   - [ ] Log SCD2 classification counts (`inserts`, `updates`, `deletes`) or log “not available” if the implementation cannot provide them.
   - [ ] Surface SCD2 errors (e.g., missing key infrastructure) as table failures, roll back transactions, and emit the run policy outcome.

5. **Post-SCD2 validation**
   - [ ] Query the target table to ensure `Current_Ind=1` rows are unique per natural key, `Current_Ind/Deleted_Ind` flags are consistent with timestamps, and rows marked closed have non-null `Update_Date`.
   - [ ] Log any validation failures and roll back if the invariants are broken.

6. **Run policies & logging**
   - [ ] Apply table/dependency failure policies (`run.on_table_failure`, `run.on_dependency_failure`) consistently.
   - [ ] Log the final result per table (success/failure, whether a rollback happened).
   - [ ] Include schema drift summaries, safe write set details, transform steps, and execution metadata in the logs.

7. **Operational notes**
   - [ ] Ensure the smoke test is documented and runnable in CI.
   - [ ] Collect metrics for downstream owners (row counts, execution duration, write counts) if possible to aid monitoring.

