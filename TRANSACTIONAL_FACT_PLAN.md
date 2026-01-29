## Transactional fact ingestion plan

0. **Validate source files**
   - Query the batch metadata to read `file_hash_md5` and `Filename`.
   - Use `Organisation_Cd`, `Submission_Period_Cd`, and `Observation_Period_Cd` to locate the raw file being processed.
   - Compute the file’s MD5 hash and compare it to `file_hash_md5`. If the hashes don’t match, halt the pipeline and surface an alert so incorrect files never hit the fact table.

1. **Build temporary fact table via lookups**
  - For every dimension referenced by the fact, update `fact_stage` by joining on the natural key and pulling the current surrogate key:
    ```sql
     UPDATE fact_stage
     SET dim_sk = d.surrogate_key
     FROM fact_stage fs
     JOIN dim_table d
       ON fs.dim_natural_key = d.natural_key
     WHERE d.current_ind = 1;
     ```
  - Repeat for each dimension, logging or capturing rows that don’t resolve.
  - As part of these lookups you can bring extra columns from the dimension (e.g., geography or metadata attributes) even if they were not in the staging table, and multiple dimensions can be joined to enrich the stage before the fact insert.

3. **Insert into fact table transactionally**
   - Wrap the following inside a single transaction:
     * Optionally delete existing fact rows for the same business key or batch marker for idempotency.
     * Insert from `fact_stage` (now containing only surrogate keys) into the production fact table.
     * Set any audit attributes such as `batch_id`, `load_timestamp`, etc.

4. **Handle unmatched rows / data quality**
   - Decide how to treat fact rows whose surrogate lookups failed: reject the batch, route them to an error table, or keep them with null FKs but flag them.
   - Capture counts/logs of unresolved keys before the insert so you can monitor drift.

5. **Cleanup and archiving**
   - After the transaction commits, archive the contents of `fact_stage` (copy to a snapshot table or file) if needed, then truncate/delete it to prepare for the next batch.

6. **Automation**
   - Package these steps behind a config-driven helper API that takes the engine, stage/fact tables, batch metadata, and the dimension-column mapping described earlier.
   - Build a dedicated `fact_loader` module that exposes `FactConfig`, surrogate join specs, hash validation, and the transactional insert logic so the workflow is implemented in one place.
   - Keep it independent of the metadata loader so it can be reused in other pipelines, but have it consume shared helpers (e.g., `stage_dataframe`, `ensure_schema_exists`) to minimize duplication.

- **Progress update**
  - Step 0 ("Validate source files") now has a working implementation: `verify_stage_file_hashes` enumerates each staging row, resolves the file (with configurable partition columns), and raises `HashMismatchError` or `MissingStageFileError` if anything is wrong. An example script lives in `examples/fact_hash_example.py`.

- **Production considerations**
- Log the raw file hash comparison result along with the business keys when validation fails so you can trace which file caused the rejection.
- Make the staging-to-dimension mapping metadata expressive enough to declare composite key joins, extra columns to pull, and multiple dimension lookups per fact column.
- Wrap the transactional insert in retry/backoff logic (or use the database’s built-in retry policies) to survive transient locks, and persist batch audit metadata (batch_id, timestamp, row counts) for reconciliation.
