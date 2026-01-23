# Metadata-driven dimension loading plan

1. **Metadata definition**
   - Store each dimension as a minimal dictionary containing target table name, natural key columns, data column mappings, surrogate/join key names, raw file path, and any dependent tables, mirroring the `old_code.py` “json-like object”.
   - Register those dictionaries via `register_metadata()` so they are validated once and stored in a central registry before any load runs.

2. **Orchestration layer**
   - Build a small module (e.g., `dataprepkit.metadata_loader`) that reads the chosen metadata entry, loads the CSV (or configured source) into pandas, applies any configured renames/transforms, and calls `dataprepkit.scd2.apply_changes`.
   - Reuse the Fabric engine helper or a token-based SQLAlchemy engine if needed, but keep the metadata orchestrator focused on wiring metadata -> data ingestion -> SCD2 API.

3. **Testing & validation**
   - Parameterize the existing SQLite-backed tests over sample metadata entries so the same coverage (insert/update/delete/reinsert/idempotency) applies when dimensions are defined via metadata.
   - Assert that the raw file path recorded in metadata matches the dataset pulled for each run, reinforcing auditability.

4. **Documentation & examples**
   - Add a short README/example describing how to add metadata records and how Fabric jobs reference the metadata to trigger the loader.
   - Keep example scripts lean; re-use `examples/run_scd2_fabric.py` but show how to swap the metadata entry for different dimensions.
