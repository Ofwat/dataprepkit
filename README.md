# dataprepkit
A collection of helpers and utilities for SQL-based data ingestion, transformation, and pipeline orchestration.

## Smoke Test

`examples/run_scd2_smoke.py` replays a Mini SCD2 run against SQLite and covers the SQL staging path you’ve built. Run it locally via:

```bash
python examples/run_scd2_smoke.py
```

Run the same command in CI (or as a pre-deploy gate) to ensure the orchestrator + SCD2 path remains healthy before hitting Fabric.

## Metadata Example

The metadata loader is driven by JSON-like entries. `examples/run_metadata_example.py` shows a full workflow with dependency joins, schema evolution, transforms, policy logging, and parquet snapshot archiving. Execute it with:

```bash
python examples/run_metadata_example.py
```

Modify the metadata dictionary in the script to match your real tables/sources before running it in your environment.
