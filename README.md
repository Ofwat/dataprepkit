# dataprepkit
A collection of helpers and utilities for SQL-based data ingestion, transformation, and pipeline orchestration.

## Smoke Test

`examples/run_scd2_smoke.py` replays a Mini SCD2 run against SQLite and covers the SQL staging path you’ve built. Run it locally via:

```bash
python examples/run_scd2_smoke.py
```

Run the same command in CI (or as a pre-deploy gate) to ensure the orchestrator + SCD2 path remains healthy before hitting Fabric.
