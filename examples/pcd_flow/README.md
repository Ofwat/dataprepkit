# PCD landing, staging and SCD2 prototype

`dataprepkit.pcd_flow` accepts normalized pandas frames representing the two
PCD landing tables, creates the seven business staging outputs, and passes
implemented PCD dimensions plus selected GEN dimensions to the package's
existing `run_dimension` SCD2 loader.

The boundary is the same as the SQL process after Fabric has loaded LND:

```python
from dataprepkit.pcd_flow import PcdDimension, run_pcd_flow

# These frames are read from lnd.price_control_deliverable and
# lnd.price_control_deliverable_scheme_detail (or equivalent files).
standard_landing = ...
scheme_detail_landing = ...

# Build these from the metadata snapshot used for the corresponding SP run.
# Each item provides the actual DimensionMetadata plus subject area, status,
# and the staging table name used by that metadata row.
dimensions = [...]

# GEN staging output is supplied by its own Python staging flow. The PCD SP
# calls the shared SCD2 procedure with GEN included, but does not create GEN
# staging as part of sp_stg_tbl_pcd.
gen_staging = {...}

result = run_pcd_flow(
    engine,
    standard_landing,
    scheme_detail_landing,
    period_codes=eds_period_codes,
    dimensions=dimensions,
    gen_staging=gen_staging,
)

# Inspect business-row parity before running against a target database.
for table, frame in result.staging.frames.items():
    print(table, len(frame), list(frame.columns))
print("SCD2 tables loaded:", result.loaded_tables)
print("Metadata rows skipped:", result.skipped_tables)
```

## Implemented SQL behavior

- `sp_stg_price_control_deliverable` output to
  `external_company_submission` (including metadata-row deduplication).
- Existing Python transforms for output and scheme detail.
- `sp_stg_price_control_deliverable_expenditure` and
  `sp_stg_price_control_deliverable_scheme`, including the explicit period
  code lookup used by scheme `numberOfSchemes`.
- The 14-row milestone and 14-row expenditure unpivots from each scheme-detail
  landing row.
- Metadata-driven SCD2 selection: only `Implemented` PCD and GEN definitions
  are run. All selected input frames are checked before the first SCD2 call.

## Parity boundary and known limits

The staging functions return business columns only. Fabric ingestion, SQL
batch numbers, load timestamps/change reason/created-by columns, clearing and
writing physical `stg` tables, and inserts into LND archive tables are not
performed here. `run_dimension` owns SCD2 persistence; the procedure archive
and operational logging flow remains a separate implementation step.

The scheme lookup is an explicit `period_codes` input sourced from
`eds.price_control_deliverable_period`. Implemented GEN dimensions require
their already-staged DataFrames in `gen_staging` unless their staging table is
one of the PCD outputs. Metadata status and keys are deliberately supplied by
the caller from the snapshot being parity-tested, so status changes are not
hardcoded in the package.

SQL Server `ISNUMERIC` accepts some locale and currency forms that Python's
ordinary float parser does not. Those formats, collation-sensitive tab
matching, and null `DateTimeCreated` timestamps need SQL-backed parity cases
before claiming full equivalence. `period_codes` lookup matches raw period
values exactly as the SQL join does.
