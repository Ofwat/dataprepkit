# PCD tests

Tests are grouped by where they sit in the PCD pipeline:

- `staging/` covers landing-to-staging transforms and their stored-procedure contracts.
  - `test_landing_transforms.py` covers submission, expenditure, scheme, milestone, and scheme-expenditure transforms.
  - `test_output_transform.py` covers `cde.sp_stg_price_control_deliverable_output`.
  - `test_scheme_detail_transform.py` covers `cde.sp_stg_deliverable_scheme_detail`.
  - `test_staging_procedure_contracts.py` holds the broader contract cases grouped by procedure.
- `flow/` covers the PCD staging orchestration, SCD2 table selection, dependency checks, and loader integration.

Shared landing-row builders live in `fixtures.py`.
