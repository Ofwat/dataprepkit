# PCD staging parity fixture

`pcd_staging_parity.py` creates two synthetic inputs matching the source columns
used by the Python PCD flow:

- `lnd.price_control_deliverable`
- `lnd.price_control_deliverable_scheme_detail`

It includes accepted and rejected tabs/codes, output and expenditure measure
routes, all scheme-delay labels, matched and unmatched EDS periods, duplicate
grouping, null/default conversions, every scheme-detail category tab (including
case and whitespace normalization), detail casts, and all milestone/expenditure
period columns.

## Notebook use

Make the helper file available to the notebook (for example, copy it into the
notebook workspace), then run:

```python
import pandas as pd

from dataprepkit.pcd_flow import stage_pcd_landing
from pcd_staging_parity import (
    compare_pcd_staging_frames,
    load_eds_period_codes,
    make_pcd_parity_inputs,
    run_sql_staging_in_rollback_transaction,
)

period_codes = load_eds_period_codes(engine)
if not period_codes:
    raise RuntimeError(
        "The matched scheme-count branch needs at least one row in "
        "eds.price_control_deliverable_period."
    )

submission_number, standard_landing, scheme_detail_landing = (
    make_pcd_parity_inputs(matched_period_code=period_codes[0])
)

python_batch = stage_pcd_landing(
    standard_landing,
    scheme_detail_landing,
    period_codes=period_codes,
)

sql_frames = run_sql_staging_in_rollback_transaction(
    engine,
    standard_landing,
    scheme_detail_landing,
    submission_number=submission_number,
    expected_frames=python_batch.frames,
)

comparison = compare_pcd_staging_frames(python_batch.frames, sql_frames)
display(pd.DataFrame.from_dict(comparison, orient="index"))
assert all(result["matches"] for result in comparison.values())
```

The SQL helper invokes `cde.sp_stg_tbl_pcd` in a transaction, reads only rows
with the generated submission number, and rolls back the source inserts,
staging replacements, batch-number changes, and archive writes. Use a disposable
test database and confirm the SQLAlchemy connection does not enable autocommit.
The runtime-default `createdDate` row is checked for non-null values and
normalized during comparison because Python and SQL evaluate their current
timestamps at different instants.

The procedure filter for `cde.sp_stg_price_control_deliverable_scheme` only
accepts the DPB3/DPWW3/DPW3 family, while its Base-category CASE lists DPB1/DPB2
tabs. Therefore that CASE's Base arm is unreachable in this stored procedure;
the fixture compares the observed N/A result for DPB3 rather than assuming the
CASE is reachable.
