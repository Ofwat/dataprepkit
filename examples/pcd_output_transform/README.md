# PCD output transform

This package module prototypes the business transform in
`cde.sp_stg_price_control_deliverable_output`. It accepts raw landing rows as a
DataFrame and returns the 22 PCD output business columns; it does not read
files, access a database, or apply SCD2 history.

The transform currently covers:

- the four accepted source tab names;
- SQL procedure defaults, trim/length rules, output constants, applicability,
  RAG fields, and target versus outturn routing;
- grouping by every non-measure output column and summing both measures;
- no selection or deduplication among rows that share a natural key.

The tests use synthetic rows only:

```powershell
python -m pytest dataprepkit/tests/test_pcd_output_transform.py -q
```

Install the latest `main` branch with pip:

```powershell
python -m pip install --upgrade "git+https://github.com/Ofwat/dataprepkit.git"
```

Then import it in a notebook:

```python
from dataprepkit.pcd_output_transform import (
    transform_price_control_deliverable_output,
)

output = transform_price_control_deliverable_output(source_dataframe)
display(output)
```

## Known parity limits

This is a first transform prototype, not production parity. SQL Server uses
`SQL_Latin1_General_CP1_CI_AS` for grouping and comparisons. Pandas grouping is
case-sensitive and accent-sensitive, so text values that SQL considers equal
may remain separate here. SQL `ISNUMERIC` also accepts a different set of values
than Python `float()`. Those behaviours need explicit decisions and database
verification before this transform can claim general parity. Aggregation can
be moved to SQL to preserve the deployed collation while keeping the mapping in
version-controlled Python.

The procedure also has a mapping inconsistency that the prototype preserves:
`BRL_DPB1_ADD` passes the source filter but maps to category `N/A` because it is
absent from the `Base` category CASE expression.
