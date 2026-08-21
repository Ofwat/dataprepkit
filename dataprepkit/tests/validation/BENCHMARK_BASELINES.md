# Excel validation benchmark baselines

These are smoke-test baselines, not performance guarantees. They are intended
to catch regressions where openpyxl iterates through an inflated worksheet
dimension instead of the rows that are actually stored in the file.

Run with:

```text
python -m pytest -q dataprepkit/tests/validation/test_performance.py
```

Baseline environment: Windows, Python 3.13, openpyxl 3.1.x.

| Fixture | Configuration | Baseline | Regression guard |
| --- | --- | ---: | ---: |
| 5,000 rows × 2 columns | normal read | under 1 second | 10 seconds |
| one value at `A1048576` | read-only | under 1 second | 3 seconds |

The inflated-range fixture is generated as an `.xlsx` file during the test;
it contains a real worksheet dimension ending at Excel's maximum row and a
real stored value in that row.
