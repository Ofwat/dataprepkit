# Direct Excel workbook checks

The `dqchecks` library validates a submitted Excel workbook against an expected
template before the workbook data is loaded into pandas. These checks focus on
workbook integrity, sheet layout, formulas, and key identifying values.

## Checks performed

### Required sheets

The submitted workbook is compared with the template and any template sheets
missing from the submission are reported.

Extra sheets in the submitted workbook are not reported by this check.

### Sheet structure

For sheets present in both workbooks, the check compares the actual used area:

- last non-empty row;
- last non-empty column;
- whether either sheet is empty.

This detects added or removed rows and columns. Trailing cells that are empty
but remain in Excel's internal used range are ignored.

When configured with a header row, the check also compares column headers by
name and order. The comparison is case-sensitive.

### Formula differences

For overlapping sheets, formulas are compared cell by cell between the
template and submitted workbook. A difference is reported when:

- both cells contain different formulas; or
- one cell contains a formula and the other contains a non-formula value.

The check supports normal and array formulas. The workbooks must be loaded with
formulas available (`data_only=False`).

### Formula errors

The submitted workbook is scanned for cells whose evaluated result is an Excel
error, such as:

- `#DIV/0!`;
- `#VALUE!`;
- `#REF!`.

Each affected cell is returned as a separate validation event. This check uses
the workbook loaded with evaluated values (`data_only=True`).

### Company name

The submitted company name is compared with the expected full company name.
The location is selected according to the workbook template:

- `SelectCompany!B4`;
- `Quarterly_Data!G9`; or
- `Validation!B5`.

A mismatch, or a missing expected sheet, is reported.

### Organisation acronym

The organisation acronym (`Organisation_Cd`) is compared with the expected
organisation code in the configured workbook cell. A mismatch is reported as a
validation event.

### Empty-row layout

For configured flat-table sheets, the workbook must preserve the expected
spacing around the header:

- row 3 must be empty;
- row 1 must be empty except for the permitted cells in columns B and C.

Unexpected values in these rows raise an empty-row pattern validation error.

### Required flat-table headers

Configured flat-table sheets are expected to contain these headers in this
order:

1. `Acronym`
2. `Reference`
3. `Item description`
4. `Unit`
5. `Model`

Missing or misordered headers raise a column-header validation error.

### Usable sheet data

Completely blank data rows are removed during sheet loading. If a configured
sheet contains no usable rows after this cleanup, validation fails.

## Validation output

Workbook-level failures are converted into standardised pandas DataFrames.
Validation events can include an event ID, sheet and cell references, rule
code, error category, severity, and a human-readable description.

## Important limitations

- The checks compare formulas; they do not recalculate formulas.
- Formula-error detection depends on cached evaluated values stored in the
  workbook by Excel or another calculation engine.
- Missing-sheet validation does not flag extra submitted sheets.
- Header and empty-row checks apply only to sheets selected by the configured
  sheet-name patterns.

