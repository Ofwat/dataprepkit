# Excel validation configuration

The public Excel validation API is in `dataprepkit.validation`. A configuration
can be built with Python models or loaded from JSON/YAML:

```python
from dataprepkit.validation import load_validation_config, validate_excel

config = load_validation_config("profiles/standalone.yaml")
result = validate_excel("candidate.xlsx", config=config)
print(result.format_report())
```

## Starter profiles

Two version-controlled starter profiles are included:

| Profile | Use it for |
| --- | --- |
| `profiles/single_file.yaml` | Run formula-error and feature checks on one workbook only. |
| `profiles/standalone.yaml` | Validate one workbook without a reference workbook. |
| `profiles/reference.yaml` | Compare a candidate workbook with a reference workbook. |

Discover profiles programmatically:

```python
from dataprepkit.validation import list_profiles, load_validation_config

for profile in list_profiles("profiles"):
    print(profile["name"], profile["path"])

config = load_validation_config("profiles/reference.yaml")
```

The profile is a starting point. Caller configuration can be merged over a
profile, and lists replace profile lists rather than being concatenated:

```python
config = load_validation_config(
    {"sheet_policy": {"extra_sheet_action": "warning"}},
    compatibility_profile="profiles/standalone.yaml",
)
```

## Runtime options

`runtime` controls workbook access and safety limits:

| Field | Values / meaning |
| --- | --- |
| `max_cells_scanned` | Maximum number of physically stored cells inspected. |
| `read_only` | Use openpyxl read-only mode; recommended for large workbooks. |
| `macro_policy` | `preserve`, `discard`, or `reject` for macro-enabled files. |
| `missing_formula_cache_action` | `not_run` or `error` when formula results are not cached. |
| `feature_policy` | Actions for workbook features and unavailable feature detection. |

Feature policies use `ignore`, `warning`, or `error` for `macros`,
`external_links`, `charts`, `pivot_tables`, and `named_ranges`. Merged-cell
detection uses the structured `action` and `scope` policy shown below.
`unavailable_action` applies when a feature cannot be inspected:

```yaml
runtime:
  max_cells_scanned: 10000000
  read_only: true
  macro_policy: reject
  missing_formula_cache_action: not_run
  feature_policy:
    unavailable_action: warning
    macros: error
    external_links: warning
    charts: ignore
    pivot_tables: warning
    named_ranges: ignore
    merged_cells:
      action: error
      scope:
        type: all_sheets
```

`merged_cells` must be configured as a structured policy and may be scoped to
specific sheets:

```yaml
feature_policy:
  merged_cells:
    action: warning
    scope:
      type: sheet_pattern
      pattern: "^Data_"
```

The supported scope types are `all_sheets`, `selected_sheets` with a `sheets`
list, and `sheet_pattern` with a regular-expression `pattern`. Only merged-cell
detection supports this scope; all other feature detections remain workbook-wide.

`error` makes the result invalid. `warning` keeps the result valid but records
the issue in `result.warnings`. `ignore` records a diagnostic in
`result.diagnostics`.

## Rule dependencies

Workbook checks can declare prerequisites with `depends_on`. Dependencies are
resolved in order even when the checks are listed in a different order. A
dependent check runs only when every dependency completes without errors.
Warnings count as a passing dependency; disabled or `NOT_RUN` dependencies
block dependent checks and produce a `NOT_RUN` event.

```yaml
workbook_checks:
  - rule_code: formula_difference
    enabled: true
    scope: overlapping_sheets
    depends_on:
      - sheet_structure
  - rule_code: sheet_structure
    enabled: true
    scope: overlapping_sheets
```

Dependencies must refer to another configured `workbook_checks` rule. Duplicate
rule codes and dependency cycles are rejected during configuration validation.

## Comparison options

`comparison` defines the SQL Server-oriented text comparison policy:

```yaml
comparison:
  case_sensitive: false
  accent_sensitive: true
  trim_whitespace: true
  collapse_internal_whitespace: false
  empty_string_is_null: true
  null_tokens: ["NULL", "N/A"]
  collation_name: mssql_case_insensitive
```

Column definitions may provide a comparison override. Expected cells may use
`exact`, `normalised`, `case_insensitive`, `numeric`, or `date` comparison
modes. Formula checks currently compare formula text; Excel/Java formula
evaluation is intentionally not part of this API yet.

## Workbook checks

Checks are configured with `workbook_checks`. Built-in checks include:

| Rule | Purpose |
| --- | --- |
| `required_sheet` | Enforced through `sheet_policy.required_selectors`. |
| `extra_sheet` | Enforced through `sheet_policy.extra_sheet_action`. |
| `missing_reference_sheet` | Finds reference sheets absent from the candidate. |
| `sheet_structure` | Compares content-based used areas. |
| `formula_difference` | Compares candidate/reference formula text. |
| `formula_error` | Finds configured Excel error tokens. |

The complete built-in catalogue is also available in Python:

```python
from dataprepkit.validation import list_available_rules

for rule in list_available_rules():
    print(rule["rule_code"], "->", rule["attachment"])
```

### Complete check catalogue

| Check | Configure it with | What it reports |
| --- | --- | --- |
| `required_sheet` | `sheet_policy.required_selectors` | A required sheet selector matched no sheet. |
| `extra_sheet` | `sheet_policy.extra_sheet_action` | A candidate sheet was not selected or ignored. |
| `expected_cell` | `expected_cells` | A resolved cell differs from its expected value. |
| `table_resolution` | `tables` and `data_boundary` | A required table or data boundary cannot be resolved. |
| `column_header` | `tables.header_policy` | Required, ordered, blank, duplicate, or extra headers are invalid. |
| `non_empty_data` | `tables.data_presence: require_one_usable_row` | A required table has no usable data row. |
| `empty_row_pattern` | `tables.empty_row_rules` | A row configured as blank contains a value. |
| `missing_value` | `tables.column_validations[].null_policy: error` | A configured value is null or blank. |
| `duplicate_value` | `tables.column_validations[].unique` | A normalized column value occurs more than once. |
| `allowed_values` | `tables.column_validations[].allowed_values` | A value is not in the configured allow-list. |
| `forbidden_values` | `tables.column_validations[].forbidden_values` or `workbook_checks` | A value is in the configured deny-list or matches a forbidden pattern. |
| `pandas_load` | `tables` | A resolved table could not be loaded into pandas. |
| `missing_column` | `tables.column_validations` or `tables.dataframe_checks` | A configured column is absent from the loaded table. |
| `empty_table` | `tables` | A resolved table loaded with no data rows. |
| `data_boundary` | `tables.data_boundary` | A configured data boundary could not be resolved. |
| `max_length` | `tables[].dataframe_checks` | A loaded DataFrame value exceeds its configured length. |
| `values_in_reference` | `cross_table_checks` | A source value is absent from another loaded table. |
| `missing_reference_sheet` | `workbook_checks` | A sheet in the reference workbook is absent from the candidate. |
| `sheet_structure` | `workbook_checks` | Candidate and reference content-based used areas differ. |
| `formula_difference` | `workbook_checks` | Candidate and reference formula text differs at a cell. |
| `formula_error` | `workbook_checks` | A cached cell contains one of the configured Excel error tokens. |
| `<feature_name>` | `runtime.feature_policy.<feature_name>` | The named workbook feature was detected. |
| `<feature_name>_detection_unavailable` | `runtime.feature_policy.unavailable_action` | The named feature could not be inspected. |

Feature events use the concrete feature name: `macros`, `external_links`,
`charts`, `pivot_tables`, `named_ranges`, or `merged_cells`. Formula evaluation
is not performed: formula checks
inspect formula text and cached values only.

### Common table checks

This example enables header, data presence, uniqueness, allow-list, and
deny-list checks without using physical cell coordinates:

```yaml
tables:
  - name: outputs
    required: true
    sheet_selector:
      mode: exact
      value: Data
    header_row: 1
    header_policy:
      required_columns: [Unit, Status]
      match_mode: exact_order
      missing_column_action: error
      extra_column_action: error
    data_boundary:
      mode: last_non_empty_row
      columns: [Unit]
    data_presence: require_one_usable_row
    column_validations:
      - column: Unit
        allowed_values: ["£", "%", Ml, Number]
      - column: Status
        forbidden_values: [DELETE, INVALID]
        unique: true
```

For `last_non_empty_row`, `columns` identifies the columns that determine the
boundary. To use every column resolved from the configured header row, set
`infer_columns: true` instead:

```yaml
    data_boundary:
      mode: last_non_empty_row
      infer_columns: true
```

If both are supplied, explicit `columns` take precedence. Supplying neither
remains invalid, because the validator cannot determine which columns define
the data boundary.

`allowed_values` and `forbidden_values` are mutually exclusive on one column
validation. Values are compared using the global `comparison` policy unless a
column definition supplies its own comparison override.

Use `forbidden_patterns` for regular-expression exclusions. Exact values and
patterns may be combined; a value fails if either check matches. Patterns are
validated during configuration loading and are applied to the normalized text
under the configured comparison policy:

```yaml
      - column: status
        forbidden_values: [Closed, Cancelled]
        forbidden_patterns: ["^Test", "Deprecated$"]
```

`allowed_values` cannot be combined with either forbidden list. Pattern-based
findings identify the matching pattern in `expected_value` and the event
description.

The same rule code can scan workbook cells directly, without defining a table.
Workbook checks support the shared sheet scopes (`all_sheets`, `selected_sheets`,
and `sheet_pattern`):

```yaml
workbook_checks:
  - rule_code: forbidden_values
    enabled: true
    severity: error
    scope:
      type: sheet_pattern
      pattern: "^Data_"
    options:
      forbidden_patterns:
        - "^Test"
        - "Deprecated$"
```

Cell values are matched as text. String values use the configured comparison
normalization; non-string values are converted to text before matching. Each
finding includes the worksheet, Excel cell reference, actual value, and matched
pattern.

### Expected cells

Use `expected_cells` when a small number of workbook-level values must be
present. Use selectors and Excel cell references such as `Z11`; the location
does not need to be hardcoded into validation code:

```yaml
expected_cells:
  - name: reporting_period
    locations:
      - sheet_selector: {mode: exact, value: Metadata}
        cell_reference: Z11
    expected_value: "2025 Q4"
    fallback_strategy: exactly_one
```

## Excel-to-pandas validation contract

The implementation has one simple rule:

```text
Excel checks whether a table can be loaded.
Pandas checks whether values in the loaded table are valid.
```

The execution path is always:

```text
resolve sheet and table
→ validate Excel table structure
→ load the table
→ run value checks
→ return one result
```

Excel is responsible for table and column structure:

- the configured sheet exists;
- the header row exists;
- headers are present and non-blank;
- headers are unique according to the configured comparison policy;
- ambiguous or duplicate headers are errors;
- any explicitly required columns exist;
- the data boundary resolves.

Pandas is responsible only for values inside resolved columns:

- the table is empty or contains usable rows;
- null values according to the configured null policy;
- uniqueness;
- allowed and forbidden values;
- forbidden patterns;
- lengths and future DataFrame rules;
- cross-table checks.

Users configure one table and its checks. They do not configure pandas or
choose an execution engine. Column names must match the Excel headers exactly;
DataPrepKit does not rename or silently alias them. Structural column errors
are reported before loading. Value findings retain the original worksheet and
Excel cell location.

Header matching is exact, including case and whitespace. The comparison policy
controls values inside the loaded table; it does not change Excel header
identity.

Required columns are an optional structural check. They are checked against the
Excel header before loading and a missing required column produces a structural
finding. The validator still loads the table when the remaining headers and
data boundary are sufficient; checks for the missing column are marked
`NOT_RUN`. Loading is blocked only when the sheet, headers, or data boundary
cannot be resolved. Required columns do not limit automatic table scanning:
`infer_columns: true` still uses all headers that are present, including columns
that are not required. Extra columns are allowed unless the header policy
explicitly rejects them.

Loading is mandatory after successful structural validation. A structural
failure prevents loading and marks all value checks for that table `NOT_RUN`. A
pandas load failure produces a `pandas_load` finding and also marks all value
checks for that table `NOT_RUN`.

An empty table is still loaded when its headers and boundary resolve. The
`data_presence` setting then determines whether the result is valid:
`allow_empty` accepts zero data rows, while `require_one_usable_row` reports a
`non_empty_data` finding. A missing sheet, invalid header, duplicate header,
missing explicit boundary column, or unresolved boundary prevents loading and
marks every value check for that table `NOT_RUN`.

The table configuration uses generic names and does not require physical cell
coordinates or separate column definitions:

```yaml
tables:
  - name: measurements
    sheet_selector:
      mode: exact
      value: Measurements
    header_row: 1
    data_boundary:
      mode: last_non_empty_row
      infer_columns: true
    header_policy:
      required_columns: [Measure_Value, Process_Cd]
    column_validations:
      - column: Process_Cd
        unique: true
      - column: status
        forbidden_values: [Closed]
        forbidden_patterns:
          - "^Test"
          - "Deprecated$"
    dataframe_checks:
      - rule_code: max_length
        column: Measure_Value
        max_length: 4000
        length_mode: characters
```

The public API has one validation entry point and one result object. Users do
not call pandas directly or select a validation engine; pandas loading,
conversion, caching, and value-check execution are internal implementation
details.

For `last_non_empty_row`, explicit boundary columns may be used instead:

```yaml
    data_boundary:
      mode: last_non_empty_row
      columns: [Measure_Value]
```

Explicit `columns` take precedence over inferred boundary columns. The same
Excel header names are used when value checks run after loading.

Required columns are configured under `header_policy.required_columns`. A
missing required column produces an Excel structural finding, but does not
prevent loading when the remaining headers and data boundary are sufficient.
Checks for the missing column are marked `NOT_RUN`. A missing sheet, invalid
header row, missing explicit boundary column, or otherwise unresolved boundary
prevents loading and marks all dependent value checks `NOT_RUN`.

`required_columns` is the only required-column concept. Value nullability is a
value check and is controlled by the null policy; it is not used to decide
whether an Excel column exists.

For `last_non_empty_row`:

- the header row is excluded from data;
- data starts at `header_row + 1`;
- `infer_columns: true` scans every present header column;
- explicit `columns` scans only those columns;
- missing explicit boundary columns prevent boundary resolution;
- values outside the selected columns do not affect the boundary;
- the final row is the greatest row containing a usable value;
- boundaries are resolved independently for each sheet selected.

A formula is loaded into pandas as its cached cell result, not as formula text.
Formula calculation is outside this contract.

Usable values are determined by the existing comparison policy. With
`trim_whitespace: true`, whitespace-only strings are treated as empty when
`empty_string_is_null: true`; values matching `null_tokens` are also treated
as null.

`data_presence` controls empty tables. Use `allow_empty` when a header-only
table is valid, or `require_one_usable_row` when at least one usable data row
is required. Usability is evaluated from the resolved table columns using the
configured comparison and null policies; values outside the table do not count.

`max_length` ignores configured null values. Length modes are explicit so that
Python character length is not accidentally confused with SQL Server byte or
UTF-16 length semantics.

For uniqueness, null-like values are controlled by the comparison policy. A
value matching `null_tokens`, or an empty value when `empty_string_is_null` is
enabled, is treated as null. Set `null_policy: ignore` to exclude repeated
null-like values from uniqueness checks; set `null_policy: error` when they
should be reported as missing.

### Table checks

Users configure checks against table columns. Excel structure is validated
first, then DataPrepKit loads the table and runs the configured value checks
against the resulting pandas data. A separate pandas API is not required.

For example, these checks are configured identically regardless of their
internal execution method:

```yaml
column_validations:
  - column: Process_Cd
    unique: true
  - column: Status
    null_policy: error
    forbidden_patterns:
      - "^Test"
```

If the Excel structure or table load fails, the configured data checks are not
executed and receive `NOT_RUN` results with the structural or load failure as
their reason.

Regex pattern checks operate on non-null values converted to text. Strings are
matched after comparison-policy normalization; numbers and dates use their
pandas string representation. Null-like values do not match patterns.
Patterns are compiled and validated during configuration loading.

Cross-table checks use the same table definitions and can match several sheets
when a selector matches several sheets. Values from all reference matches are
treated as one reference set, while failures retain the source sheet and Excel
cell location. Reference duplicates are allowed by default; reject normalized
duplicates explicitly when the lookup must be unique:

```yaml
cross_table_checks:
  - name: measurement_units_exist
    rule_code: values_in_reference
    source_table: measurements
    source_column: unit
    reference_table: allowed_units
    reference_column: unit
    null_policy: ignore
    duplicate_reference_action: error
```

The check counts every source row it evaluates in the result summary. A failed
or unresolved pandas load produces `NOT_RUN` events for dependent checks rather
than attempting to read an unbounded worksheet region.

### Reference comparison

Pass `reference_path` to enable reference checks. The standalone profile leaves
these checks empty; the reference profile enables the normal comparison set:

```python
config = load_validation_config("profiles/reference.yaml")
result = validate_excel(
    candidate_path="candidate.xlsx",
    reference_path="reference.xlsx",
    config=config,
)
```

Reference checks do not evaluate formulas. `formula_difference` compares text,
with `whitespace_policy: exact` or `normalised`; `formula_error` checks cached
Excel error values in the candidate.

Formula options are typed and reject unknown keys:

```yaml
workbook_checks:
  - rule_code: formula_error
    enabled: true
    scope: all_sheets
    options:
      error_tokens: ["#DIV/0!", "#N/A", "#NAME?", "#REF!", "#VALUE!"]
  - rule_code: formula_difference
    enabled: true
    scope: overlapping_sheets
    options:
      whitespace_policy: normalised
```

`rule_severity` may set a default severity per rule, while individual checks
and table validations can override it.

## Custom rules

Use a custom rule when a check is specific to your organization and cannot be
expressed with the built-in configuration. Register it once in the process,
then reference its stable code in `workbook_checks`:

```python
from pathlib import Path

from dataprepkit.validation import (
    ValidationEvent,
    WorkbookCheck,
    load_validation_config,
    register_rule,
    validate_excel,
)


def check_workbook_name(context):
    if Path(context.candidate_path).stem.startswith("submission_"):
        return None
    return ValidationEvent(
        rule_code=context.rule_code,
        severity="warning",
        description="Candidate filename does not use the submission_ prefix",
    )


register_rule("workbook_name", check_workbook_name)
try:
    config = load_validation_config(
        {
            "workbook_checks": [
                WorkbookCheck(
                    rule_code="workbook_name",
                    enabled=True,
                    scope="all_sheets",
                )
            ]
        },
        compatibility_profile="profiles/standalone.yaml",
    )
    result = validate_excel("candidate.xlsx", config=config)
finally:
    # Registration is process-local; unregister it when the application no
    # longer needs the rule or when tests must isolate their registries.
    from dataprepkit.validation import unregister_rule

    unregister_rule("workbook_name")
```

The validator receives a `RuleContext` containing:

| Field | Meaning |
| --- | --- |
| `rule_code` | The registered code used by the configuration. |
| `candidate_path` | Candidate workbook path as a string. |
| `reference_path` | Reference workbook path, or `None`. |
| `config` | The resolved `WorkbookValidationConfig`. |

A validator may return `None`, one `ValidationEvent`, or a list of events.
Use `severity="warning"` for non-blocking findings; failed events are errors
by default. Use `status="NOT_RUN"` when the custom rule cannot be evaluated and
include a useful `reason`. Custom registration is intentionally not serialized
into YAML/JSON profiles, so every process must register the rule before loading
or running a configuration that uses it.

## Saving and restoring results

`ValidationResult` is a stable, JSON-compatible record. It includes the result
events plus audit metadata such as configuration version, profile name,
candidate filename and version, reference version, comparison policy, and
completion status:

```python
from pathlib import Path

from dataprepkit.validation import (
    dump_validation_result,
    load_validation_result,
    validate_excel,
)

result = validate_excel(
    candidate_path=Path("candidate.xlsx"),
    config=config,
    candidate_version="2026-08-21T12:00:00Z",
    run_id="run-123",
)

Path("validation-result.json").write_text(
    dump_validation_result(result, format="json"),
    encoding="utf-8",
)

restored = load_validation_result(
    Path("validation-result.json").read_text(encoding="utf-8")
)
print(restored.is_valid)
print(restored.format_report())
```

The loader also accepts a mapping or an existing `ValidationResult`. Restored
results retain typed `ValidationEvent`, `DiagnosticEvent`, and comparison
objects, so they can be inspected without reopening either workbook. Result
serialization is separate from configuration serialization: use
`dump_validation_config` and `load_validation_config` for profiles.

## Schema and validation

The versioned schema is available as `dataprepkit/validation/schema/v1.json`
and through `get_config_schema()`. Use `validate_config()` to validate a
configuration without opening a workbook. Validation failures raise
`ConfigurationError` with a field path suitable for reporting to users.
