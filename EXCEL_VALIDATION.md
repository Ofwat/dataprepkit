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
`external_links`, `charts`, `pivot_tables`, `named_ranges`, and `merged_cells`.
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
    merged_cells: error
```

`error` makes the result invalid. `warning` keeps the result valid but records
the issue in `result.warnings`. `ignore` records a diagnostic in
`result.diagnostics`.

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
| `missing_value` | `tables.column_validations[].required` | A required column value is null or blank. |
| `duplicate_value` | `tables.column_validations[].unique` | A normalized column value occurs more than once. |
| `allowed_values` | `tables.column_validations[].allowed_values` | A value is not in the configured allow-list. |
| `forbidden_values` | `tables.column_validations[].forbidden_values` | A value is in the configured deny-list. |
| `missing_reference_sheet` | `workbook_checks` | A sheet in the reference workbook is absent from the candidate. |
| `sheet_structure` | `workbook_checks` | Candidate and reference content-based used areas differ. |
| `formula_difference` | `workbook_checks` | Candidate and reference formula text differs at a cell. |
| `formula_error` | `workbook_checks` | A cached cell contains one of the configured Excel error tokens. |
| `feature_policy` | `runtime.feature_policy` | A configured workbook feature was detected. |
| `feature_detection_unavailable` | `runtime.feature_policy.unavailable_action` | A configured feature could not be inspected. |

`feature_policy` covers macros, external links, charts, pivot tables, named
ranges, and merged cells. Formula evaluation is not performed: formula checks
inspect formula text and cached values only.

### Common table checks

This example enables header, data presence, uniqueness, allow-list, deny-list,
and required-value checks without using physical cell coordinates:

```yaml
tables:
  - name: outputs
    required: true
    sheet_selector:
      mode: exact
      value: Data
    header_row: 1
    column_definitions:
      - name: unit
      - name: status
    header_policy:
      required_columns: [unit, status]
      match_mode: exact_order
      missing_column_action: error
      extra_column_action: error
    data_boundary:
      mode: last_non_empty_row
      columns: [unit]
    data_presence: require_one_usable_row
    column_validations:
      - column: unit
        required: true
        allowed_values: ["£", "%", Ml, Number]
      - column: status
        forbidden_values: [DELETE, INVALID]
        unique: true
```

`allowed_values` and `forbidden_values` are mutually exclusive on one column
validation. Values are compared using the global `comparison` policy unless a
column definition supplies its own comparison override.

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
