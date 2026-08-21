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

## Schema and validation

The versioned schema is available as `dataprepkit/validation/schema/v1.json`
and through `get_config_schema()`. Use `validate_config()` to validate a
configuration without opening a workbook. Validation failures raise
`ConfigurationError` with a field path suitable for reporting to users.
