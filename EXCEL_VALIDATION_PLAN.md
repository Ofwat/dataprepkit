# Generic Excel validation plan

## Goal

Bring the direct Excel/workbook checks from `dqchecks` into `dataprepkit` in a
generic, configurable, and extensible way.

The validation engine should not contain hard-coded sheet names, cell
locations, header rows, or business-specific column names. Those details should
be supplied through a validated configuration object.

This applies to all validation policy, not only business names. The engine must
not hard-code ignored sheet patterns, fallback cell locations, header text,
comparison sensitivity, whitespace handling, expected match counts, severity,
or rule enablement. A caller must be able to reproduce the current `dqchecks`
behaviour by supplying a compatibility configuration.

## Scope

The first version should support these direct workbook checks:

1. Required and missing sheets.
2. Used-area and sheet-structure comparison.
3. Formula differences between a candidate workbook and a reference workbook.
4. Formula-error detection.
5. Configurable expected cell values.
6. Configurable table-header validation.
7. Configurable empty-row layout checks.
8. Configurable non-empty-data checks for required tables.
9. Configurable column rules, including required, unique, allowed-values, and
   forbidden-values checks.

Downstream semantic-data QA is out of scope for the first version.

## Proposed public API

```python
from dataprepkit.validation import (
    ColumnDefinition,
    ColumnValidation,
    ComparisonConfig,
    CellComparison,
    CellLocation,
    DataBoundary,
    ExpectedCellCheck,
    HeaderPolicy,
    RuntimePolicy,
    SheetSelector,
    SheetPolicy,
    TableConfig,
    WorkbookFeaturePolicy,
    WorkbookCheck,
    WorkbookValidationConfig,
    load_workbook_pair,
    validate_excel,
    validate_workbook,
)

config = WorkbookValidationConfig(
    config_version="1",
    comparison=ComparisonConfig(
        case_sensitive=False,
        accent_sensitive=True,
        trim_whitespace=True,
        collapse_internal_whitespace=False,
        empty_string_is_null=True,
        null_tokens=[],
        collation_name="configured-sql-server-collation",
    ),
    sheet_policy=SheetPolicy(
        required_selectors=[
            SheetSelector(mode="exact", value="SelectCompany"),
            SheetSelector(mode="exact", value="Quarterly_Data"),
        ],
        ignored_selectors=[],
        extra_sheet_action="ignore",
        selector_match_action="all",
    ),
    workbook_checks=[
        WorkbookCheck(
            rule_code="formula_error",
            enabled=True,
            scope="all_sheets",
            options={"error_tokens": ["#DIV/0!", "#VALUE!"]},
        ),
    ],
    expected_cells=[
        ExpectedCellCheck(
            name="organisation_code",
            locations=[
                CellLocation(
                    sheet_selector=SheetSelector(
                        mode="exact", value="Quarterly_Data"
                    ),
                    cell_reference="Z11",
                ),
            ],
            fallback_strategy="exactly_one",
            expected_value="configured-value",
            comparison=CellComparison(mode="case_insensitive"),
            enabled=True,
        ),
    ],
    enabled_rules=None,
    rule_severity={},
    compatibility=None,
    runtime=RuntimePolicy(
        max_cells_scanned=10_000_000,
        read_only=False,
        macro_policy="reject",
        missing_formula_cache_action="not_run",
        feature_policy=WorkbookFeaturePolicy(
            external_links="ignore",
            charts="ignore",
            pivot_tables="ignore",
            named_ranges="ignore",
            merged_cells="warning",
            macros="reject",
        ),
    ),
    tables=[
        TableConfig(
            name="outputs",
            sheet_selector=SheetSelector(mode="regex", value=r"^F_Outputs"),
            required=True,
            header_row=2,
            data_boundary=DataBoundary(
                mode="last_non_empty_row", columns=["reference"]
            ),
            data_presence="require_one_usable_row",
            column_definitions=[
                ColumnDefinition(name="unit", aliases=["Unit"]),
                ColumnDefinition(name="reference", aliases=["Reference"]),
            ],
            header_policy=HeaderPolicy(
                required_columns=["unit", "reference"],
                match_mode="contains_in_order",
                extra_column_action="allowed",
                missing_column_action="error",
            ),
            empty_row_rules=[],
            column_validations=[
                ColumnValidation(
                    column="unit",
                    allowed_values=["£", "%", "Ml", "Number"],
                    null_policy="error",
                ),
                ColumnValidation(
                    column="reference",
                    required=True,
                    unique=True,
                    null_policy="error",
                ),
            ],
        )
    ],
)

workbooks = load_workbook_pair(
    candidate_path="candidate.xlsx",
    reference_path="reference.xlsx",
)

result = validate_workbook(workbooks=workbooks, config=config)
```

For the common file-based workflow, use the one-call facade:

```python
result = validate_excel(
    candidate_path="candidate.xlsx",
    config=config,
    reference_path="reference.xlsx",
)
```

`validate_excel` loads the candidate and optional reference workbook, runs
`validate_workbook`, and closes all workbook views before returning or raising.
Its signature is:

```python
validate_excel(
    candidate_path,
    config=None,
    reference_path=None,
    candidate_version=None,
    reference_version=None,
    run_id=None,
    profile=None,
)
```

`config` may be a `WorkbookValidationConfig`, a JSON/YAML path, or a
JSON/YAML-compatible mapping. `profile` may be a pinned profile name, path, or
`CompatibilityProfile`; it is used when `config` is omitted or as the base for
the caller configuration. At least one of `config` or `profile` is required.
Workbook loading behavior comes from the resolved `config.runtime`; omitting
`reference_path` performs standalone validation. It must not mutate either
input file. Advanced callers needing in-memory workbooks, explicit lifetime
control, or multiple validations over one loaded pair should use
`load_workbook_pair` and `validate_workbook` directly.

The common quick-start path is:

```python
result = validate_excel(
    candidate_path="candidate.xlsx",
    config="validation.yaml",
)
if not result.is_valid:
    print(result.format_report())
```

For standalone validation, omit the reference workbook:

```python
workbooks = load_workbook_pair(candidate_path="candidate.xlsx")
result = validate_workbook(workbooks=workbooks, config=config)
```

The double workbook load is required because formula text and cached evaluated
values are different pieces of Excel data.

`load_workbook_pair` is the canonical loading API. It opens each available
workbook twice:

- `data_only=False` for formulas;
- `data_only=True` for cached evaluated values.

The returned `WorkbookPair` contains `candidate_formulas`, `candidate_values`,
and optional `reference_formulas` and `reference_values`. The exported
`WorkbookPair` constructor is also a supported public API for callers that
already hold both workbook views in memory.

Version metadata is supplied by the caller:

```python
workbooks = load_workbook_pair(
    candidate_path="candidate.xlsx",
    reference_path="reference.xlsx",
    candidate_version="candidate-v1",
    reference_version="reference-v3",
)
```

```python
workbooks = load_workbook_pair(
    reference_path="reference.xlsx",
    candidate_path="candidate.xlsx",
)

result = validate_workbook(workbooks=workbooks, config=config)
```

## Configuration design

Use typed configuration models, preferably Pydantic models because
`dataprepkit` already uses Pydantic for metadata validation.

Configuration should support:

- exact sheet names and regular-expression sheet selectors;
- configurable header rows;
- aliases for columns whose names vary between reference versions;
- expected cell locations with fallback locations;
- required, unique, allowed-values, and forbidden-values column rules;
- optional rule severity and rule enablement;
- reference-specific configuration without changing validation code.

There should be no hidden business defaults. A validation configuration must
make its policy explicit, including ignored sheets, table selectors, header
locations, fallback locations, comparison rules, and enabled checks. Technical
implementation defaults such as an empty result collection are acceptable, but
they must not alter validation meaning.

### Configuration fields

`WorkbookValidationConfig` should define the following complete top-level
fields:

```python
WorkbookValidationConfig(
    config_version=str,
    profile_name=str | None,
    comparison=ComparisonConfig,
    sheet_policy=SheetPolicy,
    tables=list[TableConfig],
    expected_cells=list[ExpectedCellCheck],
    workbook_checks=list[WorkbookCheck],
    enabled_rules=list[str] | None,
    rule_severity=dict[str, str],
    runtime=RuntimePolicy,
    compatibility=CompatibilityProfile | None,
)
```

The fields have these responsibilities:

- `config_version`: version of the configuration schema;
- `profile_name`: optional human-readable compatibility/profile name;
- `comparison`: case, accent, whitespace, blank, and null comparison policy;
- `sheet_policy`: required sheets, ignored sheets, extra-sheet handling, and
  selector matching rules;
- `tables`: table selectors, boundaries, headers, aliases, and column rules;
- `expected_cells`: named cell checks and fallback strategies;
- `workbook_checks`: enabled structural, formula, and standalone checks;
- `enabled_rules`: optional allow-list for rule codes;
- `rule_severity`: optional severity override by rule code.
- `runtime`: resource limits and workbook loading policy;
- `compatibility`: optional pinned external behaviour profile.

`ComparisonConfig` should define `case_sensitive`, `accent_sensitive`,
`trim_whitespace`, `collapse_internal_whitespace`, `empty_string_is_null`,
`null_tokens`, and the target collation name.

`SheetPolicy` should define `required_selectors`, `ignored_selectors`,
`extra_sheet_action`, and `selector_match_action`.

`TableConfig` should define `name`, `sheet_selector`, `required`,
`header_row`, `data_boundary`, `column_definitions`, `column_validations`,
`header_policy`, `empty_row_rules`, and `data_presence`.

`DataBoundary` is a typed model with `mode` and only the mode-specific field:
`columns` for `last_non_empty_row`, `end_row` for `fixed_end_row`, or
`table_name` for `excel_table`. Supplying fields for another mode is a
configuration error.

`DataBoundary.columns` contains logical column names declared by the enclosing
`TableConfig`; it never contains physical Excel letters or header text.
`header_row` is a positive, one-based Excel row number. `end_row` uses the same
one-based numbering and must not precede `header_row`.

`column_definitions` is required for every configured table. Header policies and
column validations refer only to its logical names; physical Excel headers are
resolved through the declared aliases.

`ExpectedCellCheck` should define `name`, `locations`, `fallback_strategy`,
`expected_value`, `comparison`, `enabled`, and `severity`.

`CellLocation` should define a `sheet_selector` and an A1
`cell_reference`. The locations list is ordered and is interpreted according
to the check's `fallback_strategy`.

`CellComparison` should define `mode` (`exact`, `normalised`,
`case_insensitive`, `numeric`, or `date`), plus any mode-specific parsing
options. `ExpectedCellCheck.comparison` must use this model.

The comparison modes mean:

- `exact`: compare values without normalisation;
- `normalised`: apply the configured whitespace and null policy;
- `case_insensitive`: apply normalisation and case-insensitive text comparison;
- `numeric`: parse both values as numbers using configured decimal rules;
- `date`: parse both values using configured date formats and timezone policy.

Numeric comparison options include `decimal_separator`, `thousands_separator`,
and `allow_numeric_strings`. Date comparison options include `accepted_formats`
and `timezone`. These options must be explicit for `numeric` and `date` modes.

`WorkbookCheck` should define `rule_code`, `enabled`, `scope`, optional
`options`, and optional `severity`. Options are required only when the selected
scope or rule needs them; `formula_error` requires its configured error-token
list even with `scope="all_sheets"`. Rule-specific options must be implemented as typed
models selected by `rule_code` (a discriminated union), not as an unvalidated
free-form dictionary. Configuration validation must reject rule codes that are
neither built-in nor registered,
conflicting options, invalid selectors, duplicate names, and ambiguous column
aliases.

`RuntimePolicy` should define `max_cells_scanned`, `read_only`,
`macro_policy`, and `missing_formula_cache_action`. `macro_policy` accepts
`preserve`, `discard`, or `reject`. `missing_formula_cache_action` accepts
`not_run` or `error` and must be explicitly configured.
`feature_policy` should be a typed `WorkbookFeaturePolicy`.

`RuntimePolicy.macro_policy` controls how `.xlsm` files are opened:
`preserve`, `discard`, or `reject`. `WorkbookFeaturePolicy.macros` controls the
validation outcome after loading: `ignore`, `warning`, or `error`. These are
independent settings and must not be merged or allowed to override one another.
`CompatibilityProfile` should define `name`, `repository_url`,
`commit_sha`, `defined_on`, `fixture_identifier`, and the complete profile
configuration.

Rule configuration has one canonical location. Table-level rules belong in
`TableConfig` fields such as `header_policy` and `empty_row_rules`; column-level
rules belong in `TableConfig.column_validations`; workbook-level rules belong
in `WorkbookValidationConfig.workbook_checks`. Duplicate declarations of the
same rule and scope are rejected.

Rule enablement and severity precedence is:

1. invalid configuration fails before validation;
2. `enabled_rules`, when supplied, acts as the rule allow-list;
3. an individual check's `enabled` value applies within that allow-list;
4. an individual check's `severity` overrides the optional `rule_severity`
   mapping;
5. an unspecified severity remains unset rather than receiving a hidden
   default.

`enabled_rules=None` means no allow-list is applied. An explicitly supplied
`enabled_rules=[]` means no rules are enabled. This distinction must be
preserved when loading JSON/YAML configuration.

Every name in `enabled_rules` and `rule_severity` must resolve to a built-in or
registered rule. `rule_severity` values are limited to `error` and `warning`;
duplicate names after the configured comparison policy are rejected.

The allow-list applies uniformly to rules configured through
`WorkbookCheck`, `ExpectedCellCheck`, `TableConfig`, and
`TableConfig.column_validations`. A configured check that is excluded by the
allow-list is not executed and is reported as `NOT_RUN` with reason
`RULE_DISABLED`. `SheetPolicy.required_selectors` is input-integrity policy,
not an optional rule: it always resolves, and a missing required sheet is
reported as `required_sheet`.

An explicitly configured check with `enabled=False` follows the same
`NOT_RUN/RULE_DISABLED` behavior. Disabled checks retain their rule code and
configured location metadata so callers can distinguish deliberate disabling
from a rule that was not configured.

Rule resolution first checks the built-in rule catalogue, then the process
custom-rule registry. A custom rule may use a name not present in the built-in
catalogue; an unregistered name remains a `ConfigurationError`. The same
resolution applies to `WorkbookCheck.rule_code` and `ColumnValidation.check`.

`scope` is configured only on `WorkbookCheck`. Its accepted values are
validated against the selected rule; rule-option models must not repeat it.
`ExpectedCellCheck` is the canonical configuration for expected-cell checks.
Each named check is active when `enabled=True` and is subject to the global
`enabled_rules` allow-list; it does not require a second `WorkbookCheck`.

Workbook readability, input consistency, resource limits, and feature-detection
availability always run regardless of `enabled_rules`. The allow-list controls
only validation rules.

Nested configuration fields should be defined as follows:

- `ComparisonConfig`: `case_sensitive`, `accent_sensitive`,
  `trim_whitespace`, `collapse_internal_whitespace`, `empty_string_is_null`,
  `null_tokens`, and `collation_name`;
- `SheetPolicy`: `required_selectors`, `ignored_selectors`,
  `extra_sheet_action`, and `selector_match_action`;
- `SheetSelector`: `mode`, `value`, `case_sensitive`, and
  `expected_match_count`;
- `ColumnDefinition`: logical `name`, physical `aliases`, and optional
  `comparison: ComparisonConfig` override;
- `ColumnValidation`: logical `column`, zero or one of `allowed_values` or
  `forbidden_values`, optional `required`, `unique`, `null_policy`, `check`,
  `check_options`, and `severity`;
- `TableConfig`: `name`, `sheet_selector`, `required`, `header_row`,
  `data_boundary`, `column_definitions`, `column_validations`,
  `header_policy`, `empty_row_rules`, and `data_presence`;
- `HeaderPolicy`: `required_columns`, `match_mode`, `extra_column_action`, and
  `missing_column_action`;
- `EmptyRowRule`: `rows`, `excluded_columns`, and `blank_policy`;
- `ExpectedCellCheck`: `name`, A1 `locations`, `fallback_strategy`,
  `expected_value`, typed `comparison`, `enabled`, and `severity`;
- `CellLocation`: `sheet_selector` and A1 `cell_reference`;
- `WorkbookCheck`: `rule_code`, `enabled`, `scope`, `options`, and optional
  `severity`.

`TableConfig.header_policy` is the canonical configuration for
`column_header`, and `TableConfig.empty_row_rules` is the canonical
configuration for `empty_row_pattern`. `TableConfig.data_presence` is the
canonical configuration for `non_empty_data`. These checks are enabled by the
corresponding table configuration and must not be duplicated in
`WorkbookCheck`. `WorkbookCheck` is reserved for workbook-level checks such as
formula and structure checks.

The configuration must use JSON/YAML-compatible lists rather than sets for
serialisable value collections. Duplicate values after normalisation must be
rejected.

The accepted policy values are:

- `extra_sheet_action`: `ignore`, `warning`, or `error`;
- `selector_match_action`: `all`, `first`, or `error_on_multiple`;
- `fallback_strategy`: `ordered_first`, `exactly_one`, or `all_must_agree`;
- `data_boundary.mode`: `last_non_empty_row`, `fixed_end_row`, or
  `excel_table`;
- `ColumnValidation.check`: a registered rule name or null;
- `ColumnValidation.null_policy`: `ignore` or `error`;
- `severity`: `error`, `warning`, or omitted. Omitted severity uses the
  validator's normal failure category (`error`).
- `match_mode`: `exact_order`, `contains_in_order`, or `contains_any_order`;
- `blank_policy`: `none_only`, `blank_strings`, or `blank_strings_and_nulls`;
- `data_presence`: `allow_empty` or `require_one_usable_row`;
- `null_policy`: `ignore` or `error`;
- `scope`: a rule-specific enumerated scope validated by that rule.

Boolean rule fields are enabled only when explicitly set to `True`. Supplying
`required=False` or `unique=False` does not enable that rule and does not count
towards the requirement for at least one active column validation.

`ColumnValidation(required=True)` is the public configuration for the
`missing_value` rule. Its emitted events use `rule_code="missing_value"`.
Likewise, `unique=True` emits `duplicate_value` events. These mappings are
fixed API semantics, not implementation-specific names.

`ColumnValidation` constraints are:

- `column` is a logical column name resolved through `ColumnDefinition`;
- at most one of `allowed_values` and `forbidden_values` may be configured;
- at least one of `required`, `unique`, `allowed_values`,
  `forbidden_values`, or `check` must be configured;
- `allowed_values` and `forbidden_values` must be non-empty lists;
- null values in those lists are rejected; null handling is configured
  separately;
- `required` and `unique` may be combined;
- duplicate rules for the same logical column and rule code are rejected.

`null_policy` applies to uniqueness and value-list checks. It must be explicit
when `unique`, `allowed_values`, or `forbidden_values` is enabled.

`ColumnDefinition` aliases must be unique after the configured header
normalisation. A physical header may resolve to only one logical column.

Comparison-policy precedence is explicit: a rule-specific comparison setting
overrides a column-level `ColumnDefinition.comparison` override, which
overrides the top-level `ComparisonConfig`. Unspecified fields inherit from
the next broader policy. No rule may silently change case, accent, whitespace,
or null handling. `ExpectedCellCheck.comparison` is a complete rule-specific
comparison and therefore takes precedence over the top-level policy for that
check.

Selectors use a typed `SheetSelector` model:

```python
SheetSelector(
    mode="exact",              # or "regex"
    value="Quarterly_Data",
    case_sensitive=False,
    expected_match_count=1,
)
```

All sheet selector fields use this model, including required and ignored
selectors. A selector with no matches or an unexpected number of matches is a
resolution error unless its policy explicitly allows that result.

A sheet matching both a required selector and an ignored selector is a
configuration error. Required and ignored selector sets must be disjoint after
their configured matching policy is applied.

Ignored-sheet selectors are applied before table and workbook-rule resolution.
An ignored sheet is excluded from all validation rules, including a table
selector that would otherwise match it. A table selector matching only ignored
sheets is treated as having no matches; it is an error when that table is
`required=True` and is skipped otherwise. A required selector that resolves to
an ignored sheet is rejected as a configuration error rather than silently
being ignored.

Extra-sheet and missing-reference-sheet comparisons apply only after ignored
selectors have been removed from both candidate and reference sheet sets.

`SheetSelector.expected_match_count` takes precedence when supplied. If it is
omitted, `SheetPolicy.selector_match_action` determines whether all matches,
the first match, or multiple-match failure is used.

Exact selectors compare the complete sheet name. Regex selectors use Python
regular-expression `fullmatch` semantics after applying the configured case
policy. Invalid regular expressions are configuration errors with the selector
value included in the message.

`TableConfig.data_boundary` must contain:

- `mode="last_non_empty_row"` and the columns used to identify data; or
- `mode="fixed_end_row"` and `end_row`; or
- `mode="excel_table"` and the Excel table name.

For `mode="excel_table"`, `table_name` is matched exactly against the
workbook's declared Excel table names. The table must be on the sheet selected
by the enclosing `TableConfig`; a missing or cross-sheet match produces
`table_resolution`. The declared Excel table range supplies the data boundary;
`header_row` must agree with its header row.

`EmptyRowRule.rows` uses Excel's one-based row numbers. Its
`excluded_columns` uses Excel column letters (`A`, `B`, ..., `Z`, `AA`, etc.).

`CellLocation.cell_reference` accepts an unqualified Excel A1 reference with
an optional absolute marker on either axis (for example `Z11`, `$Z$11`,
`Z$11`, or `$Z11`). Sheet-qualified references are invalid because the sheet
is supplied by `sheet_selector`. References are canonicalised to uppercase
column letters with absolute markers preserved.

Rule-specific options must be typed rather than left as an unvalidated generic
dictionary:

The workbook option models are `FormulaDifferenceOptions`,
`FormulaErrorOptions`, and `SheetStructureOptions`. They form a discriminated
union selected by
`WorkbookCheck.rule_code`; each model rejects options belonging to another
rule.

- `formula_difference`: optional configured sheet selectors when
  `WorkbookCheck.scope="configured_sheets"`;
- `formula_error`: optional configured sheet selectors when
  `WorkbookCheck.scope="configured_sheets"`, plus an explicit list of cached
  Excel error tokens to detect;
- `sheet_structure`: `compare_headers` and optional table scope;
- `column_header`: uses the containing table's `header_policy`;
- `empty_row_pattern`: uses the containing table's `empty_row_rules`;
`duplicate_value`, `missing_value`, `allowed_values`, and `forbidden_values`
are column-level rules and are configured only through
`TableConfig.column_validations`. They must not be repeated as
`WorkbookCheck.options`.

`WorkbookCheck.scope` uses these rule-specific public values:

- `formula_difference`: `overlapping_sheets` or `configured_sheets`;
- `formula_error`: `all_sheets` or `configured_sheets`;
- `sheet_structure`: `overlapping_sheets` or `configured_sheets`;
- registered custom workbook rules: the scope values declared at registration.

The validator rejects a scope that is not declared by the selected built-in or
registered rule before opening the workbook.

The complete built-in option fields are:

- `FormulaDifferenceOptions`: `sheet_selectors`, required and non-empty when
  the scope is `configured_sheets` and forbidden for
  `overlapping_sheets`; its type is `list[SheetSelector]` and selector
  matching follows the declared `SheetSelector` policy;
- `FormulaErrorOptions`: `error_tokens` and optional `sheet_selectors`, with
  selectors required when the scope is `configured_sheets`, using
  `list[SheetSelector]` and the declared selector policy;
- `SheetStructureOptions`: `compare_headers` and optional `table_names`; when
  supplied, each table name must resolve to one declared `TableConfig`.

`HeaderPolicy` must define `match_mode` as one of `exact_order`,
`contains_in_order`, or `contains_any_order`, `required_columns`,
`extra_column_action` (`allowed`, `warning`, or `error`), and
`missing_column_action` (`warning` or `error`). Duplicate or blank physical
headers are always a resolution error unless explicitly handled by a configured
alias policy. `required_columns` contains logical column names.

`WorkbookFeaturePolicy` must define an action of `ignore`, `warning`, or `error`
for each supported feature: `external_links`, `charts`, `pivot_tables`,
`named_ranges`, `merged_cells`, and `macros`.

`FormulaErrorOptions.error_tokens` is a required, non-empty list of the exact
cached Excel error values to report. The validator must not embed a hidden
error-token list; compatibility profiles provide the tokens needed to match
`dqchecks`. Token matching is exact and case-sensitive; tokens are compared as
Excel error values, not through the SQL text normalisation policy.

### Initial rule catalogue

The initial built-in rule codes and their reference requirements are:

| Rule code | Purpose | Reference required |
| --- | --- | --- |
| `formula_difference` | Compare formulas cell by cell | Yes |
| `formula_error` | Find cached Excel formula errors | No |
| `required_sheet` | Find configured required sheets absent from the candidate | No |
| `missing_reference_sheet` | Find sheets present in the reference but absent from the candidate | Yes |
| `sheet_structure` | Compare used areas and optional headers | Yes |
| `expected_cell_value` | Compare a configured cell with an expected value | No |
| `column_header` | Validate configured table headers | No |
| `empty_row_pattern` | Validate configured empty rows | No |
| `non_empty_data` | Require at least one usable row in a configured table | No |
| `duplicate_value` | Detect duplicate values in a configured column | No |
| `missing_value` | Detect null or blank values in a configured column | No |
| `allowed_values` | Require values to be in a configured list | No |
| `forbidden_values` | Reject values in a configured list | No |

Reference-dependent rules enabled without a reference workbook must be marked
`NOT_RUN` and reported as such. A configuration may instead explicitly disable
those rules for standalone validation.

`allowed_values` and `forbidden_values` are alternative checks and must not be
configured together for the same column. Configuration validation must reject
this combination before the workbook is inspected:

```python
# Valid
ColumnValidation(column="Unit", allowed_values=["£", "%", "Ml", "Number"])
ColumnValidation(column="Status", forbidden_values=["Deleted", "Rejected"])

# Invalid configuration
ColumnValidation(
    column="Status",
    allowed_values=["Active", "Pending"],
    forbidden_values=["Deleted"],
)
```

Both checks use the configured SQL Server comparison policy. For a
case-insensitive target collation, values such as `deleted`, `Deleted`, and
`DELETED` match the same forbidden value. The original workbook value should
be retained in the resulting validation event.

Excel locations and column names cannot be removed entirely because they are
part of the workbook contract. They should, however, be declared once at the
configuration boundary rather than embedded in rule implementations.

### SQL Server comparison policy

The validated Excel data will be loaded into a case-insensitive SQL Server
database. Validation must therefore use an explicit SQL-aligned normalisation
policy rather than Python's default case-sensitive comparisons.

The recommended policy for the SQL Server target is:

- text identifiers and text values are compared case-insensitively using
  Unicode `casefold()`;
- leading and trailing whitespace is removed before validation;
- empty strings and whitespace-only strings are treated as null;
- trailing spaces are normalised so uniqueness matches SQL Server comparison
  behaviour more closely;
- accents are not removed unless the target SQL Server collation is explicitly
  configured as accent-insensitive;
- original cell values are retained in validation events for diagnostics;
- column names, sheet selectors, aliases, allowed values, and uniqueness rules
  use the same configured comparison policy unless a rule overrides it.

The target database collation must be part of the configuration or deployment
contract. A case-insensitive database is not necessarily accent-insensitive,
and SQL Server collations can differ between environments. Python
normalisation is an explicit, deterministic approximation of the configured
collation; exact database comparison is not performed during workbook
validation. Compatibility tests must document any known differences.

The SQL Server comparison policy is configuration, not a built-in constant. The
`dqchecks` case-sensitive or reference-specific behaviours must be selected
through an explicit profile when compatibility is required.

Example:

```python
config = WorkbookValidationConfig(
    comparison=ComparisonConfig(
        case_sensitive=False,
        accent_sensitive=True,
        trim_whitespace=True,
        empty_string_is_null=True,
    ),
)
```

The normalised value should be used for comparison and uniqueness checks, but
the original value should be preserved for loading or error reporting according
to the configured ingestion policy.

Null handling must be explicit:

- `required` treats `None`, configured null tokens, empty strings, and
  whitespace-only strings as missing when `empty_string_is_null=True`;
- `unique` uses `null_policy="ignore"` or `null_policy="error"`;
- `allowed_values` and `forbidden_values` do not evaluate nulls as ordinary
  values; nulls are handled by `required` or an explicit null policy;
- Excel error values are never treated as valid nulls and are reported by the
  formula-error rule or as a value-type error;
- repeated internal whitespace is preserved unless a rule explicitly enables
  `collapse_internal_whitespace`.

### Column definitions and aliases

Business-facing rules should use stable logical column names. Physical Excel
headers should be declared in one place:

```python
ColumnDefinition(
    name="item_description",
    aliases=["Item description", "Item Description", "Description"],
)
```

Header resolution is case-insensitive and whitespace-normalised. Two physical
headers resolving to the same logical column are a configuration or data error
and must be reported before row-level validation.


## Validation model

Separate workbook access from validation logic.

`WorkbookPair` should contain:

```python
WorkbookPair(
    candidate_formulas=Workbook,
    candidate_values=Workbook,
    reference_formulas=Workbook | None,
    reference_values=Workbook | None,
    candidate_path=Path | None,
    reference_path=Path | None,
    candidate_version=str | None,
    reference_version=str | None,
    run_id=str | None,
)
```

`candidate_path` and `reference_path` are optional when workbooks are supplied
in memory. Validation events use the available filename; when no path is
available, `candidate_filename` is `None` and the caller may provide an
external `run_id`. If supplied, `run_id` is included in the validation result
and every event.

In-memory validation is a supported public workflow. Callers may construct
the exported `WorkbookPair` with candidate and optional reference formula and
value views, then pass it to `validate_workbook`. The same pairing invariant
and ownership rules apply as when using `load_workbook_pair`.

Both candidate views are mandatory: `candidate_formulas` must be the
`data_only=False` view and `candidate_values` must be the `data_only=True`
view. If a reference workbook is supplied, both reference views are mandatory
as well. The package never creates missing views for an in-memory pair;
callers needing that behavior must use `load_workbook_pair`.

`load_workbook_pair` must either provide both reference views or neither. It
must raise a clear read error for unreadable files, close workbooks on failed
construction, and provide an explicit `close()` method for successful use.
The loader must not calculate formulas. It detects formula cells with missing
cached values by comparing the formula view with the values view.
Formula-dependent rules then become
`NOT_RUN` with reason `FORMULA_RESULTS_UNAVAILABLE`.

`WorkbookPair` should also implement the context-manager protocol:

```python
with load_workbook_pair(candidate_path="candidate.xlsx") as workbooks:
    result = validate_workbook(workbooks=workbooks, config=config)
```

The caller or context manager owns workbook lifetime. `validate_workbook` must
never close a `WorkbookPair`; callers using the object without a context
manager must call `close()` explicitly.

`WorkbookPair.close()` is idempotent. Validation after the pair is closed
raises `WorkbookReadError` before rule execution, and closing a pair after a
failed validation is still safe. Exiting its context manager always closes all
opened views, including views opened before a later load failure.

Valid `WorkbookPair` inputs contain both candidate views and either both
reference views or no reference views. A reference path may be used only when
the loader opens the reference views; it cannot be combined with manually
supplied reference workbooks. Invalid combinations fail before rule execution.

`.xlsx` and `.xlsm` are supported; `.xls` is rejected with a clear unsupported
format error. Macro preservation for `.xlsm` must be configurable.

`RuntimePolicy.read_only=True` opens all workbook views in read-only mode and
forbids mutation by the loader or validator. It may reduce available feature
inspection and must produce `FEATURE_DETECTION_UNAVAILABLE` when a configured
feature cannot be determined, rather than silently changing the feature
policy. `read_only=False` permits inspection requiring normal workbook access
but validation remains non-mutating.

`openpyxl` is an optional dependency exposed through the `dataprepkit[excel]`
extra. Importing the validation models does not require it; APIs that read
Excel files raise a clear dependency error instructing callers to install the
extra. The supported loader and all Excel integration tests use that extra.

Suggested responsibilities:

- `workbook.py`: openpyxl workbook access and used-area resolution;
- `config.py`: typed validation configuration models;
- `models.py`: validation results and events;
- `rules.py`: generic, independently testable rules;
- `tables.py`: table discovery, header resolution, and row/cell mapping;
- `__init__.py`: public exports.

Possible package layout:

```text
dataprepkit/
    validation/
        __init__.py
        config.py
        models.py
        workbook.py
        rules.py
        tables.py
```

Rules should operate on resolved workbook abstractions or simple values where
possible, rather than reaching directly into workbook cells. This keeps them
small and easy to unit test.

The supported public exports from `dataprepkit.validation` are:

```python
WorkbookValidationConfig
ComparisonConfig
RuntimePolicy
CompatibilityProfile
WorkbookFeaturePolicy
SheetSelector
SheetPolicy
TableConfig
DataBoundary
ColumnDefinition
ColumnValidation
HeaderPolicy
EmptyRowRule
ExpectedCellCheck
WorkbookCheck
CellLocation
CellComparison
WorkbookPair
ValidationResult
ValidationEvent
DiagnosticEvent
RuleContext
ValidationPackageError
ConfigurationError
WorkbookReadError
DependencyError
ValidationFailedError
load_validation_config
dump_validation_config
dump_validation_result
load_validation_result
validate_config
get_config_schema
list_profiles
describe_profile
register_rule
unregister_rule
registered_rules
load_workbook_pair
validate_excel
validate_workbook
```

Anything else in the validation package is internal and must not be required by
consumer code or public-API tests.

The exported exception types are raised before validation begins:
`ConfigurationError` covers invalid models, profiles, selectors, rule
registrations, and serialized documents; `WorkbookReadError` covers unreadable
or unsupported workbook inputs; and `DependencyError` covers an unavailable
optional Excel/YAML dependency. All expose a stable error code and a concise,
actionable message. Each inherits from `ValidationPackageError`, whose public
fields are `code` and `message`; `ConfigurationError` also inherits from
`ValueError`, `WorkbookReadError` from `OSError`, and `DependencyError` from
`ImportError`. Exceptions are raised for pre-validation failures and are not
placed in `ValidationResult`. `ValidationFailedError` is raised only by
`ValidationResult.raise_if_invalid()`, inherits from `ValidationPackageError`,
has code `VALIDATION_FAILED`, and exposes the originating `ValidationResult`.

`ConfigurationError` messages include a machine-readable `field_path` using
dot and list-index notation, for example
`tables[0].column_validations[1].allowed_values`, plus the failed constraint
and a concise remediation message. Paths identify configuration fields, never
implementation details or private helper names.

The rule-option models (`FormulaDifferenceOptions`, `FormulaErrorOptions`, and
`SheetStructureOptions`) are internal implementation models. Users configure
rules through `WorkbookCheck`, `TableConfig`, `ColumnValidation`, and
`ExpectedCellCheck`, or through the serialised compatibility profile. Public
constructors and profile loaders are responsible for creating and validating
the internal option models.

`load_validation_config(source, compatibility_profile=None)` is the public
JSON/YAML configuration loader and returns a validated
`WorkbookValidationConfig`. `source` may be a `.json`, `.yaml`, or `.yml` path,
or a JSON/YAML-compatible mapping. `compatibility_profile` may be a pinned
profile object, profile path, or profile mapping. The loader performs a deep
caller-overrides-profile merge, validates the merged result, and raises a
public configuration error for unsupported formats, malformed documents,
missing files, or invalid values.

`dump_validation_config(config, format="mapping")` returns a validated
JSON/YAML-compatible mapping, JSON text, or YAML text according to `format`.
Accepted formats are `mapping`, `json`, and `yaml`; unsupported formats raise
`ConfigurationError`. JSON output uses deterministic key ordering and UTF-8;
YAML output uses a deterministic safe mapping representation. Round-tripping
must preserve omitted fields versus explicit nulls. JSON support has no extra
dependency; YAML support is included in the `dataprepkit[excel]` extra and
reports a clear dependency error when that extra is absent.

When merging a compatibility profile, mappings merge recursively, scalar
values and explicit `null` replace profile values, and lists replace profile
lists in their entirety. Lists are never concatenated implicitly, preventing
duplicate rules or selectors from appearing unexpectedly.

`validate_config(source, compatibility_profile=None)` validates configuration
without opening a workbook and returns the resolved `WorkbookValidationConfig`.
It reports the same field paths and configuration errors as `validate_excel`.
The package also publishes a JSON Schema for editor autocomplete and inline
YAML/JSON validation; the schema is versioned with `config_version` and must
reject fields not supported by that schema version.

`get_config_schema(version=None)` returns the JSON Schema as a JSON-compatible
mapping. When `version` is omitted, it returns the current schema; unsupported
versions raise `ConfigurationError`.

`list_profiles()` returns deterministic metadata for available pinned profiles.
`describe_profile(name)` returns the profile purpose, source revision, required
sheets, configured tables, enabled rule codes, and expected workbook mode
without opening a workbook. Unknown profile names raise `ConfigurationError`.

`register_rule(name, validator, options_model=None,
attachment_scopes=("column",), execution_scopes=None)`
registers a named custom rule for the current process. Names must be
non-empty, stable, and
unique; duplicate registration is a configuration error and replacement is
not supported. Registration is synchronised and applies to the current Python
process only. `attachment_scopes` must contain one or more of `column` or
`workbook`. Column-scoped rules are referenced by `ColumnValidation.check`;
workbook-scoped rules are referenced by `WorkbookCheck.rule_code`.
`execution_scopes` declares the values accepted by `WorkbookCheck.scope` for a
workbook rule. It is required for workbook-scoped rules and ignored for
column-scoped rules. These are distinct from attachment scope. Validators
receive resolved public data and metadata.
`options_model` validates `ColumnValidation.check_options` or
`WorkbookCheck.options`, according to the attachment point. Registration is
not serialised into a configuration file; a process must register a custom
rule before validating a configuration that refers to it. Custom-rule events
use the registered name as their stable `rule_code`.

`unregister_rule(name)` removes a previously registered custom rule and may not
remove built-in rules. It returns `True` when a custom rule was removed and
`False` when the name was not registered. `registered_rules()` returns a
deterministic tuple of registered custom names. Both APIs are public to support
application startup and test teardown; registrations otherwise persist for the
lifetime of the Python process. Registry operations are synchronised and
unregistering a rule does not affect an already-running validation call.

The public validator contract is:

```python
def validator(context: RuleContext, options: object | None) -> list[ValidationEvent]:
    ...
```

`options_model`, when supplied, must expose
`validate(value: Mapping[str, object] | None) -> object` and
`dump(value: object) -> Mapping[str, object]`. Validation must reject unknown
fields and return a stable, serialisable options object. Pydantic models may
implement this protocol, but callers are not required to depend on Pydantic.

`RuleContext` is an immutable public model containing `run_id`, `rule_code`,
`attachment_scope`, `execution_scope`, `candidate_filename`, optional
`sheet_name`, optional table and logical-column names, the original and
normalised values where applicable, Excel row/cell locations, the comparison
policy, and reference availability. Validators may read but must not mutate
the context or workbook objects. Validators must return only public
`ValidationEvent` objects; an exception raised by a custom validator becomes a
`VALIDATION_ERROR` inspection event and does not expose a traceback to normal
consumers.

`RuleContext` is constructed by the validator and is not required as a caller
configuration input. It is serialisable as metadata, but never contains live
workbook objects. A custom validator may return single-cell or grouped events.
The framework validates returned locations against the context, forces the
registered rule name as `rule_code`, rejects caller-supplied `PASSED` events,
and reports malformed output as `CUSTOM_RULE_OUTPUT_INVALID`.

Public extension example:

```python
register_rule(
    "valid_reference",
    validate_reference,
    options_model=ReferenceOptions,
)

config = load_validation_config("validation.yaml")
result = validate_workbook(workbooks=workbooks, config=config)
```

Rule execution should follow this order:

1. workbook readability;
2. sheet resolution;
3. used-area resolution;
4. table and header resolution;
5. structural checks;
6. formula checks;
7. row and column checks.

Rules that depend on a failed resolution step should be marked `NOT_RUN` and
must not emit misleading secondary validation errors.

When `max_cells_scanned` is exceeded, validation stops immediately and returns
the events collected so far plus one `WORKBOOK_LIMIT_ERROR`. Partial results
are explicitly marked incomplete; no later rules are executed.

The limit event includes the configured limit, observed count, stopped rule
code, and a human-readable reason.

## Multiple-error reporting

Validation should continue after ordinary rule failures and collect all issues
that can be evaluated reliably. Each issue should be represented by a separate
`ValidationEvent`.

```python
ValidationEvent(
    rule_code="allowed_values",
    severity="error",
    sheet_name="F_Outputs",
    cell_reference="H27",
    column_name="Unit",
    row_number=27,
    actual_value="GBP",
    expected_value=["£", "%", "Ml", "Number"],
    description="Value is not in the allowed values",
)
```

For column rules, report one event per invalid cell or row. This allows users
to correct all identified issues in one submission cycle.

For duplicate-value checks, preserve the `dqchecks` grouped behaviour: emit one
event per duplicated normalised value and include all affected Excel row
numbers and A1 cell references in the event. Other row-level rules emit one
event per affected cell.

The result object should expose:

```python
result.is_valid
result.errors
result.warnings
result.not_run
```

`ValidationResult` also provides the convenience methods
`to_dict()`, `format_report()`, and `raise_if_invalid()`. `to_dict()` is
equivalent to `dump_validation_result(result, format="mapping")`.
`format_report()` returns a deterministic human-readable summary containing
the counts, rule codes, locations, and descriptions of errors, warnings, and
not-run checks. `raise_if_invalid()` raises `ValidationFailedError` only when
`is_valid` is false; the exception retains the result for programmatic access.

`ValidationEvent` should include a `status` of `FAILED` or `NOT_RUN`.
`NOT_RUN` events must include the rule code and a reason, such as
`REFERENCE_WORKBOOK_REQUIRED`. They must not be counted as validation errors.

The public `ValidationEvent` fields include `rule_code: str`, `status`, an
optional `error_code`, optional `reason`, optional severity, description, expected and actual
values, candidate filename, sheet/table/column identity, and singular or
grouped Excel locations. `error_code` is reserved for framework or inspection
errors and is `None` for ordinary rule findings. Framework-generated failures
may include an opaque `diagnostic_id` that is stable within the run and contains
no exception text or sensitive data.

For custom-rule output, `rule_code` may be omitted on the returned event; the
framework fills it with the registered name. A supplied different rule code is
rejected. Every returned event must otherwise satisfy the public event model.
The validator adapter accepts this omission only at the custom-rule boundary;
all events exposed in `ValidationResult` have a non-empty `rule_code`.
Malformed output produces a failed event with the registered rule code and
`error_code="CUSTOM_RULE_OUTPUT_INVALID"`. If the validator raises an
exception, the framework catches it and emits a failed event with
`error_code="CUSTOM_RULE_EXCEPTION"`; the exception text is not exposed by
default.

Its serialised shape is stable and contains:

```python
{
    "run_id": str | None,
    "rule_code": str,
    "attachment_scope": "column" | "workbook",
    "execution_scope": str | None,
    "candidate_filename": str | None,
    "sheet_name": str | None,
    "table_name": str | None,
    "column_name": str | None,
    "row_numbers": list[int],
    "cell_references": list[str],
    "original_values": list[object],
    "normalised_values": list[object],
    "comparison": dict[str, object],
    "reference_available": bool,
}
```

Empty collections are used instead of null for grouped locations and values;
identity fields remain nullable when they do not apply. The context is
read-only during rule execution. Context values use a JSON-safe representation:
primitive values remain primitives, dates and datetimes become
`{"type": "date"|"datetime", "value": ISO-8601 text}`, decimals become
`{"type": "decimal", "value": text}`, and unsupported objects become
`{"type": "unsupported", "value": repr(text)}`. This representation is used
only for context metadata and does not replace the original workbook values in
validation events.

JSON-safe conversion is recursive: mappings become string-keyed mappings,
lists and tuples become lists, sets become sorted lists using their serialized
values, and nested dates, datetimes, decimals, and unsupported objects use the
same tagged representations. Mapping keys that are not strings are converted
to their string representation deterministically. Sets are ordered by the
canonical UTF-8 JSON encoding of each converted member, so mixed and nested
values have a stable order.

When locations are present, `row_numbers`, `cell_references`,
`original_values`, and `normalised_values` have the same length and each index
describes one location. Workbook-level contexts use empty lists for all four
collections. A context with mismatched collection lengths is invalid before
the custom validator is called. The runtime `RuleContext` and all nested
configuration/policy objects exposed through it are immutable; callers receive
read-only mappings and tuples rather than mutable aliases to validator state.

`dump_validation_result(result, format="mapping")` is the public result
serialization API and accepts the same `mapping`, `json`, and `yaml` formats
as configuration dumping. Event values use the same JSON-safe representation
as `RuleContext`; in-memory result objects retain their original Python
values. Serialization is deterministic and preserves event ordering.

Its mapping output has this stable shape:

```python
{
    "run_id": str | None,
    "complete": bool,
    "is_valid": bool,
    "config_version": str,
    "profile_name": str | None,
    "compatibility_source": str | None,
    "compatibility_commit": str | None,
    "candidate_version": str | None,
    "reference_version": str | None,
    "candidate_filename": str | None,
    "comparison": dict[str, object],
    "diagnostics": list[dict],
    "errors": list[dict],
    "warnings": list[dict],
    "not_run": list[dict],
}
```

The serialized `comparison` mapping contains every `ComparisonConfig` field,
including `case_sensitive`, `accent_sensitive`, whitespace and null policies,
`null_tokens`, and `collation_name`. It is metadata describing the run and is
not used to reinterpret or re-run the stored events.

`profile_name`, `compatibility_source`, and `compatibility_commit` are copied
from the selected `CompatibilityProfile`; they are all `None` when no profile
is used. A profile-backed result is not complete for audit purposes unless its
source and commit metadata are present and match the validated profile.

Each event mapping contains the documented public event fields, including
`rule_code`, `status`, `error_code`, `reason`, `diagnostic_id`, severity,
description, values, and location fields. `actual_value`, `expected_value`,
`original_value`, and `normalised_value` all use the same recursive JSON-safe
conversion as `RuleContext`. Deserialization is performed by
`load_validation_result(mapping)` and reconstructs a validated
`ValidationResult`; it never re-runs validation. It accepts a mapping, JSON
text, or YAML text, but not a workbook path. Deserialization recomputes
`is_valid` from the reconstructed error collection and rejects input whose
stored value disagrees. Tagged date, datetime, and decimal values are
reconstructed to their original Python types; unsupported values remain tagged
mappings because their original types cannot be safely recreated.

Deserialization requires every top-level key in the documented result shape.
`complete=False` is valid only when the result contains a fatal inspection
event, such as `WORKBOOK_READ_ERROR` or `WORKBOOK_LIMIT_ERROR`; contradictory
combinations are rejected as invalid serialized results. Event locations,
statuses, reason codes, and tagged values are validated before reconstruction.

Custom validators may return `FAILED` events to report findings or `NOT_RUN`
events when their own declared dependency is unavailable. They may not return
`PASSED` events. A malformed event is added to `errors`, validation continues
with independent rules, and `complete` remains `True` for either custom-rule
output failure or a caught custom-rule exception. Validation becomes incomplete
only when inspection must stop for a fatal resource, read, or workbook-state
failure.

`reason` is required on `NOT_RUN` events and forbidden on ordinary `FAILED`
events unless it documents a framework error. Singular locations require both
`row_number` and `cell_reference` or neither. Grouped locations require
non-empty, equally sized `row_numbers` and `cell_references`; singular and
grouped location fields may not be mixed.

Custom `NOT_RUN` events may include a reason, optional locations, and an
optional `error_code` when the dependency failure is machine-actionable. They
must not affect `is_valid` or `complete`, and severity is ignored for them.

The initial `NOT_RUN` reason codes are:

- `REFERENCE_WORKBOOK_REQUIRED`;
- `SHEET_NOT_RESOLVED`;
- `HEADER_NOT_RESOLVED`;
- `TABLE_NOT_RESOLVED`;
- `FORMULA_RESULTS_UNAVAILABLE`;
- `DEPENDENCY_NOT_RUN`;
- `RULE_DISABLED`.

Events for grouped findings may use a location collection:

```python
ValidationEvent(
    rule_code="duplicate_value",
    status="FAILED",
    row_numbers=[14, 22],
    cell_references=["B14", "B22"],
)
```

For ordinary cell-level findings, `row_number` and `cell_reference` are used.
Grouped events must not rely on comma-separated text to represent locations.

`ValidationResult` should contain separate ordered collections:

```python
ValidationResult(
    run_id=str | None,
    complete=bool,
    config_version=str,
    profile_name=str | None,
    compatibility_source=str | None,
    compatibility_commit=str | None,
    candidate_version=str | None,
    reference_version=str | None,
    candidate_filename=str | None,
    comparison=dict[str, object],
    diagnostics=list[DiagnosticEvent],
    errors=list[ValidationEvent],
    warnings=list[ValidationEvent],
    not_run=list[ValidationEvent],
)
```

Only `FAILED` events are included in `errors` or `warnings`; passed checks are
not emitted by default. `is_valid` is false when `errors` is non-empty and is
unaffected by `warnings` or `not_run`. A caller may choose to treat warnings or
not-run rules as blocking outside the validator.

`DiagnosticEvent` records informational outcomes that do not represent a
validation failure, such as an ignored unsupported feature. It should contain
`run_id`, `code`, optional `sheet_name`, optional A1 `cell_reference`, a
`description`, and optional `diagnostic_id`. Diagnostics do not affect
`is_valid` or `complete`. A framework-generated custom-rule exception emits a
matching diagnostic with the same opaque `diagnostic_id`; applications may use
that identifier to correlate validator logs without receiving exception text
in the result.

The serialised `DiagnosticEvent` shape is exactly `run_id`, `code`,
`diagnostic_id`, `sheet_name`, `cell_reference`, and `description`; omitted
optional values are represented as `null`.

Diagnostics are ordered deterministically by sheet order, row order, rule order,
then message order, and are retained in compatibility-test output where the
profile specifies them.

`complete` is `True` only when all enabled rules reached a terminal result. It
is `False` when validation stops because of a read failure, resource limit, or
other fatal inspection error.

Read failures during `load_workbook_pair` are raised as `WorkbookReadError`
before a `ValidationResult` exists. Read failures discovered after validation
has begun are represented by a fatal event and produce `complete=False`.

Each event should have a stable `rule_code`, optional `severity`, candidate filename,
sheet name, logical column name, original value, normalised value where
relevant, expected value, and human-readable description. Location fields are:

```python
run_id: str | None
error_code: str | None
diagnostic_id: str | None
row_number: int | None
cell_reference: str | None
row_numbers: list[int] | None
cell_references: list[str] | None
reason: str | None
```

Event ordering should be deterministic: sheet order, row order, column order,
then rule order.

`cell_reference` uses Excel A1 notation, such as `Z11`, when a cell applies.
For workbook- or sheet-level events it is `None`; `row_number` is also `None`
unless a specific Excel row is implicated.

Configuration errors should be raised before workbook validation. Workbook
inspection errors should be represented separately from failed validation rules.
`CONFIGURATION_ERROR` is the stable code carried by `ConfigurationError`; it is
not placed in a `ValidationResult` because validation has not started.

- `CONFIGURATION_ERROR` — the validation definition is invalid;
- `WORKBOOK_READ_ERROR` — the file cannot be opened or inspected;
- `RESOLUTION_ERROR` — a sheet, table, header, or column cannot be resolved;
- `WORKBOOK_LIMIT_ERROR` — a configured workbook scan limit was exceeded;
- `VALIDATION_ERROR` — resolved data violates a rule;
- `VALIDATION_WARNING` — a non-blocking issue;
- `CUSTOM_RULE_OUTPUT_INVALID` — a custom validator returned malformed output;
- `CUSTOM_RULE_EXCEPTION` — a custom validator raised an exception.

`is_valid` should be false for errors and true for warnings only. A configured
severity of `warning` places a failed event in `warnings`; `error` or omitted
severity places it in `errors`. The same precedence applies to
`rule_severity`, individual workbook checks, expected-cell checks, and column
validations. Severity is a result classification, not a validation rule.

Suggested behaviour:

- ordinary validation failures are accumulated;
- errors that prevent reliable inspection, such as an unreadable workbook,
  may stop processing;
- structural failures should not prevent independent checks on other sheets
  where those checks remain safe to perform.

If formula cached values are unavailable, formula-error validation must report
`FORMULA_RESULTS_UNAVAILABLE`; it must not silently report a clean workbook.

Formula comparison is exact text comparison, matching `dqchecks`: formulas are
not recalculated or algebraically simplified, and differences in whitespace,
case, quoting, absolute references, or equivalent syntax are differences.
Formula presence is compared as well as formula text. A formula-versus-value,
formula-versus-blank, or value-versus-formula cell is a difference even when
the cached displayed values are equal. Cells outside the configured comparison
scope are not inspected by this rule. Array-formula ranges are compared using
their formula text and covered range; a changed range boundary or formula text
is a difference, and every affected cell retains its Excel A1 location.

Formula-cache policy must be explicit. When a formula cell has no cached value,
the configured `not_run` policy reports `FORMULA_RESULTS_UNAVAILABLE`; the
configured `error` policy reports a validation error. The first implementation
uses only cached values already stored in the workbook and does not calculate
formulas or attempt to distinguish a missing cache from a legitimate blank
result when workbook metadata cannot make that distinction.

Formula evaluation is out of scope for the first implementation. A future
extension may define a `FormulaEvaluator` interface with adapters for an
available Excel installation or Java Apache POI. Evaluators must run before
validation and produce a workbook with refreshed cached values; they are not
part of the current `dataprepkit.validation` API or test requirement.

### Sheet and table resolution

Resolution behaviour must be explicit:

- an exact required-sheet selector matching zero sheets is an error;
- a regex selector matching zero sheets is an error when the table is required;
- a selector matching multiple sheets validates all matches unless
  `expected_match_count` says otherwise;
- overlapping table selectors are rejected during configuration validation;
- a missing header row, duplicate header, or ambiguous alias is a resolution
  error;
- extra-sheet handling must be explicitly configured as `ignore`, `warning`, or
  `error`.

The `dqchecks` ignored-sheet patterns (`Dict_`, `CLEAR_SHEET`, and `Changes
Log`) may be supplied by a compatibility configuration, but must not be
embedded in the validation engine.

Resolution and sheet-policy outcomes use stable rule codes:

- missing required candidate sheet: `required_sheet`;
- missing candidate sheet that exists in the reference: `missing_reference_sheet`;
- configured extra-sheet violation: `extra_sheet`;
- unresolved or ambiguous sheet selector: `sheet_resolution`;
- unresolved table boundary or required table: `table_resolution`;
- missing, duplicate, or ambiguous table header: `column_header`.
- required table with no usable rows: `non_empty_data`.

These rule codes are independent of the broad event categories
`RESOLUTION_ERROR` and `VALIDATION_ERROR`.

Each table configuration must define its data-boundary policy. It may select
the last row containing a non-empty value after normalisation, an explicit end
row, or an Excel table range, but no boundary policy may be implicit. Merged
cells, hidden rows, blank columns, and formulas must have defined behaviour and
be covered by tests.

Used-area comparison is the minimal rectangle from the first through last
non-empty cell in each selected sheet, after applying the configured blank and
null policy. Empty trailing rows and columns do not contribute to the area.
The `sheet_structure` rule compares rectangle coordinates and, when
`compare_headers=True`, the resolved header structure; it does not compare
cell values or formula results. A sheet with no non-empty cells has a defined
empty area and can be compared without an implicit one-cell default.

For used-area and table-boundary resolution, a formula cell counts as occupied
even when its cached value is blank; a merged range contributes only its
top-left stored value; hidden rows and columns remain part of the range; and
blank columns inside the range are retained. These rules apply consistently to
candidate and reference workbooks.

`data_presence="require_one_usable_row"` requires at least one row within the
resolved data boundary that is not blank under the configured `blank_policy`.
Rows excluded by an `EmptyRowRule` are not usable rows. A table with no usable
rows emits one `non_empty_data` failure; `allow_empty` emits no event.
If an optional table (`required=False`) is absent, table resolution and data
presence checks are both `NOT_RUN` with reason `TABLE_NOT_RESOLVED`; the
absence is not a `non_empty_data` failure.

### Expected-cell fallback behaviour

The configuration must specify how fallback locations are evaluated, such as
ordered-first, exactly-one, or all-must-agree. If no location exists, a
resolution error is emitted. Conflicting values produce a validation error
rather than an arbitrary result.

### Extensible rules

Built-in rules should use stable rule codes. Custom rules should be registered
by name so configurations remain serialisable and auditable:

```python
register_rule("valid_boncode", validate_boncode)

ColumnValidation(
    column="reference",
    check="valid_boncode",
)
```

Custom rules receive resolved, normalised rows plus candidate-cell metadata and
return `ValidationEvent` objects. `ColumnValidation.check_options` is passed to
the registered rule and must be validated by that rule's typed option model.
Callables may be supported for Python-only configuration, but named rules are
the supported configuration-file API.

## Testing approach

Tests must exercise the public API as the primary contract. Tests should use
only exported APIs, especially `load_workbook_pair`, the public configuration
models, `validate_workbook`, `ValidationResult`, `ValidationEvent`, and
`DiagnosticEvent`. Tests must not import or call private helpers, internal rule
functions, or implementation modules to establish correctness.

Use small generated workbooks and invoke validation through the complete public
workflow:

```python
with load_workbook_pair(
    candidate_path=candidate_path,
    reference_path=reference_path,
) as workbooks:
    result = validate_workbook(workbooks=workbooks, config=config)
```

Standalone tests must use the same public workflow with no reference path.
Configuration-only tests may construct public Pydantic models directly, but
must assert public validation errors and messages.

Initial test cases should cover:

- standalone validation with no reference workbook;
- one-call `validate_excel` standalone and reference-backed workflows;
- `validate_excel` cleanup on success, configuration failure, read failure, and
  validation failure;
- config-path, mapping, and named-profile resolution through `validate_excel`;
- configuration-only validation without opening workbooks;
- JSON Schema validation and version rejection;
- current and unsupported `get_config_schema` versions;
- deterministic profile listing and profile descriptions;
- `ValidationResult` convenience helpers and `ValidationFailedError`;
- actionable configuration-error field paths;
- reference-dependent rules reported as `NOT_RUN` in standalone mode;
- matching and missing sheets;
- extra candidate sheets;
- row and column shape differences;
- formula changes and formula-versus-value changes;
- formula presence, empty-area, merged-cell, hidden-row, and trailing-boundary
  semantics;
- array-formula text and covered-range differences;
- cached formula errors using precomputed fixture workbooks only;
- expected cell matches and mismatches;
- fallback cell locations;
- missing and misordered headers;
- empty-row violations;
- required tables with no usable rows;
- required values;
- duplicate values;
- allowed-values violations;
- forbidden-values violations;
- `allowed_values` and `forbidden_values` configured together;
- multiple errors returned from one workbook;
- invalid configuration rejected before validation.
- table-level rule ownership and rejection of duplicate `WorkbookCheck`
  declarations;
- `required=True` and `unique=True` result rule-code mappings;
- ignored-sheet precedence over table selectors;
- valid and invalid in-memory `WorkbookPair` construction;

Tests should use temporary `.xlsx` files generated with `openpyxl`, keeping
them independent of external workbooks or production data.

Additional comparison-policy tests should cover:

- values differing only by case;
- values with leading, trailing, or repeated whitespace;
- empty strings versus nulls;
- duplicate keys after case and whitespace normalisation;
- allowed values after normalisation;
- accent-sensitive versus accent-insensitive configuration;
- deterministic ordering of multiple events;
- missing formula cached values;
- ambiguous sheet selectors and column aliases;
- malformed configuration and unsupported rule names.
- null-policy behaviour for uniqueness and value-list checks;
- typed expected-cell comparison modes;
- A1 reference validation and canonicalisation;
- custom-rule option validation;
- required/ignored selector overlap;
- partial results and rule stopping after `WORKBOOK_LIMIT_ERROR`.
- `ValidationResult.complete=False` after fatal inspection failures;
- each supported workbook feature configured as `ignore`, `warning`, and
  `error`.
- ignored or unavailable features producing deterministic diagnostic events;
- unavailable feature detection producing `FEATURE_DETECTION_UNAVAILABLE`.
- invalid selector regular expressions;
- selector full-match and case-policy behaviour;
- invalid `WorkbookPair` reference-view combinations;
- each defined `NOT_RUN` reason code.
- compatibility-profile merging where caller values override profile values
  and unspecified profile values are retained.
- workbook-pair context-manager cleanup after successful validation;
- workbook-pair cleanup when loading or validation raises an error;
- runtime cell-scan limits and `WORKBOOK_LIMIT_ERROR`.

`dqchecks` compatibility tests should also cover:

- ignored sheet selectors supplied through configuration;
- sheet-name comparison ignoring tab order;
- array-formula comparison;
- formula-versus-value differences;
- used-area detection based on actual non-empty cells;
- row 1 and row 3 empty-row behaviour;
- configured header-row behaviour;
- regex-selected table sheets and configurable header spacing;
- preservation of Excel row numbers and A1 cell references such as `Z11`.

At least one end-to-end compatibility test must run the configured profile
against a representative workbook and verify the rule codes, affected sheets,
row numbers, A1 cell references, and error descriptions expected from the
selected `dqchecks` source version.

Every public rule capability must have at least one public-API test covering a
passing case and a failing case. The public-API suite must also verify:

- standalone and reference-backed invocation;
- multiple errors, warnings, diagnostics, and `NOT_RUN` results;
- deterministic result ordering;
- context-manager cleanup on success and failure;
- invalid public configuration rejection;
- JSON/YAML configuration loading and validated round-tripping;
- public custom-rule registration, option validation, and duplicate-name
  rejection;
- custom-rule events using the registered rule name;
- custom column- and workbook-scoped validator contracts;
- custom event metadata enforcement and grouped-event handling;
- custom-rule registration, listing, and teardown isolation;
- custom attachment versus execution-scope validation;
- complete built-in option models and scope-dependent option validation;
- mode-specific `DataBoundary` validation;
- Excel-table boundary resolution and optional-table data-presence behavior;
- compatibility-profile provenance in validation results;
- options-model protocol validation and serialisation;
- built-in versus registered custom-rule resolution;
- custom workbook-rule option validation;
- `enabled_rules` behavior for workbook, expected-cell, table, and column
  rules, including `RULE_DISABLED`;
- explicitly disabled checks returning `NOT_RUN/RULE_DISABLED`;
- public exception inheritance, stable codes, and pre-validation raising;
- unsupported serialization formats and malformed JSON/YAML;
- malformed custom events and `CUSTOM_RULE_OUTPUT_INVALID` handling;
- custom `NOT_RUN` output, malformed-event continuation, and `complete` status;
- custom exceptions and `CUSTOM_RULE_EXCEPTION` handling;
- event reason and singular/grouped-location invariants;
- serialized `RuleContext` shape and read-only behavior;
- JSON-safe serialization of date, datetime, decimal, and unsupported values;
- serialized validation results and deterministic event ordering;
- validation-result deserialization without re-running rules;
- result metadata and complete round-tripping of all event collections;
- rejection of inconsistent serialized `is_valid` values;
- context-array length validation;
- safe diagnostic identifiers for custom exceptions;
- custom `NOT_RUN` metadata and non-blocking semantics;
- compatibility-profile loading and merge behaviour;
- compatibility fixture manifest existence, coverage, and expected-outcome
  validation;
- scalar, explicit-null, mapping, and list replacement semantics during
  profile merging;
- serialisation and reconstruction of supported configuration models.

Feature-policy tests must use representative fixtures containing, where
supported, merged cells, macros, charts, external links, named ranges, and
pivot tables. Each fixture must be tested with `ignore`, `warning`, and `error`
actions.

### Configuration and operational concerns

The configuration schema should have an explicit version so reference-specific
configuration can evolve without silently changing validation semantics.

The compatibility source must also be pinned. The implementation plan should
record the `dqchecks` repository URL, commit SHA, and the date on which the
compatibility profile was defined. Future upstream changes require an explicit
compatibility review rather than silently changing behaviour.

The compatibility profile is incomplete until `commit_sha` and a fixture
identifier are populated. Profile validation must fail if either is missing;
the plan must not use a moving branch such as `main` as the source version.

The initial source pin for this plan is:

```yaml
name: dqchecks-direct-excel-v1
repository_url: https://github.com/Ofwat/dqchecks
commit_sha: 23113afcd61862760feadc6b3d8abd2822752ddb
defined_on: 2026-08-18
fixture_identifier: dqchecks-direct-excel-fixtures-v1
```

The compatibility fixture identified above must be version-controlled at
`tests/fixtures/dqchecks-direct-excel-fixtures-v1/` and contain a
`manifest.yaml`. The manifest must map each fixture to its candidate and
optional reference workbook, configured profile, expected rule codes, expected
statuses, and expected locations. It must include passing and failing cases
for every direct Excel check in scope, including standalone and
reference-backed cases.

The manifest schema is:

```yaml
manifest_version: "1"
profile: dqchecks-direct-excel-v1
fixtures:
  - id: formula-error-failing
    candidate: formula-error-failing.xlsx
    reference: null
    mode: standalone
    expected:
      outcome: invalid
      complete: true
      event_statuses: [FAILED]
      rule_codes: [formula_error]
      locations:
        - sheet_name: F_Outputs
          cell_reference: Z11
```

`manifest_version`, `profile`, and non-empty `fixtures` are required. Each
fixture requires a unique `id`, paths relative to the manifest directory,
`mode` (`standalone` or `reference_backed`), and an `expected` block.
`expected.outcome` is `valid` or `invalid`, and `expected.complete` is a
required boolean; `is_valid` is derived from `outcome`. `event_statuses` contains only
`FAILED` or `NOT_RUN` because passed checks are not emitted as events. Expected
rule codes and locations use the public result schema. A reference-backed
fixture must provide `reference`; a standalone fixture must not. Passing
fixtures use `outcome: valid` and an empty failure list, while failing fixtures
use `outcome: invalid` and list the expected failed rule codes and locations.

Profile validation must verify that the fixture directory and manifest exist,
that every manifest workbook exists, and that every configured rule has at
least one passing and one failing fixture. Updating the source SHA, fixture
identifier, fixture contents, or manifest expectations requires a new profile
name or explicit compatibility-version review.

The package should include maintained starter profiles as version-controlled
YAML examples, including a standalone workbook-integrity profile and a
reference-backed flat-table profile. Starter profiles are explicit templates,
not hidden defaults: users copy or select one and can inspect every configured
sheet selector, location, boundary, comparison policy, and rule.

The compatibility profile should be a version-controlled YAML or JSON file
containing the complete `WorkbookValidationConfig`. It must include the pinned
source repository, commit SHA, profile name, and all ignored selectors, rule
options, comparison policies, and expected locations needed to reproduce the
selected behaviour.

The profile must also include `config_version`, runtime policy, comparison
policy, rule enablement and severity settings, and the identifier or path of
the compatibility test fixture used to verify it.

Compatibility profiles are loaded first. An explicit caller configuration may
override profile values, but the merge must be deep, validated, and
deterministic. Caller values always win; unspecified values remain from the
profile. A profile and caller configuration may not silently merge two
conflicting rule definitions.

Configuration loading must distinguish an omitted field from an explicit null.
An omitted field inherits the profile value; an explicit null clears the profile
value where the field is nullable. This distinction must survive JSON/YAML
loading and profile merging.

`openpyxl` is an optional dependency provided by the `dataprepkit[excel]`
extra. The loader supports `.xlsx` and `.xlsm`, rejects `.xls`, and defines
explicit behaviour for read-only mode, invalid files, and resource cleanup.

Unsupported workbook features must have explicit policy settings. The initial
policy should cover external links, charts, pivot tables, named ranges, merged
cells, and macros, with each feature configured as `ignore`, `warning`, or
`error` where applicable.

Feature detection capability must be explicit. If the selected openpyxl-based
loader cannot determine whether a configured feature is present, it emits
`FEATURE_DETECTION_UNAVAILABLE` and applies that feature's configured action;
it must not silently treat the feature as absent. Fixtures must identify which
features are detectable in the supported file formats.

For `ignore`, the unavailable feature is recorded in diagnostics and does not
affect `is_valid`. For `warning`, it produces a warning event. For `error`, it
produces an error event and makes `is_valid` false. Feature unavailability does
not make the result incomplete unless inspection itself stops.

Performance should be measured on representative large workbooks. Rules should
avoid repeatedly scanning the same worksheet and should share resolved table
data and used-area information.

The implementation should define a configurable safety limit for maximum unique
worksheet cells inspected per workbook. Shared workbook-resolution caches count
each cell once. Exceeding the limit produces `WORKBOOK_LIMIT_ERROR`
rather than an unbounded scan. Performance tests should cover formula scans,
used-area detection, and multiple table rules over the same sheet.

`WorkbookPair` should accept optional `candidate_version` and
`reference_version` metadata. The validation result should include the
configuration version, those workbook versions when supplied, target
collation/comparison policy, and candidate filename so that a validation run
can be audited and reproduced.

## Implementation status

Status is tracked against the public specification and updated as each
public-API test and implementation slice is completed.

Current status as of 2026-08-19:

| Area | Status | Evidence or next action |
| --- | --- | --- |
| Direct `dqchecks` behavior documented | Complete | `EXCEL_WORKBOOK_CHECKS.md` |
| Generic validation specification | Complete | This document |
| Public API and result contract | Complete | Public exports and result model sections |
| Compatibility profile definition | Complete | Pinned source and manifest schema |
| Compatibility fixture files and manifest | Pending | Create the version-controlled fixture directory |
| Public configuration models | In progress | First public model-validation slice is green |
| Workbook loading and cleanup | In progress | First `validate_excel` cleanup slice is green |
| `validate_config` and config serialization | Pending | Write failing YAML/schema tests |
| `validate_excel` facade | In progress | Standalone required-sheet slice is green |
| First validation rules | In progress | Required sheets implemented; formula errors next |
| Remaining validation rules | Pending | Implement one public rule slice at a time |
| Compatibility verification | Pending | Run the pinned profile against the fixture manifest |

Implementation must not be marked complete from documentation alone. A row
becomes `Complete` only when its public behavior has passing tests and the
corresponding implementation is committed. `Pending` means no implementation
or public contract test has been completed; `In progress` means a failing test
or implementation work is currently underway.

### TDD execution loop

For every status row:

1. Translate the relevant specification into a failing public-API test.
2. Implement the smallest behavior that satisfies the test.
3. Run the focused test and the relevant full test suite.
4. Refactor without changing the public contract.
5. Update this status table only after the tests pass.

Tests must use exported APIs and real generated `.xlsx` fixtures where workbook
behavior is involved. Private helpers may be tested indirectly but must not be
the contract that drives implementation.

## Implementation phases

### Phase 1: Models and configuration

- Add the `dataprepkit.validation` package.
- Define typed configuration models.
- Define `ValidationEvent` and `ValidationResult`.
- Validate configuration values and produce clear configuration errors.
- Define public exception types, result metadata, and serialization contracts.
- Implement JSON/YAML configuration loading, dumping, and profile merging.
- Publish the versioned configuration JSON Schema.
- Implement configuration-only validation and profile discovery/description.

### Phase 2: Workbook loading and resolution

- Add workbook-pair loading support for formula and evaluated-value views.
- Add the `validate_excel` one-call file-based facade.
- Implement sheet selection and used-area resolution.
- Implement table header and row-to-cell mapping.

### Phase 3: Core workbook rules

- Implement required-sheet checks.
- Implement structure checks.
- Implement formula comparison.
- Implement formula-error detection.

### Phase 4: Configurable content rules

- Implement expected-cell checks.
- Implement header and empty-row checks.
- Implement required, unique, allowed-values, and forbidden-values column
  rules.

### Phase 5: Extensibility and result handling

- Implement built-in versus registered custom-rule resolution.
- Implement custom-rule registration, option validation, lifecycle, and
  safe event adaptation.
- Implement result dumping and validated result reconstruction.
- Implement public registry and result-serialization tests.

### Phase 6: Integration and documentation

- Export the public API from `dataprepkit.validation`.
- Add examples for a reference-specific configuration.
- Add the pinned `dqchecks` compatibility profile.
- Add maintained standalone and reference-backed starter profiles.
- Link the new documentation from `README.md`.
- Run the full test suite and relevant validation tests.
- Confirm the configured comparison policy matches the target SQL Server
  collation.

## Completion criteria

The work is complete when:

- a consumer can configure all first-version checks without editing library
  source code;
- one validation call returns all safely detectable errors;
- every error includes enough sheet, row, and cell context to fix the workbook;
- the core rules have public-behaviour tests;
- every public validation rule has passing and failing public-API coverage;
- tests do not depend on private implementation helpers;
- every documented public export imports successfully from
  `dataprepkit.validation`;
- the API and configuration are documented with a runnable example;
- case-insensitive matching, null handling, and key uniqueness match the target
  SQL Server collation policy;
- validation output is deterministic and contains enough provenance for audit;
- workbook resources are released correctly on success and failure.
