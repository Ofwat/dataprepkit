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

The checks are grouped by what they inspect. In normal use, configure the
workbook checks first. Configure a table when you need headers, boundaries,
or checks on values within a tab.

#### Workbook and sheet checks

These checks inspect workbook structure and individual cells without loading a
table into pandas.

| Check | Configure it with | What it reports |
| --- | --- | --- |
| `required_sheet` | `sheet_policy.required_selectors` | A required sheet selector matched no sheet. |
| `extra_sheet` | `sheet_policy.extra_sheet_action` | A candidate sheet was not selected or ignored. |
| `expected_cell` | `expected_cells` | A resolved cell differs from its expected value. |
| `required_filled_cells` | `workbook_checks` | A cell with a configured fill colour is blank. |
| `unexpected_formula` | `workbook_checks` | A formula appears in a filled input cell. |
| `formula_error` | `workbook_checks` | A cached cell contains a configured Excel error token. |

Example: require a sheet and reject Excel errors:

```yaml
sheet_policy:
  required_selectors:
    - mode: exact
      value: Data

workbook_checks:
  - rule_code: formula_error
    enabled: true
    scope: all_sheets
    options:
      error_tokens: ["#REF!", "#VALUE!"]
```

#### Reference comparison checks

These checks require `reference_path` when calling `validate_excel`.

| Check | Configure it with | What it reports |
| --- | --- | --- |
| `missing_reference_sheet` | `workbook_checks` | A reference sheet is absent from the candidate. |
| `sheet_structure` | `workbook_checks` | Candidate and reference content-based used areas differ. |
| `formula_difference` | `workbook_checks` | Formula state or formula text differs at a corresponding cell. |

Example:

```yaml
workbook_checks:
  - rule_code: sheet_structure
    enabled: true
    scope: overlapping_sheets
  - rule_code: formula_difference
    enabled: true
    scope: overlapping_sheets
    options:
      whitespace_policy: normalised
```

#### Table resolution and structure checks

These checks establish whether a sheet can be treated as a table. They run
before value checks and produce `NOT_RUN` for dependent checks when the table
cannot be resolved.

| Check | Configure it with | What it reports |
| --- | --- | --- |
| `table_resolution` | `tables` and `data_boundary` | A required sheet, table, or boundary cannot be resolved. |
| `column_header` | `tables.header_policy` | Required, ordered, blank, duplicate, or extra headers are invalid. |
| `data_boundary` | `tables.data_boundary` | The configured data boundary cannot be resolved. |
| `pandas_load` | `tables` | A resolved table cannot be loaded into pandas. |
| `missing_column` | `tables.column_validations` | A configured column is absent from the loaded table. |
| `empty_table` | `tables` | A resolved table has no data rows. |
| `non_empty_data` | `tables.data_presence` | A required table has no usable data row. |
| `empty_row_pattern` | `tables.empty_row_rules` | A row configured as blank contains a value. |

Example:

```yaml
tables:
  - name: outputs
    required: true
    sheet_selector: {mode: exact, value: Data}
    header_row: 1
    header_policy:
      required_columns: [Measure_Cd, Measure_Value]
    data_boundary:
      mode: last_non_empty_row
      infer_columns: true
    data_presence: require_one_usable_row
```

#### Column value checks

These checks inspect values in resolved table columns. They do not define the
table; missing columns are reported by the structural checks above.

| Check | Configure it with | What it reports |
| --- | --- | --- |
| `missing_value` | `column_validations[].null_policy: error` | A configured value is null or blank. |
| `duplicate_value` | `column_validations[].unique` | A normalized column value occurs more than once. |
| `allowed_values` | `column_validations[].allowed_values` | A value is not in the allow-list. |
| `forbidden_values` | `column_validations[].forbidden_values` or `workbook_checks` | A value is denied or matches a forbidden pattern. |
| `max_length` | `column_validations[].max_length` | Text exceeds the configured length. |
| `value_type` | `column_validations[].value_type` | A value is not text or numeric as configured. |

Example:

```yaml
column_validations:
  - column: Measure_Cd
    unique: true
  - column: Measure_Value
    max_length: 4000
    forbidden_patterns: ["¬¬"]
    value_type: numeric
```

#### Table and cross-table checks

| Check | Configure it with | What it reports |
| --- | --- | --- |
| `conflicting_duplicate` | `tables[].table_validations` or `database_checks` | The same identity combination has conflicting values, optionally after database dimension resolution. |
| `values_in_reference` | `cross_table_checks` | A source value is absent from another loaded table. |

Example:

```yaml
table_validations:
  - rule_code: conflicting_duplicate
    key_columns: {mode: pattern, pattern: ".*_Cd$"}
    value_columns: [Measure_Value]
```

#### Database-backed checks

Database-backed checks are an optional second-stage validation. They use the
tables already resolved and loaded from Excel, then enrich or validate those
values against a relational table.

The SQLAlchemy engine is runtime input, not configuration data:

```python
result = validate_excel(
    candidate_path=candidate_path,
    reference_path=None,
    config=config,
    engine=engine,  # optional
)
```

The intended execution order is:

```text
Excel structural checks
→ load resolved Excel tables
→ run Excel/table value checks
→ load configured lookup rows through engine
→ join lookup data in memory
→ run database-backed checks
```

The proposed configuration keeps database checks separate from ordinary
column checks, while using the same naming conventions as the rest of the
API: named objects, `rule_code`, `enabled`, `severity`, and `depends_on`.
Lookup definitions are reusable so several checks share one query and cache:

```yaml
database_lookups:
  - name: measure_dimension
    schema: Dimensions
    table: dim_measure
    key_columns:
      Measure_Cd: Measure_Cd
    value_columns:
      - Expected_Value_Type
    batch_size: 500

database_checks:
  - rule_code: measure_value_type
    enabled: true
    severity: error
    source_table: process_data
    lookup: measure_dimension
    column_validations:
      - column: Measure_Value
        value_type_from: Expected_Value_Type
```

For duplicate detection where Excel column names differ from database
dimension keys, configure one lookup per dimension and compare the canonical
dimension values:

```yaml
database_lookups:
  - name: measure_dimension
    schema: Dimensions
    table: dim_measure
    key_columns: {Measure_Cd: measure_code}
    value_columns: [measure_id]
  - name: organisation_dimension
    schema: Dimensions
    table: dim_organisation
    key_columns: {Organisation_Cd: organisation_code}
    value_columns: [organisation_id]

database_checks:
  - name: conflicting_measure_records
    rule_code: conflicting_duplicate
    source_table: process_data
    dimensions:
      - source_column: Measure_Cd
        lookup: measure_dimension
        canonical_column: measure_id
      - source_column: Organisation_Cd
        lookup: organisation_dimension
        canonical_column: organisation_id
    value_columns: [Measure_Value]
```

`source_table` refers to the configured `tables[].name`; `lookup` refers to a
named `database_lookups[].name`. A table selected from multiple sheets is
validated independently per sheet, preserving normal table provenance. There
is no implicit combination of sheets.

For example, a lookup row could declare `Expected_Value_Type: text` for
`Measure_Cd = INN001`. A numeric value in `Measure_Value` would then fail;
when the lookup says `numeric`, a value such as `TBC` would fail.

The database-check contract is:

| Situation | Result |
| --- | --- |
| No engine is supplied | The database check is `NOT_RUN` with reason `DATABASE_ENGINE_REQUIRED`. |
| Excel table did not resolve or load | The database check is `NOT_RUN` with reason `PANDAS_LOAD_FAILED`. |
| Lookup table or schema is absent | The database check is `NOT_RUN` with reason `LOOKUP_TABLE_MISSING`; `complete=False`. |
| Database user lacks `SELECT` permission | The database check is `NOT_RUN` with reason `DATABASE_PERMISSION_DENIED`; `complete=False`. |
| Lookup query times out | The database check is `NOT_RUN` with reason `DATABASE_TIMEOUT`; `complete=False`. |
| Lookup query fails for another reason | The database check is `NOT_RUN` with reason `DATABASE_LOOKUP_FAILED`; `complete=False`. |
| A configured lookup column is absent from the database table | The database check is `NOT_RUN` with reason `LOOKUP_COLUMN_MISSING`; `complete=False`. |
| A source key has no lookup row | A validation error identifies the Excel row and key. |
| Lookup keys are duplicated | A validation error reports the ambiguous lookup; no row is selected. |
| Source key is null-like | The database check skips that row; Excel null checks remain responsible for reporting it. |
| Lookup returns a null canonical or expected value | A validation error reports the incomplete lookup data. |
| Lookup values cannot be converted to the configured type | The database check is `NOT_RUN` with reason `LOOKUP_TYPE_CONVERSION_FAILED`; `complete=False`. |
| Lookup succeeds but returns no rows | Source rows requiring a lookup produce `database_missing_lookup`; an empty source table remains valid for this check. |

Additional rules keep the boundary predictable:

- Database checks run only after Excel structural checks and pandas loading
  succeed. They do not replace header, boundary, or table checks.
- They operate on the resolved pandas table, not by independently rereading
  worksheet cells.
- They use the same `enabled`, `severity`, and `depends_on` concepts as other
  configured checks. A disabled or blocked prerequisite produces `NOT_RUN`.
- Distinct non-null source keys are queried in parameterized batches; the
  lookup `batch_size` is configurable with a safe default and a validated
  upper bound.
- Key matching uses the configured comparison policy for case, accents,
  whitespace, and null tokens. Database collation is not assumed to match
  automatically.
- Table and column identifiers are validated and safely quoted for the target
  SQL dialect. Values are always bound parameters.
- The supplied engine is used for reads only. Validation generates and
  executes `SELECT` statements; it never issues `INSERT`, `UPDATE`,
  `DELETE`, DDL, or stored-procedure calls, and never commits database
  changes. It does not manage the caller's engine lifecycle.
- Findings retain the Excel table name, sheet name, row number, cell reference,
  source key, lookup table, and lookup key where applicable.
- Lookup results are cached for the duration of one validation run so multiple
  checks do not repeat the same query.
- Lookup definitions are read-only and declarative: they contain schema,
  table, key mapping, selected value columns, and batching only. They cannot
  contain arbitrary SQL.

The implementation contract is deliberately narrow:

```text
DatabaseLookup
  name: str
  schema: str | null
  table: str
  key_columns: mapping[str, str]
  value_columns: list[str]
  batch_size: int = 500
  max_distinct_keys: int = 100000
  timeout_seconds: int = 30
  collation_name: str | null
  isolation_level: snapshot | read_committed = snapshot
  retry_count: int = 0
  persist_lookup_keys: bool = true

DatabaseCheck
  rule_code: str
  enabled: bool = true
  severity: error | warning | ignore
  source_table: str
  lookup: str | null
  column_validations: list[LookupColumnValidation]
  dimensions: list[DatabaseDimension]
  value_columns: list[str]
  depends_on: list[str] = []

LookupColumnValidation
  column: str
  value_type_from: str

DatabaseDimension
  source_column: str
  lookup: str
  canonical_column: str
```

`value_type_from` currently accepts only `text` and `numeric` values from the
named lookup column. Additional lookup-backed validations will add explicit
typed fields rather than accepting arbitrary rule dictionaries.

The following decisions apply:

- The first public API accepts a synchronous SQLAlchemy `Engine` through the
  keyword-only `engine` argument. The caller owns its lifecycle; DataPrepKit
  does not close or dispose it.
- A database check is automatically gated on successful resolution and pandas
  loading of its `source_table`. Its explicit `depends_on` entries may name
  other database checks; workbook/table loading is an implicit prerequisite.
- A missing source column in the Excel table is handled by the existing table
  and pandas structural checks. It prevents the dependent database check from
  running; it is not treated as a missing database lookup row.
- A missing engine or database failure produces `complete=False` and a
  `NOT_RUN` event. It does not silently produce a valid result.
- `batch_size` defaults to `500` and may not exceed `5000`. More than
  `100000` distinct source keys produces a configuration/runtime finding
  rather than an unbounded query.
- The lookup cache is scoped to one validation run. Its key includes the
  engine database identity, schema, table, key mapping, selected value
  columns, comparison policy, and batch settings.
- Source keys are deduplicated after applying the configured null and text
  normalization policy. Null-like keys are excluded from lookup queries.
  Returned keys and source keys are compared again using that same policy.
- Composite keys use the declared `key_columns` mapping as one tuple. A
  finding reports every source and lookup key column/value, preserving the
  declared mapping order.
- Lookup value columns must not collide with source column names after the
  configured comparison normalization. Such a collision is a configuration
  error; implicit overwriting or suffix generation is not allowed.
- Duplicate lookup rows with identical selected values are collapsed. Rows
  with conflicting selected values produce an error and are not used.
- Missing lookup key or value columns are schema/configuration failures. They
  produce `database_lookup` with reason `LOOKUP_COLUMN_MISSING`; they do not
  produce misleading row-level duplicate or missing-key findings.
- A missing lookup table or schema produces `database_lookup` with reason
  `LOOKUP_TABLE_MISSING`. Permission failures use
  `DATABASE_PERMISSION_DENIED`; timeouts use `DATABASE_TIMEOUT`.
- A lookup row with a null canonical key or required expected value produces a
  row-level `database_lookup` finding with reason `LOOKUP_VALUE_NULL` and is
  excluded from dependent checks.
- Lookup type-conversion failures use
  `LOOKUP_TYPE_CONVERSION_FAILED`; the validator never silently coerces an
  invalid database value.
- Text-key lookups use the configured SQL Server collation when one is
  supplied. If the database collation cannot provide the requested case or
  accent behavior, the lookup is `NOT_RUN` with reason
  `DATABASE_COLLATION_INCOMPATIBLE`; it is never silently treated as an exact
  comparison.
- A single read-only transaction is used for all batches of one lookup when
  the SQLAlchemy dialect supports it. DataPrepKit never commits or rolls back
  a transaction it did not create.
- Each lookup has a default `timeout_seconds: 30`; timeout and cancellation
  are reported as `DATABASE_LOOKUP_FAILED` and do not leave a background query
  running.
- Caches are local to one `validate_excel` call and are not shared between
  threads or processes. Concurrent validations therefore cannot observe or
  mutate each other's lookup state.
- The initial implementation supports SQLAlchemy `Engine`, SQL Server, and
  SQLite test coverage. Other dialects require verified identifier quoting,
  parameter limits, and type behavior before being supported.
- Database findings include `source_table`, lookup name, lookup key, Excel
  table name, sheet name, row number, and cell reference where available.

Stable database event codes are:

| Code | Meaning |
| --- | --- |
| `database_lookup` | The lookup could not be loaded or used. |
| `database_missing_lookup` | A source key has no matching lookup row. |
| `database_duplicate_lookup` | Lookup rows are ambiguous or conflicting. |
| The configured `rule_code` | A loaded lookup value failed the requested validation. |

Database `NOT_RUN` events are included in the normal `not_run` result
collection and summary counts. They do not disappear when the database phase
is unavailable or incomplete.

The public test contract must cover missing engines, lookup failures and
timeouts, single and composite keys, missing and duplicate lookups, null keys,
batch limits, cache reuse, column collisions, collation incompatibility, and
finding provenance.

SQL Server integration tests must additionally verify snapshot-isolation
behavior, permission mapping, timeout mapping, rollback, SELECT-only
execution, cache limits, retry count zero, and lookup-key redaction.

##### Final implementation decisions

The first implementation uses these decisions to keep the feature predictable:

- Accept a SQLAlchemy synchronous `Engine` only. Existing `Connection` objects,
  async engines, and connection factories are out of scope for version one.
- During the database phase, DataPrepKit obtains and owns one connection per
  validation run. It may use a transaction for a consistent read and rolls
  back that transaction before releasing the connection; it never disposes
  the caller's engine. The no-write guarantee comes from the generated
  `SELECT`-only SQL, not from a database-specific read-only transaction flag.
- A driver timeout of 30 seconds applies to each lookup batch. SQL Server
  `pyodbc` is the supported production path; unsupported drivers produce
  `DATABASE_LOOKUP_FAILED` rather than pretending the timeout was enforced.
- Text key lookups use the lookup's optional `collation_name`, which is the
  actual SQL Server collation name. The identifier is validated against a safe
  identifier pattern and applied to both sides of the key comparison. The
  general `comparison.collation_name` remains a policy label. A missing or
  incompatible database collation for a text key produces
  `DATABASE_COLLATION_INCOMPATIBLE`.
- Connection and permission errors use `database_lookup` with reason
  `DATABASE_LOOKUP_FAILED`. They set `complete=False` and do not expose raw
  credentials or provider-specific connection details in the result.
- Numeric validation accepts integers, finite decimals, finite floats, and
  trimmed numeric strings. Booleans, empty strings, dates, `NaN`, and infinity
  are not numeric. Text validation accepts strings only; numbers are not
  coerced to text.
- Each database check has a required unique `name` and a stable `rule_code`.
  Dependencies target check names, while result events use `rule_code`.
  Dependency cycles and unknown names are configuration errors.
- Database findings carry structured `metadata` containing lookup name,
  source table, lookup table, source key, lookup key, and Excel provenance.
  The metadata is included in result serialization and the validation result
  DataFrame as a JSON string suitable for warehouse staging.
- `persist_lookup_keys` defaults to `true` for useful diagnostics. When false,
  persisted result metadata redacts source and lookup key values while keeping
  column names, sheet, row, and cell reference.
- `retry_count` defaults to zero. Validation does not retry database reads
  automatically; callers can rerun the complete validation explicitly.
- SQL Server uses `SNAPSHOT` isolation by default so all lookup batches see one
  consistent dimension view. If snapshot isolation is unavailable, the check
  is `NOT_RUN` with reason `DATABASE_ISOLATION_UNAVAILABLE`.
  `read_committed` is an explicit opt-in when snapshot isolation cannot be
  enabled.
- Driver exceptions are mapped to stable reasons without persisting provider
  messages or credentials: missing objects, permission denied, timeout,
  conversion failure, isolation unavailable, and generic lookup failure.
- Lookup results are bounded by both `max_distinct_keys` and a cache-row limit
  of `250000`. Exceeding either limit produces `DATABASE_LOOKUP_LIMIT` and
  `complete=False`.
- Identifier names are limited to configured schema/table/column identifiers
  and validated before SQL generation. Lookup values are always bound
  parameters. Arbitrary SQL is not supported.
- Schema, table, and column identifiers must match
  `[A-Za-z_][A-Za-z0-9_]*`. Names containing spaces, dots, brackets, quotes,
  SQL expressions, or other punctuation are rejected during configuration
  loading rather than dynamically interpreted.
- Production deployments should use a database principal with `SELECT`-only
  permissions. This is defence in depth; validation must remain non-writing
  even when given a more privileged engine.
- `database_lookups`, `database_checks`, and their options must be included in
  the versioned JSON Schema and at least one starter profile before release.

Non-write enforcement is part of the implementation acceptance criteria:

- Database configuration has no raw SQL, SQL fragments, expressions, or
  stored-procedure fields.
- Schema, table, and column names are validated before being passed to the
  dialect-aware SQL builder.
- Lookup values are supplied only as bound parameters; they are never joined
  into SQL text.
- The database executor exposes only a read operation to database checks.
  There is no validation code path for DML, DDL, or procedure invocation.
- Lookup failure has no write fallback and is reported as `NOT_RUN`.
- Tests use a recording SQLAlchemy engine to assert that every generated
  statement is a `SELECT`, no commit is issued, and no DML/DDL text is sent.
- Tests also run against a `SELECT`-only database principal where available.

These guarantees apply to DataPrepKit's generated database operations. A
caller-controlled SQLAlchemy event hook or intentionally malicious custom
engine is outside the validator's control and should not be supplied.

Database-backed checks are part of the versioned configuration contract. The
SQLAlchemy engine is optional; configured database checks become `NOT_RUN` and
make the result incomplete when no engine is supplied.

#### Workbook feature policies

Feature policies report the concrete feature name when a workbook contains it.
Supported features are `macros`, `external_links`, `charts`, `pivot_tables`,
`named_ranges`, and `merged_cells`. The corresponding
`<feature>_detection_unavailable` event is reported when inspection is not
possible.

| Event | Configure it with | What it reports |
| --- | --- | --- |
| `<feature_name>` | `runtime.feature_policy.<feature_name>` | The named workbook feature was detected. |
| `<feature_name>_detection_unavailable` | `runtime.feature_policy.unavailable_action` | The named feature could not be inspected. |

Example:

```yaml
runtime:
  feature_policy:
    merged_cells: {action: warning, scope: {type: all_sheets}}
    external_links: warning
    macros: error

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
      - column: Measure_Value
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

### Formula checks

Formula checks inspect the workbook directly. They do not calculate formulas.

| Check | Requires reference workbook | Behaviour |
| --- | --- | --- |
| `formula_difference` | Yes | Compares formula text at corresponding cells. |
| `unexpected_formula` | No | Finds formulas in cells whose fill colour identifies user input. |
| `formula_error` | No | Finds cached Excel error values matching `error_tokens`. |

`formula_difference` reports a finding whenever the formula state or formula
text differs between candidate and reference. The `reason` identifies the
transition:

- `FORMULA_ADDED`: the reference contains a value and the candidate contains a formula;
- `FORMULA_REMOVED`: the reference contains a formula and the candidate contains a value;
- `FORMULA_CHANGED`: both contain formulas, but their formula text differs.

For example, a candidate changing `=SUM(A1)` to `=SUM(A2)` produces
`FORMULA_CHANGED`. A formula replaced with a typed value produces
`FORMULA_REMOVED`; a typed value replaced with a formula produces
`FORMULA_ADDED`.

Use `unexpected_formula` when a standalone workbook must reject formulas in
input cells. The fill colour is configured as an RGB or ARGB hexadecimal
value; ARGB values use their final six digits as the RGB colour. A tolerance
of `0` requires an exact match. A non-zero `tolerance_percent` allows each
RGB channel to differ by the corresponding percentage of the 0–255 range.

```yaml
workbook_checks:
  - rule_code: unexpected_formula
    enabled: true
    scope: all_sheets
    options:
      fill_colors:
        - "#FFFF00"       # yellow input cells
      tolerance_percent: 0
```

Each finding includes the worksheet and Excel cell reference, for example
`Inputs!A1`. A formula in an unfilled cell, or in a cell whose fill does not
match the configured colours, is not reported by `unexpected_formula`.

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

Use `value_type` for conditional text or numeric requirements. The condition
uses another column and exactly one comparison operator:

```yaml
column_validations:
  - column: Measure_Value
    value_type: text
    when:
      column: Unit
      equals: Text
  - column: Measure_Value
    value_type: numeric
    when:
      column: Unit
      not_equals: Text
```

`text` accepts strings. `numeric` accepts numeric values, but not booleans or
numeric-looking strings. If the condition column is missing, the check is
`NOT_RUN`; if the condition value is null, the condition does not match.

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

For duplicate identities within one table, use a table validation. Repeated
identities with the same values are allowed; only conflicting values fail:

```yaml
table_validations:
  - rule_code: conflicting_duplicate
    key_columns:
      mode: pattern
      pattern: ".*_Cd$"
    value_columns:
      - Measure_Value
```

`key_columns.mode` may be `explicit`, `all`, or `pattern`. Value columns are
always explicit so that they cannot accidentally become part of the identity.
Missing columns or a selector matching no columns produce a finding and mark
the table validation `NOT_RUN`.

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

`formula_difference` reports formula changes between candidate and reference
workbooks with these reasons: `FORMULA_ADDED`, `FORMULA_REMOVED`, and
`FORMULA_CHANGED`. To reject formulas in coloured input cells without needing
a reference workbook, use `unexpected_formula`:

```yaml
workbook_checks:
  - rule_code: unexpected_formula
    enabled: true
    scope: all_sheets
    options:
      fill_colors: ["#FFFF00"]
      tolerance_percent: 0
```

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
