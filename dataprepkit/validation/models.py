from __future__ import annotations

import re
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class ValidationPackageError(Exception):
    """Base error for the public validation API."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class ConfigurationError(ValidationPackageError, ValueError):
    """Raised when validation configuration is invalid."""

    def __init__(self, message: str, field_path: str | None = None):
        rendered = f"{field_path}: {message}" if field_path else message
        super().__init__("CONFIGURATION_ERROR", rendered)
        self.field_path = field_path


class WorkbookReadError(ValidationPackageError, OSError):
    """Raised when an Excel workbook cannot be opened or inspected."""

    def __init__(self, message: str):
        super().__init__("WORKBOOK_READ_ERROR", message)


class DependencyError(ValidationPackageError, ImportError):
    """Raised when an optional validation dependency is unavailable."""

    def __init__(self, message: str):
        super().__init__("DEPENDENCY_ERROR", message)


class ValidationFailedError(ValidationPackageError):
    """Raised by ``ValidationResult.raise_if_invalid``."""

    def __init__(self, result: "ValidationResult"):
        super().__init__("VALIDATION_FAILED", result.format_report())
        self.result = result


class _PublicModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SheetSelector(_PublicModel):
    mode: str
    value: str
    case_sensitive: bool = False
    expected_match_count: int | None = None


class SheetPolicy(_PublicModel):
    required_selectors: list[SheetSelector] = Field(default_factory=list)
    ignored_selectors: list[SheetSelector] = Field(default_factory=list)
    extra_sheet_action: str = "ignore"
    selector_match_action: str = "all"


class ComparisonConfig(_PublicModel):
    case_sensitive: bool
    accent_sensitive: bool
    trim_whitespace: bool
    collapse_internal_whitespace: bool
    empty_string_is_null: bool
    null_tokens: list[Any]
    collation_name: str


class WorkbookFeaturePolicy(_PublicModel):
    unavailable_action: str = "warning"
    external_links: str = "ignore"
    charts: str = "ignore"
    pivot_tables: str = "ignore"
    named_ranges: str = "ignore"
    merged_cells: str = "ignore"
    macros: str = "ignore"

    @model_validator(mode="after")
    def validate_actions(self):
        if self.unavailable_action not in {"ignore", "warning", "error"}:
            raise ValueError(
                "unavailable_action must be ignore, warning, or error"
            )
        for name in (
            "external_links",
            "charts",
            "pivot_tables",
            "named_ranges",
            "merged_cells",
            "macros",
        ):
            if getattr(self, name) not in {"ignore", "warning", "error"}:
                raise ValueError(
                    f"{name} action must be ignore, warning, or error"
                )
        return self


class RuntimePolicy(_PublicModel):
    max_cells_scanned: int
    read_only: bool
    macro_policy: str
    missing_formula_cache_action: str
    feature_policy: WorkbookFeaturePolicy = Field(
        default_factory=WorkbookFeaturePolicy
    )

    @model_validator(mode="after")
    def validate_policy(self):
        if self.macro_policy not in {"preserve", "discard", "reject"}:
            raise ValueError(
                "macro_policy must be preserve, discard, or reject"
            )
        if self.missing_formula_cache_action not in {"not_run", "error"}:
            raise ValueError(
                "missing_formula_cache_action must be not_run or error"
            )
        return self


class ColumnValidation(_PublicModel):
    column: str
    required: bool = False
    unique: bool = False
    allowed_values: list[Any] | None = None
    forbidden_values: list[Any] | None = None
    null_policy: str | None = None
    severity: str | None = None

    @model_validator(mode="after")
    def validate_value_lists(self):
        if self.allowed_values is not None and self.forbidden_values is not None:
            raise ValueError(
                "allowed_values and forbidden_values are mutually exclusive"
            )
        if self.null_policy not in {None, "ignore", "error", "allow"}:
            raise ValueError("null_policy must be ignore, error, or allow")
        return self


class CellLocation(_PublicModel):
    sheet_selector: SheetSelector
    cell_reference: str

    @field_validator("cell_reference")
    @classmethod
    def validate_cell_reference(cls, value):
        if not re.fullmatch(r"\$?[A-Za-z]{1,3}\$?[1-9][0-9]*", value):
            raise ValueError("must be an unqualified Excel A1 cell reference")
        return value


class CellComparison(_PublicModel):
    mode: str = "exact"
    options: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_mode(self):
        if self.mode not in {
            "exact",
            "normalised",
            "case_insensitive",
            "numeric",
            "date",
        }:
            raise ValueError("invalid cell comparison mode")
        if self.mode == "numeric":
            required = {
                "decimal_separator",
                "thousands_separator",
                "allow_numeric_strings",
            }
            if not required.issubset(self.options):
                raise ValueError(
                    "numeric comparison requires decimal_separator, "
                    "thousands_separator, and allow_numeric_strings"
                )
        if self.mode == "date" and not self.options.get("accepted_formats"):
            raise ValueError(
                "date comparison requires accepted_formats"
            )
        allowed_options = {
            "numeric": {
                "decimal_separator",
                "thousands_separator",
                "allow_numeric_strings",
            },
            "date": {"accepted_formats", "timezone"},
            "exact": set(),
            "normalised": set(),
            "case_insensitive": set(),
        }[self.mode]
        unknown_options = set(self.options) - allowed_options
        if unknown_options:
            raise ValueError(
                f"unsupported options for {self.mode} comparison: "
                f"{sorted(unknown_options)}"
            )
        return self


class ExpectedCellCheck(_PublicModel):
    name: str
    locations: list[CellLocation] = Field(min_length=1)
    fallback_strategy: str = "ordered_first"
    expected_value: Any
    comparison: CellComparison = Field(default_factory=CellComparison)
    enabled: bool = True
    severity: str | None = None

    @field_validator("fallback_strategy")
    @classmethod
    def validate_fallback_strategy(cls, value):
        if value not in {"ordered_first", "exactly_one"}:
            raise ValueError(
                "fallback_strategy must be ordered_first or exactly_one"
            )
        return value


class ColumnDefinition(_PublicModel):
    name: str
    aliases: list[str] = Field(default_factory=list)
    comparison: ComparisonConfig | None = None


class HeaderPolicy(_PublicModel):
    required_columns: list[str] = Field(default_factory=list)
    match_mode: str = "exact_order"
    extra_column_action: str = "allowed"
    missing_column_action: str = "error"

    @model_validator(mode="after")
    def validate_policy(self):
        if self.match_mode not in {
            "exact_order",
            "contains_in_order",
            "contains_any_order",
        }:
            raise ValueError("invalid header match_mode")
        if self.extra_column_action not in {"allowed", "warning", "error"}:
            raise ValueError("invalid extra_column_action")
        if self.missing_column_action not in {"warning", "error"}:
            raise ValueError("invalid missing_column_action")
        return self


class EmptyRowRule(_PublicModel):
    rows: list[int] = Field(min_length=1)
    excluded_columns: list[str] = Field(default_factory=list)
    blank_policy: str = "none_only"

    @model_validator(mode="after")
    def validate_policy(self):
        if any(row < 1 for row in self.rows):
            raise ValueError("rows must contain positive Excel row numbers")
        if self.blank_policy not in {
            "none_only",
            "blank_strings",
            "blank_strings_and_nulls",
        }:
            raise ValueError("invalid blank_policy")
        return self


class DataBoundary(_PublicModel):
    mode: str
    columns: list[str] | None = None
    end_row: int | None = None
    table_name: str | None = None

    @model_validator(mode="after")
    def validate_mode_fields(self):
        if self.mode not in {
            "last_non_empty_row",
            "fixed_end_row",
            "excel_table",
        }:
            raise ValueError("mode must be last_non_empty_row, fixed_end_row, or excel_table")
        if self.mode == "last_non_empty_row":
            if not self.columns:
                raise ValueError("columns is required for last_non_empty_row")
            if self.end_row is not None or self.table_name is not None:
                raise ValueError("only columns may be supplied for last_non_empty_row")
        if self.mode == "fixed_end_row":
            if self.end_row is None or self.end_row < 1:
                raise ValueError("end_row must be a positive row number")
            if self.columns is not None or self.table_name is not None:
                raise ValueError("only end_row may be supplied for fixed_end_row")
        if self.mode == "excel_table":
            if not self.table_name:
                raise ValueError("table_name is required for excel_table")
            if self.columns is not None or self.end_row is not None:
                raise ValueError("only table_name may be supplied for excel_table")
        return self


class DataFrameLoadPolicy(_PublicModel):
    enabled: bool = True
    infer_types: bool = False
    preserve_empty_values: bool = True


class DataFrameCheck(_PublicModel):
    rule_code: str
    column: str
    max_length: int | None = None
    min_length: int | None = None
    length_mode: str = "characters"
    enabled: bool = True
    severity: str | None = None

    @model_validator(mode="after")
    def validate_options(self):
        if self.rule_code != "max_length":
            raise ValueError("unsupported DataFrame rule")
        if self.max_length is None or self.max_length < 0:
            raise ValueError("max_length must be a non-negative integer")
        if self.min_length is not None and self.min_length < 0:
            raise ValueError("min_length must be a non-negative integer")
        if self.length_mode != "characters":
            raise ValueError("length_mode must be characters")
        return self


class CrossTableCheck(_PublicModel):
    name: str
    rule_code: str = "values_in_reference"
    source_table: str
    source_column: str
    reference_table: str
    reference_column: str
    enabled: bool = True
    severity: str | None = None

    @model_validator(mode="after")
    def validate_rule(self):
        if self.rule_code != "values_in_reference":
            raise ValueError("unsupported cross-table rule")
        return self


class TableConfig(_PublicModel):
    name: str
    sheet_selector: SheetSelector | None = None
    required: bool = False
    header_row: int | None = None
    data_boundary: DataBoundary | None = None
    column_definitions: list[ColumnDefinition] = Field(default_factory=list)
    column_validations: list[ColumnValidation] = Field(default_factory=list)
    header_policy: HeaderPolicy | None = None
    empty_row_rules: list[EmptyRowRule] = Field(default_factory=list)
    data_presence: str = "allow_empty"
    load_policy: DataFrameLoadPolicy | None = None
    dataframe_checks: list[DataFrameCheck] = Field(default_factory=list)

    @field_validator("data_presence")
    @classmethod
    def validate_data_presence(cls, value):
        if value not in {"allow_empty", "require_one_usable_row"}:
            raise ValueError(
                "data_presence must be allow_empty or "
                "require_one_usable_row"
            )
        return value


class WorkbookCheck(_PublicModel):
    rule_code: str
    enabled: bool
    scope: str
    options: dict[str, Any] | None = None
    depends_on: list[str] = Field(default_factory=list)
    severity: str | None = None

    @field_validator("depends_on")
    @classmethod
    def validate_dependencies(cls, value):
        if len(value) != len(set(value)):
            raise ValueError("depends_on must not contain duplicates")
        if any(not dependency for dependency in value):
            raise ValueError("depends_on must contain non-empty rule codes")
        return value

    @model_validator(mode="after")
    def validate_rule_options(self):
        options = self.options or {}
        built_in_options = {
            "formula_error": {"error_tokens"},
            "formula_difference": {"whitespace_policy"},
            "sheet_structure": {"compare_headers", "table_names"},
            "missing_reference_sheet": set(),
        }
        if self.rule_code in built_in_options:
            unknown_options = set(options) - built_in_options[self.rule_code]
            if unknown_options:
                raise ValueError(
                    f"unsupported options for {self.rule_code}: "
                    f"{sorted(unknown_options)}"
                )
        if self.rule_code == "formula_error" and self.enabled:
            if not isinstance(options.get("error_tokens"), list):
                raise ValueError(
                    "formula_error requires error_tokens as a list"
                )
        if self.rule_code == "formula_difference":
            whitespace_policy = options.get(
                "whitespace_policy",
                "normalised",
            )
            if whitespace_policy not in {"exact", "normalised"}:
                raise ValueError(
                    "formula_difference whitespace_policy must be "
                    "exact or normalised"
                )
        if self.rule_code == "sheet_structure":
            if "compare_headers" in options and not isinstance(
                options["compare_headers"],
                bool,
            ):
                raise ValueError(
                    "sheet_structure compare_headers must be a boolean"
                )
            if "table_names" in options and not isinstance(
                options["table_names"],
                list,
            ):
                raise ValueError(
                    "sheet_structure table_names must be a list"
                )
        return self


class WorkbookValidationConfig(_PublicModel):
    config_version: str
    profile_name: str | None = None
    comparison: ComparisonConfig
    sheet_policy: SheetPolicy
    tables: list[TableConfig] = Field(default_factory=list)
    cross_table_checks: list[CrossTableCheck] = Field(default_factory=list)
    expected_cells: list[ExpectedCellCheck] = Field(default_factory=list)
    workbook_checks: list[WorkbookCheck] = Field(default_factory=list)
    enabled_rules: list[str] | None = None
    rule_severity: dict[str, str] = Field(default_factory=dict)
    runtime: RuntimePolicy
    compatibility: dict[str, Any] | None = None

    @model_validator(mode="after")
    def validate_rule_dependencies(self):
        checks = {check.rule_code: check for check in self.workbook_checks}
        if len(checks) != len(self.workbook_checks):
            raise ValueError("workbook_checks must not contain duplicate rule codes")
        for check in self.workbook_checks:
            missing = set(check.depends_on) - checks.keys()
            if missing:
                raise ValueError(
                    f"dependencies for {check.rule_code} are not configured: "
                    f"{sorted(missing)}"
                )

        visiting = set()
        visited = set()

        def visit(rule_code):
            if rule_code in visiting:
                raise ValueError("workbook rule dependencies must not contain cycles")
            if rule_code in visited:
                return
            visiting.add(rule_code)
            for dependency in checks[rule_code].depends_on:
                visit(dependency)
            visiting.remove(rule_code)
            visited.add(rule_code)

        for rule_code in checks:
            visit(rule_code)
        return self


class RuleContext(_PublicModel):
    rule_code: str
    candidate_path: str
    reference_path: str | None = None
    config: WorkbookValidationConfig


class ValidationEvent(_PublicModel):
    rule_code: str
    status: str = "FAILED"
    reason: str | None = None
    severity: str | None = None
    sheet_name: str | None = None
    description: str = ""
    actual_value: Any = None
    expected_value: Any = None
    cell_reference: str | None = None
    row_number: int | None = None
    column_number: int | None = None

    @model_validator(mode="after")
    def default_reason(self):
        if self.reason is None:
            default_reasons = {
                "required_sheet": "REQUIRED_SHEET_MISSING",
                "extra_sheet": "EXTRA_SHEET_DETECTED",
                "expected_cell": "EXPECTED_CELL_MISMATCH",
                "table_resolution": "TABLE_RESOLUTION_FAILED",
                "column_header": "COLUMN_HEADER_INVALID",
                "non_empty_data": "NO_USABLE_DATA",
                "empty_row_pattern": "EMPTY_ROW_VIOLATION",
                "missing_value": "REQUIRED_VALUE_MISSING",
                "duplicate_value": "DUPLICATE_VALUE",
                "allowed_values": "VALUE_NOT_ALLOWED",
                "forbidden_values": "VALUE_FORBIDDEN",
                "missing_reference_sheet": "REFERENCE_SHEET_MISSING",
                "sheet_structure": "SHEET_STRUCTURE_MISMATCH",
                "formula_difference": "FORMULA_DIFFERENCE",
                "sheet_selector": "SHEET_SELECTOR_MULTIPLE_MATCHES",
                "WORKBOOK_LIMIT_ERROR": "WORKBOOK_CELL_LIMIT_EXCEEDED",
            }
            normalized_code = self.rule_code.upper().replace("-", "_")
            self.reason = default_reasons.get(
                self.rule_code,
                f"{normalized_code}_EVENT",
            )
        if self.column_number is None and self.cell_reference is not None:
            match = re.fullmatch(
                r"\$?([A-Za-z]{1,3})\$?[1-9][0-9]*",
                self.cell_reference,
            )
            if match:
                self.column_number = sum(
                    (ord(character.upper()) - ord("A") + 1) * 26**power
                    for power, character in enumerate(reversed(match.group(1)))
                )
        return self


class DiagnosticEvent(_PublicModel):
    run_id: str | None = None
    code: str
    description: str


class ValidationResult(_PublicModel):
    run_id: str | None = None
    config_version: str | None = None
    profile_name: str | None = None
    candidate_filename: str | None = None
    candidate_version: str | None = None
    reference_filename: str | None = None
    reference_version: str | None = None
    comparison: ComparisonConfig | None = None
    complete: bool = True
    processed_counts: dict[str, int] = Field(default_factory=dict)
    processed_severities: dict[str, str | None] = Field(default_factory=dict)
    diagnostics: list[DiagnosticEvent] = Field(default_factory=list)
    errors: list[ValidationEvent] = Field(default_factory=list)
    warnings: list[ValidationEvent] = Field(default_factory=list)
    not_run: list[ValidationEvent] = Field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ValidationResult":
        return cls.model_validate(value)

    def format_report(self) -> str:
        lines = [
            f"Validation {'passed' if self.is_valid else 'failed'}: "
            f"{len(self.errors)} errors, {len(self.warnings)} warnings, "
            f"{len(self.not_run)} not run"
        ]
        for event in [*self.errors, *self.warnings, *self.not_run]:
            location = f" ({event.sheet_name}!{event.cell_reference})" \
                if event.sheet_name and event.cell_reference else ""
            lines.append(f"- {event.rule_code}{location}: {event.description}")
        return "\n".join(lines)

    def raise_if_invalid(self) -> None:
        if not self.is_valid:
            raise ValidationFailedError(self)


VALIDATION_RESULT_DATAFRAME_COLUMNS = [
    "run_id",
    "config_version",
    "profile_name",
    "candidate_filename",
    "candidate_version",
    "reference_filename",
    "reference_version",
    "is_valid",
    "complete",
    "event_type",
    "processed_count",
    "issue_count",
    "rule_code",
    "code",
    "status",
    "reason",
    "severity",
    "sheet_name",
    "cell_reference",
    "row_number",
    "column_number",
    "description",
    "actual_value",
    "expected_value",
]


def validation_result_to_dataframe(result: ValidationResult) -> pd.DataFrame:
    """Convert validation events and diagnostics into a tabular result."""
    metadata = {
        "run_id": result.run_id,
        "config_version": result.config_version,
        "profile_name": result.profile_name,
        "candidate_filename": result.candidate_filename,
        "candidate_version": result.candidate_version,
        "reference_filename": result.reference_filename,
        "reference_version": result.reference_version,
        "is_valid": result.is_valid,
        "complete": result.complete,
    }
    rows = []
    def count_key(rule_code: str | None, code: str | None) -> str:
        return f"code:{code}" if code is not None else str(rule_code)

    issue_counts: dict[str, int] = {}
    error_counts: dict[str, int] = {}
    not_run_counts: dict[str, int] = {}
    reasons: dict[str, set[str]] = {}
    for event in result.errors:
        key = count_key(event.rule_code, None)
        issue_counts[key] = issue_counts.get(key, 0) + 1
        error_counts[key] = error_counts.get(key, 0) + 1
        if event.reason is not None:
            reasons.setdefault(key, set()).add(event.reason)
    for event in result.warnings:
        key = count_key(event.rule_code, None)
        issue_counts[key] = issue_counts.get(key, 0) + 1
        if event.reason is not None:
            reasons.setdefault(key, set()).add(event.reason)
    for event in result.not_run:
        key = count_key(event.rule_code, None)
        not_run_counts[key] = not_run_counts.get(key, 0) + 1
        if event.reason is not None:
            reasons.setdefault(key, set()).add(event.reason)
    for diagnostic in result.diagnostics:
        key = count_key(None, diagnostic.code)
        issue_counts[key] = issue_counts.get(key, 0) + 1
        reasons.setdefault(key, set()).add(diagnostic.code)

    for event_type, events in (
        ("error", result.errors),
        ("warning", result.warnings),
        ("not_run", result.not_run),
    ):
        for event in events:
            rows.append(
                {
                    **metadata,
                    "event_type": event_type,
                    "processed_count": result.processed_counts.get(
                        count_key(event.rule_code, None)
                    ),
                    "issue_count": None,
                    "rule_code": event.rule_code,
                    "status": event.status,
                    "reason": event.reason,
                    "severity": event.severity,
                    "sheet_name": event.sheet_name,
                    "description": event.description,
                    "actual_value": event.actual_value,
                    "expected_value": event.expected_value,
                    "cell_reference": event.cell_reference,
                    "row_number": event.row_number,
                    "column_number": event.column_number,
                }
            )
    for diagnostic in result.diagnostics:
        rows.append(
            {
                    **metadata,
                    "event_type": "diagnostic",
                    "processed_count": result.processed_counts.get(
                        count_key(None, diagnostic.code)
                    ),
                    "issue_count": None,
                    "code": diagnostic.code,
                    "description": diagnostic.description,
                }
            )
    for key, processed_count in result.processed_counts.items():
        if key.startswith("code:"):
            rule_code = None
            code = key[5:]
        else:
            rule_code = key
            code = None
        if error_counts.get(key, 0):
            summary_status = "FAILED"
        elif processed_count == 0 and not_run_counts.get(key, 0):
            summary_status = "NOT_RUN"
        else:
            summary_status = "PASSED"
        summary_reasons = reasons.get(key, set())
        if len(summary_reasons) == 1:
            summary_reason = next(iter(summary_reasons))
        elif len(summary_reasons) > 1:
            summary_reason = "MULTIPLE_REASONS"
        elif code is not None:
            summary_reason = code
        else:
            summary_reason = ValidationEvent(rule_code=rule_code).reason
        rows.append(
            {
                **metadata,
                "event_type": "summary",
                "rule_code": rule_code,
                "code": code,
                "status": summary_status,
                "processed_count": processed_count,
                "issue_count": issue_counts.get(key, 0),
                "reason": summary_reason,
                "severity": result.processed_severities.get(key),
            }
        )
    return pd.DataFrame(rows, columns=VALIDATION_RESULT_DATAFRAME_COLUMNS)


def dump_validation_result(
    result: ValidationResult,
    format: str = "mapping",
):
    if format == "mapping":
        return result.to_dict()
    if format == "json":
        import json

        return json.dumps(
            result.to_dict(),
            indent=2,
            sort_keys=True,
        )
    raise ConfigurationError(
        "format must be mapping or json",
        field_path="format",
    )


def load_validation_result(
    source: ValidationResult | dict[str, Any] | str,
) -> ValidationResult:
    if isinstance(source, ValidationResult):
        return source
    if isinstance(source, dict):
        return ValidationResult.from_dict(source)
    try:
        import json

        return ValidationResult.from_dict(json.loads(source))
    except (TypeError, ValueError) as error:
        raise ConfigurationError(
            "result must be a mapping or valid JSON",
            field_path="source",
        ) from error
