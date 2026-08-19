from __future__ import annotations

import re
from typing import Any

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
    external_links: str = "ignore"
    charts: str = "ignore"
    pivot_tables: str = "ignore"
    named_ranges: str = "ignore"
    merged_cells: str = "ignore"
    macros: str = "reject"


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
        return self


class ExpectedCellCheck(_PublicModel):
    name: str
    locations: list[CellLocation] = Field(min_length=1)
    fallback_strategy: str = "ordered_first"
    expected_value: Any
    comparison: CellComparison = Field(default_factory=CellComparison)
    enabled: bool = True
    severity: str | None = None


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


class WorkbookCheck(_PublicModel):
    rule_code: str
    enabled: bool
    scope: str
    options: dict[str, Any] | None = None
    severity: str | None = None


class WorkbookValidationConfig(_PublicModel):
    config_version: str
    profile_name: str | None = None
    comparison: ComparisonConfig
    sheet_policy: SheetPolicy
    tables: list[TableConfig] = Field(default_factory=list)
    expected_cells: list[ExpectedCellCheck] = Field(default_factory=list)
    workbook_checks: list[WorkbookCheck] = Field(default_factory=list)
    enabled_rules: list[str] | None = None
    rule_severity: dict[str, str] = Field(default_factory=dict)
    runtime: RuntimePolicy
    compatibility: dict[str, Any] | None = None


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
    reference_version: str | None = None
    comparison: ComparisonConfig | None = None
    complete: bool = True
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
