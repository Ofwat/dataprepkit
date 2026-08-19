from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


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
        return self


class TableConfig(_PublicModel):
    name: str
    sheet_selector: SheetSelector | None = None
    required: bool = False
    header_row: int | None = None
    data_boundary: dict[str, Any] | None = None
    column_definitions: list[dict[str, Any]] = Field(default_factory=list)
    column_validations: list[ColumnValidation] = Field(default_factory=list)
    header_policy: dict[str, Any] | None = None
    empty_row_rules: list[dict[str, Any]] = Field(default_factory=list)
    data_presence: str = "allow_empty"


class WorkbookValidationConfig(_PublicModel):
    config_version: str
    profile_name: str | None = None
    comparison: ComparisonConfig
    sheet_policy: SheetPolicy
    tables: list[TableConfig] = Field(default_factory=list)
    expected_cells: list[dict[str, Any]] = Field(default_factory=list)
    workbook_checks: list[dict[str, Any]] = Field(default_factory=list)
    enabled_rules: list[str] | None = None
    rule_severity: dict[str, str] = Field(default_factory=dict)
    runtime: RuntimePolicy
    compatibility: dict[str, Any] | None = None


class ValidationEvent(_PublicModel):
    rule_code: str
    status: str = "FAILED"
    severity: str | None = None
    sheet_name: str | None = None
    description: str = ""
    cell_reference: str | None = None
    row_number: int | None = None


class DiagnosticEvent(_PublicModel):
    run_id: str | None = None
    code: str
    description: str


class ValidationResult(_PublicModel):
    run_id: str | None = None
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
