from __future__ import annotations

from collections.abc import Callable

from .models import ConfigurationError, RuleContext, ValidationEvent


_CUSTOM_RULES: dict[str, Callable[[RuleContext], object]] = {}

_BUILT_IN_RULES = (
    {
        "rule_code": "required_sheet",
        "attachment": "sheet_policy.required_selectors",
        "description": "Require one or more sheets to match configured selectors.",
    },
    {
        "rule_code": "extra_sheet",
        "attachment": "sheet_policy.extra_sheet_action",
        "description": "Report candidate sheets that are not selected or ignored.",
    },
    {
        "rule_code": "expected_cell",
        "attachment": "expected_cells",
        "description": "Compare configured cells with expected values.",
    },
    {
        "rule_code": "table_resolution",
        "attachment": "tables",
        "description": "Resolve required tables and their data boundaries.",
    },
    {
        "rule_code": "column_header",
        "attachment": "tables.header_policy",
        "description": "Validate required, ordered, duplicate, and extra headers.",
    },
    {
        "rule_code": "non_empty_data",
        "attachment": "tables.data_presence",
        "description": "Require at least one usable row in a table.",
    },
    {
        "rule_code": "empty_row_pattern",
        "attachment": "tables.empty_row_rules",
        "description": "Reject values in rows configured to be blank.",
    },
    {
        "rule_code": "missing_value",
        "attachment": "tables.column_validations.required",
        "description": "Reject null or blank values in a configured column.",
    },
    {
        "rule_code": "duplicate_value",
        "attachment": "tables.column_validations.unique",
        "description": "Reject duplicate normalized values in a column.",
    },
    {
        "rule_code": "allowed_values",
        "attachment": "tables.column_validations.allowed_values",
        "description": "Require values to belong to a configured allow-list.",
    },
    {
        "rule_code": "forbidden_values",
        "attachment": "tables.column_validations.forbidden_values",
        "description": "Reject values that belong to a configured deny-list.",
    },
    {
        "rule_code": "pandas_load",
        "attachment": "tables.load_policy",
        "description": "Load a resolved Excel table into pandas.",
    },
    {
        "rule_code": "max_length",
        "attachment": "tables.dataframe_checks",
        "description": "Reject DataFrame values exceeding a configured length.",
    },
    {
        "rule_code": "missing_reference_sheet",
        "attachment": "workbook_checks",
        "description": "Find reference sheets absent from the candidate workbook.",
    },
    {
        "rule_code": "sheet_structure",
        "attachment": "workbook_checks",
        "description": "Compare content-based used areas between workbooks.",
    },
    {
        "rule_code": "formula_difference",
        "attachment": "workbook_checks",
        "description": "Compare candidate and reference formula text.",
    },
    {
        "rule_code": "formula_error",
        "attachment": "workbook_checks",
        "description": "Find configured Excel error values.",
    },
    {
        "rule_code": "feature_policy",
        "attachment": "runtime.feature_policy",
        "description": "Apply actions to detected workbook features.",
    },
    {
        "rule_code": "feature_detection_unavailable",
        "attachment": "runtime.feature_policy.unavailable_action",
        "description": "Apply an action when a workbook feature cannot be inspected.",
    },
)


def register_rule(rule_code: str, validator: Callable[[RuleContext], object]):
    if not rule_code or not isinstance(rule_code, str):
        raise ConfigurationError(
            "rule_code must be a non-empty string",
            field_path="rule_code",
        )
    if not callable(validator):
        raise ConfigurationError(
            "validator must be callable",
            field_path="validator",
        )
    if rule_code in _CUSTOM_RULES:
        raise ConfigurationError(
            f"rule is already registered: {rule_code}",
            field_path="rule_code",
        )
    _CUSTOM_RULES[rule_code] = validator


def unregister_rule(rule_code: str) -> None:
    _CUSTOM_RULES.pop(rule_code, None)


def list_registered_rules() -> list[str]:
    return sorted(_CUSTOM_RULES)


def list_available_rules() -> list[dict[str, str]]:
    """Return built-in rule metadata for discovery and documentation tools."""
    return [rule.copy() for rule in _BUILT_IN_RULES]


def get_registered_rule(rule_code: str):
    return _CUSTOM_RULES.get(rule_code)


def validate_custom_result(value):
    if value is None:
        return []
    if isinstance(value, ValidationEvent):
        return [value]
    if isinstance(value, list):
        return [ValidationEvent.model_validate(event) for event in value]
    raise ConfigurationError(
        "custom validator must return ValidationEvent, list, or None",
        field_path="validator",
    )
