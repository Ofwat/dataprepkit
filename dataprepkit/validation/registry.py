from __future__ import annotations

from collections.abc import Callable

from .models import ConfigurationError, RuleContext, ValidationEvent


_CUSTOM_RULES: dict[str, Callable[[RuleContext], object]] = {}


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
