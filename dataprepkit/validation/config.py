from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from .models import (
    ConfigurationError,
    DependencyError,
    WorkbookValidationConfig,
)


def validate_config(
    source: WorkbookValidationConfig | dict[str, Any],
    compatibility_profile=None,
) -> WorkbookValidationConfig:
    if compatibility_profile is not None:
        source_data = (
            source.model_dump(mode="json")
            if isinstance(source, WorkbookValidationConfig)
            else source
        )
        source = _deep_merge(
            _read_config_mapping(compatibility_profile),
            source_data,
        )
    if isinstance(source, WorkbookValidationConfig):
        return source
    try:
        return WorkbookValidationConfig.model_validate(source)
    except ValidationError as error:
        first = error.errors()[0]
        path = "".join(
            f"[{part}]" if isinstance(part, int) else (
                f".{part}" if index else str(part)
            )
            for index, part in enumerate(first["loc"])
        )
        raise ConfigurationError(
            first["msg"],
            field_path=path,
        ) from error


def dump_validation_config(
    config: WorkbookValidationConfig,
    format: str = "mapping",
):
    resolved = validate_config(config)
    if format == "mapping":
        return resolved.model_dump(mode="json")
    if format == "json":
        return json.dumps(
            resolved.model_dump(mode="json"),
            indent=2,
            sort_keys=True,
        )
    if format == "yaml":
        try:
            import yaml
        except ImportError as error:
            from .models import DependencyError

            raise DependencyError(
                "PyYAML is required for YAML configuration"
            ) from error
        return yaml.safe_dump(
            resolved.model_dump(mode="json"),
            sort_keys=False,
        )
    raise ConfigurationError(
        "format must be mapping, json, or yaml",
        field_path="format",
    )


def load_validation_config(
    source: WorkbookValidationConfig | dict[str, Any] | str | Path,
    compatibility_profile=None,
) -> WorkbookValidationConfig:
    source_data = _read_config_mapping(source)
    if compatibility_profile is not None:
        profile_data = _read_config_mapping(compatibility_profile)
        source_data = _deep_merge(profile_data, source_data)
    return validate_config(source_data)


def _read_config_mapping(source):
    if isinstance(source, WorkbookValidationConfig):
        return source.model_dump(mode="json")
    if isinstance(source, dict):
        return source
    try:
        is_path = isinstance(source, Path) or (
            isinstance(source, str)
            and "\n" not in source
            and not source.lstrip().startswith(("{", "["))
            and Path(source).exists()
        )
        if is_path:
            source_path = Path(source)
            payload = Path(source).read_text(encoding="utf-8")
        else:
            source_path = None
            payload = source
        yaml_source = (
            source_path is not None
            and source_path.suffix.lower() in {".yaml", ".yml"}
        ) or (
            source_path is None
            and isinstance(source, str)
            and not source.lstrip().startswith(("{", "["))
        )
        if yaml_source:
            try:
                import yaml
            except ImportError as error:
                raise DependencyError(
                    "PyYAML is required for YAML configuration"
                ) from error
            mapping = yaml.safe_load(payload)
        else:
            mapping = json.loads(payload)
        if not isinstance(mapping, dict):
            raise ConfigurationError(
                "configuration must be a mapping",
                field_path="source",
            )
        return mapping
    except (OSError, json.JSONDecodeError, TypeError) as error:
        raise ConfigurationError(
            "configuration must be a mapping or valid JSON/YAML",
            field_path="source",
        ) from error


def _deep_merge(base, overrides):
    if not isinstance(base, dict) or not isinstance(overrides, dict):
        return overrides
    merged = dict(base)
    for key, value in overrides.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged
