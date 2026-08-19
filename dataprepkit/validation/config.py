from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from .models import ConfigurationError, WorkbookValidationConfig


def validate_config(
    source: WorkbookValidationConfig | dict[str, Any],
) -> WorkbookValidationConfig:
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
) -> WorkbookValidationConfig:
    if isinstance(source, WorkbookValidationConfig):
        return source
    if isinstance(source, dict):
        return validate_config(source)
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
                from .models import DependencyError

                raise DependencyError(
                    "PyYAML is required for YAML configuration"
                ) from error
            return validate_config(yaml.safe_load(payload))
        return validate_config(json.loads(payload))
    except (OSError, json.JSONDecodeError, TypeError) as error:
        raise ConfigurationError(
            "configuration must be a mapping or valid JSON/YAML",
            field_path="source",
        ) from error
