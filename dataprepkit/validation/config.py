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
    raise ConfigurationError(
        "format must be mapping or json",
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
        if isinstance(source, Path) or (
            isinstance(source, str)
            and not source.lstrip().startswith(("{", "["))
        ):
            payload = Path(source).read_text(encoding="utf-8")
        else:
            payload = source
        return validate_config(json.loads(payload))
    except (OSError, json.JSONDecodeError, TypeError) as error:
        raise ConfigurationError(
            "configuration must be a mapping or valid JSON",
            field_path="source",
        ) from error
