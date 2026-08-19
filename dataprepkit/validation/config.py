from __future__ import annotations

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
