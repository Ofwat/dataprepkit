from __future__ import annotations

from pathlib import Path

import openpyxl

from .config import validate_config
from .models import (
    ValidationEvent,
    ValidationResult,
    WorkbookReadError,
    WorkbookValidationConfig,
)


def validate_excel(
    candidate_path,
    config: WorkbookValidationConfig,
    reference_path=None,
    candidate_version=None,
    reference_version=None,
    run_id=None,
    profile=None,
):
    del reference_path, candidate_version, reference_version, profile
    resolved_config = validate_config(config)
    candidate_path = Path(candidate_path)
    try:
        workbook = openpyxl.load_workbook(
            candidate_path,
            read_only=resolved_config.runtime.read_only,
            data_only=False,
            keep_vba=candidate_path.suffix.lower() == ".xlsm",
        )
    except Exception as error:
        raise WorkbookReadError(str(error)) from error

    try:
        errors = []
        sheet_names = set(workbook.sheetnames)
        for selector in resolved_config.sheet_policy.required_selectors:
            matches = _match_sheets(sheet_names, selector)
            if not matches:
                errors.append(
                    ValidationEvent(
                        rule_code="required_sheet",
                        sheet_name=selector.value,
                        description=(
                            f"Required sheet selector did not match: "
                            f"{selector.value}"
                        ),
                    )
                )
        return ValidationResult(run_id=run_id, errors=errors)
    finally:
        workbook.close()


def _match_sheets(sheet_names, selector):
    if selector.mode != "exact":
        return []
    if selector.case_sensitive:
        return [name for name in sheet_names if name == selector.value]
    expected = selector.value.casefold()
    return [name for name in sheet_names if name.casefold() == expected]
