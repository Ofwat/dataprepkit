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
    del candidate_version, reference_version, profile
    resolved_config = validate_config(config)
    candidate_path = Path(candidate_path)
    formula_workbook = None
    value_workbook = None
    reference_workbook = None
    try:
        formula_workbook = openpyxl.load_workbook(
            candidate_path,
            read_only=resolved_config.runtime.read_only,
            data_only=False,
            keep_vba=candidate_path.suffix.lower() == ".xlsm",
        )
        value_workbook = openpyxl.load_workbook(
            candidate_path,
            read_only=resolved_config.runtime.read_only,
            data_only=True,
            keep_vba=candidate_path.suffix.lower() == ".xlsm",
        )
        if reference_path is not None:
            reference_workbook = openpyxl.load_workbook(
                reference_path,
                read_only=resolved_config.runtime.read_only,
                data_only=True,
                keep_vba=Path(reference_path).suffix.lower() == ".xlsm",
            )
    except Exception as error:
        if formula_workbook is not None:
            formula_workbook.close()
        if value_workbook is not None:
            value_workbook.close()
        if reference_workbook is not None:
            reference_workbook.close()
        raise WorkbookReadError(str(error)) from error

    try:
        errors = []
        not_run = []
        sheet_names = set(formula_workbook.sheetnames)
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
        for check in resolved_config.workbook_checks:
            if not check.enabled:
                continue
            if check.rule_code == "missing_reference_sheet":
                if reference_workbook is None:
                    not_run.append(
                        ValidationEvent(
                            rule_code="missing_reference_sheet",
                            status="NOT_RUN",
                            reason="REFERENCE_WORKBOOK_REQUIRED",
                            description=(
                                "Reference workbook is required for this check"
                            ),
                        )
                    )
                    continue
                candidate_names = set(formula_workbook.sheetnames)
                for sheet_name in reference_workbook.sheetnames:
                    if sheet_name not in candidate_names:
                        errors.append(
                            ValidationEvent(
                                rule_code="missing_reference_sheet",
                                sheet_name=sheet_name,
                                description=(
                                    f"Reference sheet missing from candidate: "
                                    f"{sheet_name}"
                                ),
                            )
                        )
                continue
            if check.rule_code != "formula_error":
                continue
            tokens = (check.options or {}).get("error_tokens", [])
            for sheet in value_workbook.worksheets:
                for row in sheet.iter_rows():
                    for cell in row:
                        if cell.data_type == "e" or cell.value in tokens:
                            errors.append(
                                ValidationEvent(
                                    rule_code="formula_error",
                                    sheet_name=sheet.title,
                                    cell_reference=cell.coordinate,
                                    actual_value=cell.value,
                                    expected_value=tokens,
                                    description=(
                                        f"Excel error value found: {cell.value}"
                                    ),
                                )
                            )
        return ValidationResult(
            run_id=run_id,
            errors=errors,
            not_run=not_run,
        )
    finally:
        formula_workbook.close()
        value_workbook.close()
        if reference_workbook is not None:
            reference_workbook.close()


def _match_sheets(sheet_names, selector):
    if selector.mode != "exact":
        return []
    if selector.case_sensitive:
        return [name for name in sheet_names if name == selector.value]
    expected = selector.value.casefold()
    return [name for name in sheet_names if name.casefold() == expected]
