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
    reference_formula_workbook = None
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
            reference_path = Path(reference_path)
            reference_formula_workbook = openpyxl.load_workbook(
                reference_path,
                read_only=resolved_config.runtime.read_only,
                data_only=False,
                keep_vba=reference_path.suffix.lower() == ".xlsm",
            )
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
        if reference_formula_workbook is not None:
            reference_formula_workbook.close()
        raise WorkbookReadError(str(error)) from error

    try:
        errors = []
        warnings = []
        not_run = []
        sheet_names = set(formula_workbook.sheetnames)
        selected_sheet_names = set()
        for selector in resolved_config.sheet_policy.required_selectors:
            matches = _match_sheets(sheet_names, selector)
            selected_sheet_names.update(matches)
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
        for selector in resolved_config.sheet_policy.ignored_selectors:
            selected_sheet_names.update(_match_sheets(sheet_names, selector))
        extra_sheets = sheet_names - selected_sheet_names
        action = resolved_config.sheet_policy.extra_sheet_action
        for sheet_name in formula_workbook.sheetnames:
            if sheet_name not in extra_sheets or action == "ignore":
                continue
            event = ValidationEvent(
                rule_code="extra_sheet",
                severity=action,
                sheet_name=sheet_name,
                description=f"Extra sheet found: {sheet_name}",
            )
            if action == "warning":
                warnings.append(event)
            elif action == "error":
                errors.append(event)
        for check in resolved_config.workbook_checks:
            if (
                not check.enabled
                or (
                    resolved_config.enabled_rules is not None
                    and check.rule_code not in resolved_config.enabled_rules
                )
            ):
                not_run.append(
                    ValidationEvent(
                        rule_code=check.rule_code,
                        status="NOT_RUN",
                        reason="RULE_DISABLED",
                        severity=check.severity
                        or resolved_config.rule_severity.get(check.rule_code),
                        description="Rule is disabled by configuration",
                    )
                )
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
            if check.rule_code == "formula_difference":
                if reference_formula_workbook is None:
                    not_run.append(
                        ValidationEvent(
                            rule_code="formula_difference",
                            status="NOT_RUN",
                            reason="REFERENCE_WORKBOOK_REQUIRED",
                            description=(
                                "Reference workbook is required for this check"
                            ),
                        )
                    )
                    continue
                reference_sheets = {
                    sheet.title: sheet for sheet in reference_formula_workbook.worksheets
                }
                for candidate_sheet in formula_workbook.worksheets:
                    reference_sheet = reference_sheets.get(candidate_sheet.title)
                    if reference_sheet is None:
                        continue
                    max_row = max(
                        candidate_sheet.max_row,
                        reference_sheet.max_row,
                    )
                    max_column = max(
                        candidate_sheet.max_column,
                        reference_sheet.max_column,
                    )
                    for row_number in range(1, max_row + 1):
                        for column_number in range(1, max_column + 1):
                            candidate_cell = candidate_sheet.cell(
                                row=row_number,
                                column=column_number,
                            )
                            reference_cell = reference_sheet.cell(
                                row=row_number,
                                column=column_number,
                            )
                            candidate_is_formula = candidate_cell.data_type == "f"
                            reference_is_formula = reference_cell.data_type == "f"
                            if not (candidate_is_formula or reference_is_formula):
                                continue
                            if candidate_cell.value == reference_cell.value:
                                continue
                            errors.append(
                                ValidationEvent(
                                    rule_code="formula_difference",
                                    sheet_name=candidate_sheet.title,
                                    cell_reference=candidate_cell.coordinate,
                                    actual_value=candidate_cell.value,
                                    expected_value=reference_cell.value,
                                    description=(
                                        "Candidate formula differs from reference"
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
            warnings=warnings,
            not_run=not_run,
        )
    finally:
        formula_workbook.close()
        value_workbook.close()
        if reference_workbook is not None:
            reference_workbook.close()
        if reference_formula_workbook is not None:
            reference_formula_workbook.close()


def _match_sheets(sheet_names, selector):
    if selector.mode != "exact":
        return []
    if selector.case_sensitive:
        return [name for name in sheet_names if name == selector.value]
    expected = selector.value.casefold()
    return [name for name in sheet_names if name.casefold() == expected]
