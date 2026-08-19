from __future__ import annotations

from pathlib import Path

import openpyxl
from openpyxl.utils.cell import column_index_from_string

from .config import validate_config
from .models import (
    ConfigurationError,
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
        for expected_cell in resolved_config.expected_cells:
            if not expected_cell.enabled:
                not_run.append(
                    ValidationEvent(
                        rule_code="expected_cell",
                        status="NOT_RUN",
                        reason="RULE_DISABLED",
                        severity=expected_cell.severity
                        or resolved_config.rule_severity.get("expected_cell"),
                        description="Rule is disabled by configuration",
                    )
                )
                continue
            resolved_locations = []
            for location in expected_cell.locations:
                matches = _match_sheets(
                    value_workbook.sheetnames,
                    location.sheet_selector,
                )
                resolved_locations.extend(
                    (sheet_name, location.cell_reference) for sheet_name in matches
                )
                if (
                    expected_cell.fallback_strategy == "ordered_first"
                    and resolved_locations
                ):
                    break
            if not resolved_locations:
                errors.append(
                    ValidationEvent(
                        rule_code="expected_cell",
                        severity=expected_cell.severity
                        or resolved_config.rule_severity.get("expected_cell"),
                        description=(
                            f"Expected cell location did not resolve: "
                            f"{expected_cell.name}"
                        ),
                    )
                )
                continue
            if (
                expected_cell.fallback_strategy == "exactly_one"
                and len(resolved_locations) != 1
            ):
                errors.append(
                    ValidationEvent(
                        rule_code="expected_cell",
                        severity=expected_cell.severity
                        or resolved_config.rule_severity.get("expected_cell"),
                        description=(
                            f"Expected cell requires exactly one location: "
                            f"{expected_cell.name}"
                        ),
                    )
                )
                continue
            for sheet_name, cell_reference in resolved_locations:
                actual_cell = value_workbook[sheet_name][cell_reference]
                actual_value = actual_cell.value
                if _values_equal(
                    actual_value,
                    expected_cell.expected_value,
                    expected_cell.comparison.mode,
                ):
                    continue
                errors.append(
                    ValidationEvent(
                        rule_code="expected_cell",
                        severity=expected_cell.severity
                        or resolved_config.rule_severity.get("expected_cell"),
                        sheet_name=sheet_name,
                        cell_reference=cell_reference,
                        actual_value=actual_value,
                        expected_value=expected_cell.expected_value,
                        description=(
                            f"Expected cell value differs: {expected_cell.name}"
                        ),
                    )
                )
                if expected_cell.fallback_strategy == "ordered_first":
                    break
        for table in resolved_config.tables:
            if table.header_policy is None or table.sheet_selector is None:
                continue
            sheet_matches = _match_sheets(
                value_workbook.sheetnames,
                table.sheet_selector,
            )
            if not sheet_matches:
                continue
            if table.header_row is None:
                continue
            definitions = {definition.name: definition for definition in table.column_definitions}
            for sheet_name in sheet_matches:
                sheet = value_workbook[sheet_name]
                headers = [
                    sheet.cell(table.header_row, column).value
                    for column in range(1, sheet.max_column + 1)
                ]
                while headers and headers[-1] is None:
                    headers.pop()
                normalised_headers = {
                    _normalise_header(header): index
                    for index, header in enumerate(headers)
                    if header is not None
                }
                logical_columns = {}
                for definition in table.column_definitions:
                    names = [definition.name, *definition.aliases]
                    for name in names:
                        position = normalised_headers.get(_normalise_header(name))
                        if position is not None:
                            logical_columns[definition.name] = position + 1
                            break
                required_positions = []
                missing_columns = []
                for logical_name in table.header_policy.required_columns:
                    definition = definitions.get(logical_name)
                    names = [logical_name]
                    if definition is not None:
                        names.extend(definition.aliases)
                    position = next(
                        (
                            normalised_headers[_normalise_header(name)]
                            for name in names
                            if _normalise_header(name) in normalised_headers
                        ),
                        None,
                    )
                    if position is None:
                        missing_columns.append(logical_name)
                    else:
                        required_positions.append(position)
                if (
                    table.header_policy.match_mode == "contains_in_order"
                    and required_positions != sorted(required_positions)
                ):
                    missing_columns.append("required columns are out of order")
                header_resolution_failed = bool(missing_columns)
                if missing_columns and table.header_policy.missing_column_action == "error":
                    errors.append(
                        ValidationEvent(
                            rule_code="column_header",
                            sheet_name=sheet_name,
                            actual_value=headers,
                            expected_value=table.header_policy.required_columns,
                            description=(
                                f"Table '{table.name}' header validation failed: "
                                f"{', '.join(missing_columns)}"
                            ),
                        )
                    )
                if table.data_presence == "require_one_usable_row":
                    formula_sheet = formula_workbook[sheet_name]
                    has_data_row = False
                    max_row = max(sheet.max_row, formula_sheet.max_row)
                    max_column = max(sheet.max_column, formula_sheet.max_column)
                    for row_number in range(table.header_row + 1, max_row + 1):
                        if any(
                            sheet.cell(row_number, column).value is not None
                            or formula_sheet.cell(row_number, column).value is not None
                            for column in range(1, max_column + 1)
                        ):
                            has_data_row = True
                            break
                    if not has_data_row:
                        errors.append(
                            ValidationEvent(
                                rule_code="non_empty_data",
                                sheet_name=sheet_name,
                                expected_value="at least one usable data row",
                                description=(
                                    f"Required table '{table.name}' has no "
                                    "usable data rows"
                                ),
                            )
                        )
                formula_sheet = formula_workbook[sheet_name]
                max_column = max(sheet.max_column, formula_sheet.max_column)
                for empty_row_rule in table.empty_row_rules:
                    excluded_columns = {
                        column_index_from_string(column)
                        for column in empty_row_rule.excluded_columns
                    }
                    for row_number in empty_row_rule.rows:
                        for column_number in range(1, max_column + 1):
                            if column_number in excluded_columns:
                                continue
                            cell = sheet.cell(row_number, column_number)
                            if _is_blank_value(
                                cell.value,
                                empty_row_rule.blank_policy,
                                resolved_config.comparison.null_tokens,
                            ):
                                continue
                            errors.append(
                                ValidationEvent(
                                    rule_code="empty_row_pattern",
                                    sheet_name=sheet_name,
                                    cell_reference=cell.coordinate,
                                    row_number=row_number,
                                    actual_value=cell.value,
                                    expected_value="blank",
                                    description=(
                                        f"Configured empty row {row_number} "
                                        "contains a value"
                                    ),
                                )
                            )
                max_row = max(sheet.max_row, formula_workbook[sheet_name].max_row)
                if header_resolution_failed:
                    for validation in table.column_validations:
                        not_run.append(
                            ValidationEvent(
                                rule_code=(
                                    "allowed_values"
                                    if validation.allowed_values is not None
                                    else "forbidden_values"
                                ),
                                status="NOT_RUN",
                                reason="TABLE_HEADER_RESOLUTION_FAILED",
                                severity=validation.severity
                                or resolved_config.rule_severity.get(
                                    "allowed_values"
                                    if validation.allowed_values is not None
                                    else "forbidden_values"
                                ),
                                description=(
                                    f"Column rule for '{validation.column}' "
                                    "requires resolved table headers"
                                ),
                            )
                        )
                    continue
                for validation in table.column_validations:
                    column_number = logical_columns.get(validation.column)
                    if column_number is None:
                        continue
                    seen_values = {}
                    for row_number in range(table.header_row + 1, max_row + 1):
                        cell = sheet.cell(row_number, column_number)
                        actual_value = cell.value
                        is_null = actual_value is None or (
                            resolved_config.comparison.empty_string_is_null
                            and actual_value == ""
                        )
                        if validation.required and is_null:
                            errors.append(
                                ValidationEvent(
                                    rule_code="missing_value",
                                    severity=validation.severity
                                    or resolved_config.rule_severity.get(
                                        "missing_value"
                                    ),
                                    sheet_name=sheet_name,
                                    cell_reference=cell.coordinate,
                                    row_number=row_number,
                                    actual_value=actual_value,
                                    expected_value="non-null value",
                                    description=(
                                        f"Required value is missing for column "
                                        f"'{validation.column}'"
                                    ),
                                )
                            )
                        if is_null and validation.null_policy == "allow":
                            continue
                        if validation.unique:
                            value_key = repr(actual_value)
                            if value_key in seen_values:
                                errors.append(
                                    ValidationEvent(
                                        rule_code="duplicate_value",
                                        severity=validation.severity
                                        or resolved_config.rule_severity.get(
                                            "duplicate_value"
                                        ),
                                        sheet_name=sheet_name,
                                        cell_reference=cell.coordinate,
                                        row_number=row_number,
                                        actual_value=actual_value,
                                        expected_value="unique value",
                                        description=(
                                            f"Duplicate value found for column "
                                            f"'{validation.column}'"
                                        ),
                                    )
                                )
                            else:
                                seen_values[value_key] = cell.coordinate
                        if (
                            validation.allowed_values is not None
                            and actual_value not in validation.allowed_values
                        ):
                            errors.append(
                                ValidationEvent(
                                    rule_code="allowed_values",
                                    severity=validation.severity
                                    or resolved_config.rule_severity.get(
                                        "allowed_values"
                                    ),
                                    sheet_name=sheet_name,
                                    cell_reference=cell.coordinate,
                                    row_number=row_number,
                                    actual_value=actual_value,
                                    expected_value=validation.allowed_values,
                                    description=(
                                        f"Value is not allowed for column "
                                        f"'{validation.column}'"
                                    ),
                                )
                            )
                        if (
                            validation.forbidden_values is not None
                            and actual_value in validation.forbidden_values
                        ):
                            errors.append(
                                ValidationEvent(
                                    rule_code="forbidden_values",
                                    severity=validation.severity
                                    or resolved_config.rule_severity.get(
                                        "forbidden_values"
                                    ),
                                    sheet_name=sheet_name,
                                    cell_reference=cell.coordinate,
                                    row_number=row_number,
                                    actual_value=actual_value,
                                    expected_value=validation.forbidden_values,
                                    description=(
                                        f"Value is forbidden for column "
                                        f"'{validation.column}'"
                                    ),
                                )
                            )
        for check_index, check in enumerate(resolved_config.workbook_checks):
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
            if check.rule_code == "sheet_structure":
                if reference_workbook is None:
                    not_run.append(
                        ValidationEvent(
                            rule_code="sheet_structure",
                            status="NOT_RUN",
                            reason="REFERENCE_WORKBOOK_REQUIRED",
                            description=(
                                "Reference workbook is required for this check"
                            ),
                        )
                    )
                    continue
                reference_sheets = {
                    sheet.title: sheet for sheet in reference_workbook.worksheets
                }
                reference_formula_sheets = {
                    sheet.title: sheet
                    for sheet in reference_formula_workbook.worksheets
                }
                candidate_formula_sheets = {
                    sheet.title: sheet
                    for sheet in formula_workbook.worksheets
                }
                for candidate_sheet in value_workbook.worksheets:
                    reference_sheet = reference_sheets.get(candidate_sheet.title)
                    if reference_sheet is None:
                        continue
                    actual_shape = _content_shape(
                        candidate_sheet,
                        candidate_formula_sheets[candidate_sheet.title],
                    )
                    expected_shape = _content_shape(
                        reference_sheet,
                        reference_formula_sheets[reference_sheet.title],
                    )
                    if actual_shape == expected_shape:
                        continue
                    errors.append(
                        ValidationEvent(
                            rule_code="sheet_structure",
                            sheet_name=candidate_sheet.title,
                            actual_value=actual_shape,
                            expected_value=expected_shape,
                            description=(
                                "Candidate used area differs from reference"
                            ),
                        )
                    )
                continue
            if check.rule_code == "formula_difference":
                whitespace_policy = (check.options or {}).get(
                    "whitespace_policy",
                    "normalised",
                )
                if whitespace_policy not in {"exact", "normalised"}:
                    raise ConfigurationError(
                        "must be 'exact' or 'normalised'",
                        field_path=(
                            f"workbook_checks[{check_index}]"
                            ".options.whitespace_policy"
                        ),
                    )
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
                            candidate_formula = _normalise_formula(
                                candidate_cell.value,
                                whitespace_policy,
                            )
                            reference_formula = _normalise_formula(
                                reference_cell.value,
                                whitespace_policy,
                            )
                            if candidate_formula == reference_formula:
                                continue
                            errors.append(
                                ValidationEvent(
                                    rule_code="formula_difference",
                                    sheet_name=candidate_sheet.title,
                                    cell_reference=candidate_cell.coordinate,
                                    actual_value=candidate_formula,
                                    expected_value=reference_formula,
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


def _normalise_formula(value, whitespace_policy="normalised"):
    text = getattr(value, "text", None)
    if text is None:
        return _normalise_formula_text(value, whitespace_policy)
    return {
        "text": _normalise_formula_text(text, whitespace_policy),
        "ref": getattr(value, "ref", None),
    }


def _normalise_formula_text(value, whitespace_policy):
    if not isinstance(value, str):
        return value
    if whitespace_policy == "exact":
        return value
    text = value.strip()
    if text.startswith("="):
        return "=" + text[1:].lstrip()
    return text


def _values_equal(actual, expected, mode):
    if mode == "exact":
        return actual == expected
    if mode in {"normalised", "case_insensitive"}:
        if isinstance(actual, str) and isinstance(expected, str):
            actual_text = actual.strip()
            expected_text = expected.strip()
            if mode == "case_insensitive":
                return actual_text.casefold() == expected_text.casefold()
            return actual_text == expected_text
    return actual == expected


def _normalise_header(value):
    return str(value).strip().casefold()


def _is_blank_value(value, blank_policy, null_tokens):
    if blank_policy == "none_only":
        return value is None
    if blank_policy == "blank_strings":
        return value is None or value == ""
    if blank_policy == "blank_strings_and_nulls":
        return value is None or value == "" or value in null_tokens
    return value is None


def _content_shape(value_sheet, formula_sheet):
    max_row = 0
    max_column = 0
    scan_max_row = max(value_sheet.max_row, formula_sheet.max_row)
    scan_max_column = max(value_sheet.max_column, formula_sheet.max_column)
    for row_number in range(1, scan_max_row + 1):
        for column_number in range(1, scan_max_column + 1):
            value_cell = value_sheet.cell(row_number, column_number)
            formula_cell = formula_sheet.cell(row_number, column_number)
            if value_cell.value is None and formula_cell.value is None:
                continue
            max_row = max(max_row, row_number)
            max_column = max(max_column, column_number)
    return {"max_row": max_row, "max_column": max_column}
