from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import re
import unicodedata
from pathlib import Path

import openpyxl
from openpyxl.utils.cell import column_index_from_string

from .config import validate_config
from .models import (
    ConfigurationError,
    DiagnosticEvent,
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
    resolved_config = validate_config(config)
    candidate_path = Path(candidate_path)
    result_metadata = {
        "run_id": run_id,
        "config_version": resolved_config.config_version,
        "profile_name": resolved_config.profile_name,
        "candidate_filename": candidate_path.name,
        "candidate_version": candidate_version,
        "reference_version": reference_version,
        "comparison": resolved_config.comparison,
    }
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
        diagnostics = []
        for feature_name, sheet_name in _detected_features(
            formula_workbook,
            candidate_path,
        ):
            action = getattr(
                resolved_config.runtime.feature_policy,
                feature_name,
            )
            description = f"Detected workbook feature: {feature_name}"
            event = ValidationEvent(
                rule_code="feature_policy",
                sheet_name=sheet_name,
                severity=action,
                description=description,
            )
            if action == "error":
                errors.append(event)
            elif action == "warning":
                warnings.append(event)
            else:
                diagnostics.append(
                    DiagnosticEvent(
                        run_id=run_id,
                        code="FEATURE_DETECTED",
                        description=description,
                    )
                )
        sheet_names = set(formula_workbook.sheetnames)
        selected_sheet_names = set()
        for selector in resolved_config.sheet_policy.required_selectors:
            matches = _select_sheet_matches(
                _match_sheets(sheet_names, selector),
                resolved_config.sheet_policy.selector_match_action,
                selector,
                errors,
            )
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
            selected_sheet_names.update(
                _select_sheet_matches(
                    _match_sheets(sheet_names, selector),
                    resolved_config.sheet_policy.selector_match_action,
                    selector,
                    errors,
                )
            )
        ignored_sheet_names = set()
        for selector in resolved_config.sheet_policy.ignored_selectors:
            ignored_sheet_names.update(
                _match_sheets(sheet_names, selector)
            )
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
        observed_cells = _cells_until_limit(
            value_workbook,
            resolved_config.runtime.max_cells_scanned,
        )
        if observed_cells is not None:
            errors.append(
                ValidationEvent(
                    rule_code="WORKBOOK_LIMIT_ERROR",
                    actual_value=observed_cells,
                    expected_value=resolved_config.runtime.max_cells_scanned,
                    description=(
                        "Workbook cell scan limit exceeded; validation stopped"
                    ),
                )
            )
            return ValidationResult(
                **result_metadata,
                complete=False,
                errors=errors,
                warnings=warnings,
                not_run=not_run,
                diagnostics=diagnostics,
            )
        for expected_cell in resolved_config.expected_cells:
            if (
                not expected_cell.enabled
                or (
                    resolved_config.enabled_rules is not None
                    and "expected_cell" not in resolved_config.enabled_rules
                )
            ):
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
                    expected_cell.comparison.options,
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
            sheet_matches = [
                sheet_name
                for sheet_name in sheet_matches
                if sheet_name not in ignored_sheet_names
            ]
            if not sheet_matches:
                if table.required:
                    event = ValidationEvent(
                        rule_code="table_resolution",
                        status=(
                            "FAILED"
                            if (
                                resolved_config.enabled_rules is None
                                or "table_resolution"
                                in resolved_config.enabled_rules
                            )
                            else "NOT_RUN"
                        ),
                        reason=(
                            None
                            if (
                                resolved_config.enabled_rules is None
                                or "table_resolution"
                                in resolved_config.enabled_rules
                            )
                            else "RULE_DISABLED"
                        ),
                        sheet_name=table.sheet_selector.value,
                        severity=resolved_config.rule_severity.get(
                            "table_resolution"
                        ),
                        description=(
                            f"Required table '{table.name}' did not resolve"
                        ),
                    )
                    if event.status == "NOT_RUN":
                        not_run.append(event)
                    else:
                        errors.append(event)
                continue
            if table.header_row is None:
                continue
            definitions = {definition.name: definition for definition in table.column_definitions}
            for sheet_name in sheet_matches:
                enabled_rules = resolved_config.enabled_rules
                header_rule_enabled = (
                    enabled_rules is None or "column_header" in enabled_rules
                )
                data_presence_rule_enabled = (
                    table.data_presence != "require_one_usable_row"
                    or enabled_rules is None
                    or "non_empty_data" in enabled_rules
                )
                empty_row_rule_enabled = (
                    not table.empty_row_rules
                    or enabled_rules is None
                    or "empty_row_pattern" in enabled_rules
                )
                if not header_rule_enabled:
                    not_run.append(
                        ValidationEvent(
                            rule_code="column_header",
                            status="NOT_RUN",
                            reason="RULE_DISABLED",
                            description=(
                                f"Header checks for table '{table.name}' "
                                "are disabled by configuration"
                            ),
                        )
                    )
                sheet = value_workbook[sheet_name]
                headers = [
                    sheet.cell(table.header_row, column).value
                    for column in range(1, sheet.max_column + 1)
                ]
                while headers and headers[-1] is None:
                    headers.pop()
                physical_header_issues = []
                seen_headers = set()
                for header in headers:
                    if header is None or (
                        isinstance(header, str) and not header.strip()
                    ):
                        physical_header_issues.append("blank header")
                        continue
                    normalised_header = _normalise_header(header)
                    if normalised_header in seen_headers:
                        physical_header_issues.append(
                            f"duplicate header: {header}"
                        )
                    seen_headers.add(normalised_header)
                if physical_header_issues and header_rule_enabled:
                    errors.append(
                        ValidationEvent(
                            rule_code="column_header",
                            sheet_name=sheet_name,
                            actual_value=headers,
                            description=(
                                f"Table '{table.name}' has invalid physical "
                                f"headers: {', '.join(physical_header_issues)}"
                            ),
                        )
                    )
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
                data_end_row = _table_data_end_row(
                    table,
                    table.header_row,
                    sheet,
                    formula_workbook[sheet_name],
                    logical_columns,
                )
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
                if table.header_policy.match_mode == "exact_order":
                    if required_positions != list(range(len(required_positions))):
                        missing_columns.append("required columns are out of order")
                elif (
                    table.header_policy.match_mode == "contains_in_order"
                    and required_positions != sorted(required_positions)
                ):
                    missing_columns.append("required columns are out of order")
                header_resolution_failed = bool(
                    missing_columns or physical_header_issues
                )
                boundary_resolution_failed = (
                    table.data_boundary is not None
                    and table.data_boundary.mode == "last_non_empty_row"
                    and any(
                        column not in logical_columns
                        for column in (table.data_boundary.columns or [])
                    )
                )
                if missing_columns and header_rule_enabled:
                    event = ValidationEvent(
                        rule_code="column_header",
                        sheet_name=sheet_name,
                        actual_value=headers,
                        expected_value=table.header_policy.required_columns,
                        description=(
                            f"Table '{table.name}' header validation failed: "
                            f"{', '.join(missing_columns)}"
                        ),
                    )
                    if table.header_policy.missing_column_action == "warning":
                        warnings.append(event)
                    else:
                        errors.append(event)
                known_headers = {
                    _normalise_header(name)
                    for definition in table.column_definitions
                    for name in [definition.name, *definition.aliases]
                }
                extra_headers = [
                    header
                    for header in headers
                    if header is not None
                    and _normalise_header(header) not in known_headers
                ]
                if (
                    extra_headers
                    and header_rule_enabled
                    and table.header_policy.extra_column_action != "allowed"
                ):
                    for header in extra_headers:
                        event = ValidationEvent(
                            rule_code="column_header",
                            sheet_name=sheet_name,
                            actual_value=header,
                            expected_value="known table header",
                            description=(
                                f"Extra table header found in '{table.name}': "
                                f"{header}"
                            ),
                        )
                        if table.header_policy.extra_column_action == "warning":
                            warnings.append(event)
                        else:
                            errors.append(event)
                if table.data_presence == "require_one_usable_row":
                    if not data_presence_rule_enabled:
                        not_run.append(
                            ValidationEvent(
                                rule_code="non_empty_data",
                                status="NOT_RUN",
                                reason="RULE_DISABLED",
                                description=(
                                    f"Required table '{table.name}' data "
                                    "presence check is disabled by configuration"
                                ),
                            )
                        )
                    elif boundary_resolution_failed:
                        not_run.append(
                            ValidationEvent(
                                rule_code="non_empty_data",
                                status="NOT_RUN",
                                reason="DATA_BOUNDARY_RESOLUTION_FAILED",
                                description=(
                                    f"Table '{table.name}' data boundary "
                                    "could not be resolved"
                                ),
                            )
                        )
                    else:
                        formula_sheet = formula_workbook[sheet_name]
                        has_data_row = False
                        max_row = data_end_row
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
                if table.empty_row_rules and not empty_row_rule_enabled:
                    not_run.append(
                        ValidationEvent(
                            rule_code="empty_row_pattern",
                            status="NOT_RUN",
                            reason="RULE_DISABLED",
                            description=(
                                f"Empty row checks for table '{table.name}' "
                                "are disabled by configuration"
                            ),
                        )
                    )
                for empty_row_rule in (
                    table.empty_row_rules if empty_row_rule_enabled else []
                ):
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
                max_row = data_end_row
                if header_resolution_failed:
                    for validation in table.column_validations:
                        column_rule_codes = []
                        if validation.required:
                            column_rule_codes.append("missing_value")
                        if validation.unique:
                            column_rule_codes.append("duplicate_value")
                        if validation.allowed_values is not None:
                            column_rule_codes.append("allowed_values")
                        if validation.forbidden_values is not None:
                            column_rule_codes.append("forbidden_values")
                        for rule_code in column_rule_codes:
                            disabled = (
                                enabled_rules is not None
                                and rule_code not in enabled_rules
                            )
                            not_run.append(
                                ValidationEvent(
                                    rule_code=rule_code,
                                    status="NOT_RUN",
                                    reason=(
                                        "RULE_DISABLED"
                                        if disabled
                                        else "TABLE_HEADER_RESOLUTION_FAILED"
                                    ),
                                    severity=validation.severity
                                    or resolved_config.rule_severity.get(rule_code),
                                    description=(
                                        f"Column rule for '{validation.column}' "
                                        + (
                                            "is disabled by configuration"
                                            if disabled
                                            else "requires resolved table headers"
                                        )
                                    ),
                                )
                            )
                    continue
                for validation in table.column_validations:
                    column_number = logical_columns.get(validation.column)
                    if column_number is None:
                        continue
                    column_rule_codes = []
                    if validation.required:
                        column_rule_codes.append("missing_value")
                    if validation.unique:
                        column_rule_codes.append("duplicate_value")
                    if validation.allowed_values is not None:
                        column_rule_codes.append("allowed_values")
                    if validation.forbidden_values is not None:
                        column_rule_codes.append("forbidden_values")
                    disabled_column_rules = [
                        rule_code
                        for rule_code in column_rule_codes
                        if (
                            resolved_config.enabled_rules is not None
                            and rule_code not in resolved_config.enabled_rules
                        )
                    ]
                    if disabled_column_rules:
                        for rule_code in disabled_column_rules:
                            not_run.append(
                                ValidationEvent(
                                    rule_code=rule_code,
                                    status="NOT_RUN",
                                    reason="RULE_DISABLED",
                                    severity=validation.severity
                                    or resolved_config.rule_severity.get(rule_code),
                                    description=(
                                        f"Column rule for '{validation.column}' "
                                        "is disabled by configuration"
                                    ),
                                )
                            )
                        if len(disabled_column_rules) == len(column_rule_codes):
                            continue
                    missing_enabled = "missing_value" not in disabled_column_rules
                    duplicate_enabled = "duplicate_value" not in disabled_column_rules
                    allowed_enabled = "allowed_values" not in disabled_column_rules
                    forbidden_enabled = "forbidden_values" not in disabled_column_rules
                    seen_values = {}
                    for row_number in range(table.header_row + 1, max_row + 1):
                        cell = sheet.cell(row_number, column_number)
                        actual_value = cell.value
                        normalised_value = _normalise_comparison_value(
                            actual_value,
                            resolved_config.comparison,
                        )
                        is_null = normalised_value is None
                        if validation.required and missing_enabled and is_null:
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
                        if validation.unique and duplicate_enabled:
                            value_key = repr(normalised_value)
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
                            and allowed_enabled
                            and normalised_value
                            not in {
                                _normalise_comparison_value(
                                    value,
                                    resolved_config.comparison,
                                )
                                for value in validation.allowed_values
                            }
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
                            and forbidden_enabled
                            and normalised_value
                            in {
                                _normalise_comparison_value(
                                    value,
                                    resolved_config.comparison,
                                )
                                for value in validation.forbidden_values
                            }
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
                    for row_number, column_number in _comparison_coordinates(
                        candidate_sheet,
                        reference_sheet,
                    ):
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
            missing_cache_action = (
                resolved_config.runtime.missing_formula_cache_action
            )
            missing_cache_cells = []
            for formula_sheet in formula_workbook.worksheets:
                value_sheet = value_workbook[formula_sheet.title]
                for row in formula_sheet.iter_rows():
                    for formula_cell in row:
                        if (
                            formula_cell.data_type == "f"
                            and value_sheet[formula_cell.coordinate].value is None
                        ):
                            missing_cache_cells.append(
                                (formula_sheet.title, formula_cell.coordinate)
                            )
            if missing_cache_cells:
                status = (
                    "NOT_RUN"
                    if missing_cache_action == "not_run"
                    else "FAILED"
                )
                for sheet_name, cell_reference in missing_cache_cells:
                    event = ValidationEvent(
                        rule_code="formula_error",
                        status=status,
                        reason="FORMULA_RESULTS_UNAVAILABLE",
                        sheet_name=sheet_name,
                        cell_reference=cell_reference,
                        description=(
                            "Formula result is unavailable because the "
                            "workbook has no cached value"
                        ),
                    )
                    if status == "NOT_RUN":
                        not_run.append(event)
                    else:
                        errors.append(event)
                if status == "NOT_RUN":
                    continue
            for sheet in value_workbook.worksheets:
                for cell in _stored_cells(sheet):
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
            **result_metadata,
            errors=errors,
            warnings=warnings,
            not_run=not_run,
            diagnostics=diagnostics,
        )
    finally:
        formula_workbook.close()
        value_workbook.close()
        if reference_workbook is not None:
            reference_workbook.close()
        if reference_formula_workbook is not None:
            reference_formula_workbook.close()


def _match_sheets(sheet_names, selector):
    if selector.mode == "exact":
        if selector.case_sensitive:
            return [name for name in sheet_names if name == selector.value]
        expected = selector.value.casefold()
        return [name for name in sheet_names if name.casefold() == expected]
    if selector.mode == "regex":
        try:
            pattern = re.compile(
                selector.value,
                0 if selector.case_sensitive else re.IGNORECASE,
            )
        except re.error as error:
            raise ConfigurationError(
                f"invalid sheet selector regex: {error}"
            ) from error
        return [
            name
            for name in sheet_names
            if pattern.fullmatch(name) is not None
        ]
    return []


def _select_sheet_matches(matches, action, selector, errors):
    if action == "first":
        return matches[:1]
    if action == "error_on_multiple" and len(matches) > 1:
        errors.append(
            ValidationEvent(
                rule_code="sheet_selector",
                actual_value=len(matches),
                expected_value=1,
                description=(
                    f"Sheet selector matched multiple sheets: {selector.value}"
                ),
            )
        )
    return matches


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


def _values_equal(actual, expected, mode, options=None):
    options = options or {}
    if mode == "exact":
        return actual == expected
    if mode in {"normalised", "case_insensitive"}:
        if isinstance(actual, str) and isinstance(expected, str):
            actual_text = actual.strip()
            expected_text = expected.strip()
            if mode == "case_insensitive":
                return actual_text.casefold() == expected_text.casefold()
            return actual_text == expected_text
    if mode == "numeric":
        actual_number = _numeric_value(actual, options)
        expected_number = _numeric_value(expected, options)
        return (
            actual_number is not None
            and expected_number is not None
            and actual_number == expected_number
        )
    if mode == "date":
        actual_date = _date_value(actual, options)
        expected_date = _date_value(expected, options)
        return (
            actual_date is not None
            and expected_date is not None
            and actual_date == expected_date
        )
    return actual == expected


def _normalise_comparison_value(value, comparison):
    if value is None:
        return None
    if (
        comparison.empty_string_is_null
        and isinstance(value, str)
        and value == ""
    ):
        return None
    if value in comparison.null_tokens:
        return None
    if not isinstance(value, str):
        return value
    text = value
    if comparison.trim_whitespace:
        text = text.strip()
    if comparison.collapse_internal_whitespace:
        text = re.sub(r"\s+", " ", text)
    if not comparison.accent_sensitive:
        text = "".join(
            character
            for character in unicodedata.normalize("NFD", text)
            if not unicodedata.combining(character)
        )
    if not comparison.case_sensitive:
        text = text.casefold()
    return text


def _numeric_value(value, options):
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float, Decimal)):
        return Decimal(str(value))
    if not options.get("allow_numeric_strings") or not isinstance(value, str):
        return None
    text = value.strip().replace(
        options["thousands_separator"],
        "",
    )
    text = text.replace(options["decimal_separator"], ".")
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _date_value(value, options):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        return None
    for accepted_format in options["accepted_formats"]:
        try:
            return datetime.strptime(value.strip(), accepted_format).date()
        except ValueError:
            continue
    return None


def _detected_features(workbook, candidate_path):
    if candidate_path.suffix.lower() == ".xlsm":
        yield "macros", None
    if getattr(workbook, "_external_links", []):
        yield "external_links", None
    if getattr(workbook, "defined_names", {}):
        yield "named_ranges", None
    for sheet in workbook.worksheets:
        if getattr(sheet, "_charts", []):
            yield "charts", sheet.title
        if getattr(sheet, "_pivots", []):
            yield "pivot_tables", sheet.title
        if list(sheet.merged_cells.ranges):
            yield "merged_cells", sheet.title


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


def _cells_until_limit(workbook, limit):
    count = 0
    for sheet in workbook.worksheets:
        for _cell in _stored_cells(sheet):
            count += 1
            if count > limit:
                return count
    return None


def _stored_cells(sheet):
    cells = getattr(sheet, "_cells", None)
    if cells is not None:
        return cells.values()
    return (
        cell
        for row in sheet.iter_rows()
        for cell in row
    )


def _comparison_coordinates(candidate_sheet, reference_sheet):
    if (
        hasattr(candidate_sheet, "_cells")
        and hasattr(reference_sheet, "_cells")
    ):
        coordinates = {
            (cell.row, cell.column)
            for cell in _stored_cells(candidate_sheet)
        }
        coordinates.update(
            (cell.row, cell.column)
            for cell in _stored_cells(reference_sheet)
        )
        return coordinates
    max_row = max(candidate_sheet.max_row, reference_sheet.max_row)
    max_column = max(
        candidate_sheet.max_column,
        reference_sheet.max_column,
    )
    return (
        (row_number, column_number)
        for row_number in range(1, max_row + 1)
        for column_number in range(1, max_column + 1)
    )


def _table_data_end_row(
    table,
    header_row,
    value_sheet,
    formula_sheet,
    logical_columns,
):
    max_row = max(value_sheet.max_row, formula_sheet.max_row)
    boundary = table.data_boundary
    if boundary is None:
        return max_row
    if boundary.mode == "fixed_end_row" and boundary.end_row is not None:
        return min(max_row, boundary.end_row)
    if boundary.mode == "last_non_empty_row" and boundary.columns:
        column_numbers = [
            logical_columns[column]
            for column in boundary.columns
            if column in logical_columns
        ]
        if hasattr(value_sheet, "_cells") and hasattr(formula_sheet, "_cells"):
            rows = [
                cell.row
                for cell in _stored_cells(value_sheet)
                if cell.column in column_numbers
                and cell.row > header_row
                and cell.value is not None
            ]
            rows.extend(
                cell.row
                for cell in _stored_cells(formula_sheet)
                if cell.column in column_numbers
                and cell.row > header_row
                and cell.value is not None
            )
            return max(rows, default=header_row)
        for row_number in range(max_row, header_row, -1):
            if any(
                value_sheet.cell(row_number, column).value is not None
                or formula_sheet.cell(row_number, column).value is not None
                for column in column_numbers
            ):
                return row_number
        return header_row
    return max_row


def _content_shape(value_sheet, formula_sheet):
    max_row = 0
    max_column = 0
    if hasattr(value_sheet, "_cells") and hasattr(formula_sheet, "_cells"):
        coordinates = {
            cell.coordinate
            for cell in _stored_cells(value_sheet)
        }
        coordinates.update(
            cell.coordinate
            for cell in _stored_cells(formula_sheet)
        )
        for coordinate in coordinates:
            value_cell = value_sheet[coordinate]
            formula_cell = formula_sheet[coordinate]
            if value_cell.value is None and formula_cell.value is None:
                continue
            max_row = max(max_row, value_cell.row, formula_cell.row)
            max_column = max(
                max_column,
                value_cell.column,
                formula_cell.column,
            )
        return {"max_row": max_row, "max_column": max_column}
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
