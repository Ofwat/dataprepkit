from pathlib import Path

import openpyxl
import pytest


from dataprepkit.validation import (
    ComparisonConfig,
    ConfigurationError,
    RuntimePolicy,
    SheetPolicy,
    SheetSelector,
    WorkbookValidationConfig,
    WorkbookCheck,
    validate_config,
    validate_excel,
)


def make_config(required_sheet="Data", workbook_checks=None):
    return WorkbookValidationConfig(
        config_version="1",
        comparison=ComparisonConfig(
            case_sensitive=False,
            accent_sensitive=True,
            trim_whitespace=True,
            collapse_internal_whitespace=False,
            empty_string_is_null=True,
            null_tokens=[],
            collation_name="test-collation",
        ),
        sheet_policy=SheetPolicy(
            required_selectors=[
                SheetSelector(mode="exact", value=required_sheet)
            ],
            ignored_selectors=[],
            extra_sheet_action="ignore",
            selector_match_action="all",
        ),
        tables=[],
        expected_cells=[],
        workbook_checks=workbook_checks or [],
        enabled_rules=None,
        rule_severity={},
        runtime=RuntimePolicy(
            max_cells_scanned=1000,
            read_only=False,
            macro_policy="reject",
            missing_formula_cache_action="not_run",
        ),
        compatibility=None,
    )


def write_workbook(path: Path, sheet_name: str):
    workbook = openpyxl.Workbook()
    workbook.active.title = sheet_name
    workbook.save(path)


def write_workbook_with_sheets(path: Path, sheet_names):
    workbook = openpyxl.Workbook()
    workbook.active.title = sheet_names[0]
    for sheet_name in sheet_names[1:]:
        workbook.create_sheet(sheet_name)
    workbook.save(path)


def test_public_validation_exports_are_available():
    assert WorkbookValidationConfig
    assert validate_config
    assert validate_excel


def test_validate_config_returns_a_validated_public_model():
    config = make_config()

    resolved = validate_config(config)

    assert resolved is config
    assert resolved.config_version == "1"


def test_validate_config_reports_a_public_field_path():
    with pytest.raises(ConfigurationError) as error:
        validate_config(
            {
                **make_config().model_dump(),
                "tables": [
                    {
                        "name": "outputs",
                        "column_validations": [
                            {
                                "column": "status",
                                "allowed_values": ["active"],
                                "forbidden_values": ["deleted"],
                            }
                        ],
                    }
                ],
            }
        )

    assert "tables[0].column_validations[0]" in str(error.value)


def test_validate_excel_standalone_reports_missing_required_sheet(tmp_path):
    candidate_path = tmp_path / "candidate.xlsx"
    write_workbook(candidate_path, "Other")

    result = validate_excel(
        candidate_path=candidate_path,
        config=make_config(required_sheet="Data"),
    )

    assert result.is_valid is False
    assert [event.rule_code for event in result.errors] == ["required_sheet"]
    assert result.errors[0].status == "FAILED"


def test_validate_excel_standalone_accepts_required_sheet(tmp_path):
    candidate_path = tmp_path / "candidate.xlsx"
    write_workbook(candidate_path, "Data")

    result = validate_excel(
        candidate_path=candidate_path,
        config=make_config(required_sheet="Data"),
    )

    assert result.is_valid is True
    assert result.errors == []


def test_validate_excel_reports_configured_formula_errors(tmp_path):
    candidate_path = tmp_path / "candidate.xlsx"
    workbook = openpyxl.Workbook()
    workbook.active.title = "Data"
    workbook.active["A1"] = "#DIV/0!"
    workbook.save(candidate_path)

    config = make_config(
        workbook_checks=[
            WorkbookCheck(
                rule_code="formula_error",
                enabled=True,
                scope="all_sheets",
                options={"error_tokens": ["#DIV/0!"]},
            )
        ]
    )

    result = validate_excel(candidate_path=candidate_path, config=config)

    assert result.is_valid is False
    assert len(result.errors) == 1
    assert result.errors[0].rule_code == "formula_error"
    assert result.errors[0].sheet_name == "Data"
    assert result.errors[0].cell_reference == "A1"


def test_validate_excel_reports_missing_reference_sheet(tmp_path):
    candidate_path = tmp_path / "candidate.xlsx"
    reference_path = tmp_path / "reference.xlsx"
    write_workbook_with_sheets(candidate_path, ["Data"])
    write_workbook_with_sheets(reference_path, ["Data", "Required_From_Template"])

    config = make_config(
        workbook_checks=[
            WorkbookCheck(
                rule_code="missing_reference_sheet",
                enabled=True,
                scope="all_sheets",
            )
        ]
    )

    result = validate_excel(
        candidate_path=candidate_path,
        reference_path=reference_path,
        config=config,
    )

    assert result.is_valid is False
    assert [event.rule_code for event in result.errors] == [
        "missing_reference_sheet"
    ]
    assert result.errors[0].sheet_name == "Required_From_Template"


def test_reference_rule_is_not_run_without_reference_workbook(tmp_path):
    candidate_path = tmp_path / "candidate.xlsx"
    write_workbook(candidate_path, "Data")
    config = make_config(
        workbook_checks=[
            WorkbookCheck(
                rule_code="missing_reference_sheet",
                enabled=True,
                scope="all_sheets",
            )
        ]
    )

    result = validate_excel(candidate_path=candidate_path, config=config)

    assert result.is_valid is True
    assert len(result.not_run) == 1
    assert result.not_run[0].reason == "REFERENCE_WORKBOOK_REQUIRED"
