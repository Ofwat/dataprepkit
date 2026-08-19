from time import perf_counter

import openpyxl

from dataprepkit.validation import RuntimePolicy, validate_excel

from test_public_api import make_config


def test_large_workbook_validation_timing(tmp_path):
    path = tmp_path / "large.xlsx"
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "Data"
    for row_number in range(1, 5001):
        sheet.cell(row_number, 1).value = row_number
        sheet.cell(row_number, 2).value = f"value-{row_number}"
    workbook.save(path)

    started = perf_counter()
    result = validate_excel(
        candidate_path=path,
        config=make_config().model_copy(
            update={
                "runtime": RuntimePolicy(
                    max_cells_scanned=20_000,
                    read_only=False,
                    macro_policy="reject",
                    missing_formula_cache_action="not_run",
                )
            }
        ),
    )
    elapsed = perf_counter() - started

    assert result.is_valid is True
    assert elapsed < 10
