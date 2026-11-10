import pandas as pd
import pytest

from dataprepkit import pcd_flow

from dataprepkit.tests.pcd.staging.contract_fixtures import (
    _list_row as _contract_list_row,
    _scheme_row as _contract_scheme_row,
    _standard_row as _contract_standard_row,
)

class TestDeliverableSchemeExpenditureContract:
    def test_unpivots_all_fourteen_values_and_maps_period_and_type(self):
        row = _contract_list_row()
        columns = [
            f"{kind}{year}_{(year + 1) % 100:02d}"
            for kind in ("baseline", "forecast")
            for year in range(2023, 2030)
        ]
        for index, column in enumerate(columns, start=1):
            row[column] = str(index + 0.5)

        actual = pcd_flow.transform_deliverable_scheme_expenditure(
            pd.DataFrame([row])
        )

        assert actual.columns.tolist() == [
            "submissionNumber", "businessDomain", "partyCode", "recordStatus",
            "schemeId", "milestoneExpenditure", "periodCode", "milestoneDetailType",
        ]
        assert actual["periodCode"].tolist() == [
            "2023-24", "2024-25", "2025-26", "2026-27", "2027-28",
            "2028-29", "2029-30",
        ] * 2
        assert actual["milestoneExpenditure"].tolist() == [
            index + 0.5 for index in range(1, 15)
        ]
        assert actual["milestoneDetailType"].tolist() == [
            *("Baseline" for _ in range(7)), *("Forecast" for _ in range(7))
        ]
        forecast_2027_28 = actual.loc[
            (actual["periodCode"] == "2027-28")
            & (actual["milestoneDetailType"] == "Forecast")
        ].iloc[0]
        assert forecast_2027_28["milestoneExpenditure"] == 12.5
        assert actual.iloc[0].to_dict() == {
            "submissionNumber": "5", "businessDomain": "PCD", "partyCode": "HDD",
            "recordStatus": "Active", "schemeId": "S-1",
            "milestoneExpenditure": 1.5, "periodCode": "2023-24",
            "milestoneDetailType": "Baseline",
        }

    def test_invalid_values_become_zero_and_duplicate_rows_are_preserved(self):
        row = _contract_list_row(baseline2023_24="not-numeric")
        actual = pcd_flow.transform_deliverable_scheme_expenditure(
            pd.DataFrame([row, row.copy()])
        )

        assert len(actual) == 28
        baseline_period_rows = actual.loc[
            (actual["periodCode"] == "2023-24")
            & (actual["milestoneDetailType"] == "Baseline"),
            "milestoneExpenditure",
        ]
        assert baseline_period_rows.tolist() == [0.0, 0.0]
        assert actual.loc[0, "partyCode"] == "HDD"
        assert actual.loc[0, "schemeId"] == "S-1"

    def test_null_defaults_and_left_limits_match_insert_projection(self):
        row = _contract_list_row(submissionNumber=None, companyCode=None, schemeId=None)
        actual = pcd_flow.transform_deliverable_scheme_expenditure(
            pd.DataFrame([row])
        )

        assert actual.loc[0, "submissionNumber"] is None
        assert actual.loc[0, "partyCode"] == "N/A"
        assert actual.loc[0, "schemeId"] == "N/A"
        assert actual.loc[0, "milestoneExpenditure"] == 0

    def test_left_limits_apply_to_party_code_and_scheme_id(self):
        row = _contract_list_row(companyCode="C" * 12, schemeId="S" * 1100)
        actual = pcd_flow.transform_deliverable_scheme_expenditure(
            pd.DataFrame([row])
        )

        assert actual.loc[0, "partyCode"] == "C" * 8
        assert actual.loc[0, "schemeId"] == "S" * 1024

    def test_requires_every_expenditure_period_column(self):
        row = pd.DataFrame([_contract_list_row()]).drop(columns="forecast2028_29")

        with pytest.raises(ValueError, match="forecast2028_29"):
            pcd_flow.transform_deliverable_scheme_expenditure(row)
