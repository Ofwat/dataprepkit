import pandas as pd
import pytest

from dataprepkit import pcd_flow
from datetime import date

from dataprepkit.tests.pcd.staging.contract_fixtures import (
    _list_row as _contract_list_row,
    _scheme_row as _contract_scheme_row,
    _standard_row as _contract_standard_row,
)

class TestDeliverableSchemeMilestoneContract:
    def test_unpivots_each_of_the_fourteen_date_columns_in_sql_order(self):
        row = _contract_list_row()
        expected_periods = [
            *(f"baselineIm{i}" for i in range(1, 8)),
            *(f"forecastIm{i}" for i in range(1, 8)),
        ]
        for index, period in enumerate(expected_periods, start=1):
            row[period] = f"2025-01-{index:02d}"

        actual = pcd_flow.transform_deliverable_scheme_milestone(
            pd.DataFrame([row])
        )

        assert actual.columns.tolist() == [
            "submissionNumber", "businessDomain", "partyCode", "recordStatus",
            "schemeId", "milestoneDate", "periodCode", "milestoneDetailType",
        ]
        assert actual["periodCode"].tolist() == expected_periods
        assert actual["milestoneDate"].tolist() == [
            date(2025, 1, day) for day in range(1, 15)
        ]
        assert actual["milestoneDetailType"].tolist() == [
            "Baseline" if period.startswith("baseline") else "Forecast"
            for period in expected_periods
        ]
        assert set(actual["businessDomain"]) == {"PCD"}
        assert set(actual["recordStatus"]) == {"Active"}
        assert actual.iloc[0].to_dict() == {
            "submissionNumber": "5", "businessDomain": "PCD", "partyCode": "HDD",
            "recordStatus": "Active", "schemeId": "S-1",
            "milestoneDate": date(2025, 1, 1), "periodCode": "baselineIm1",
            "milestoneDetailType": "Baseline",
        }

    def test_null_invalid_and_duplicate_source_dates_follow_cross_join_rules(self):
        row = _contract_list_row(
            submissionNumber=None,
            companyCode="C" * 12,
            schemeId="S" * 1100,
            baselineIm1="not-a-date",
        )
        actual = pcd_flow.transform_deliverable_scheme_milestone(
            pd.DataFrame([row, row.copy()])
        )

        assert len(actual) == 28
        first = actual.iloc[0]
        assert first["submissionNumber"] is None
        assert first["partyCode"] == "C" * 8
        assert first["schemeId"] == "S" * 1024
        assert pd.isna(first["milestoneDate"])

    def test_requires_every_date_field_read_by_procedure(self):
        row = pd.DataFrame([_contract_list_row()]).drop(columns="forecastIm6")

        with pytest.raises(ValueError, match="forecastIm6"):
            pcd_flow.transform_deliverable_scheme_milestone(row)
