import pandas as pd
import pytest

from dataprepkit import pcd_flow
from dataprepkit.tests.pcd.fixtures import _list_row

def test_scheme_detail_milestone_and_expenditure_expansion_match_sql_periods():
    source = pd.DataFrame(
        [
            _list_row(
                baselineIm1="2024-05-06",
                forecastIm7="invalid",
                baseline2023_24="12.5",
                forecast2029_30="not-number",
            )
        ]
    )

    milestones = pcd_flow.transform_deliverable_scheme_milestone(source)
    expenditure = pcd_flow.transform_deliverable_scheme_expenditure(source)

    assert len(milestones) == len(expenditure) == 14
    assert milestones.loc[milestones["periodCode"] == "baselineIm1", "milestoneDate"].iloc[0].isoformat() == "2024-05-06"
    assert pd.isna(milestones.loc[milestones["periodCode"] == "forecastIm7", "milestoneDate"].iloc[0])
    assert expenditure.loc[expenditure["periodCode"] == "2023-24", "milestoneExpenditure"].iloc[0] == 12.5
    assert expenditure.loc[expenditure["periodCode"] == "2029-30", "milestoneExpenditure"].iloc[0] == 0
