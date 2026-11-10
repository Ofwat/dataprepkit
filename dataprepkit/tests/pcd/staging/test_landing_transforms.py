import pandas as pd
import pytest

from dataprepkit import pcd_flow
from dataprepkit.tests.pcd.fixtures import (
    _list_row,
    _standard_landing,
    _standard_row,
)

def test_external_submission_deduplicates_same_metadata_row():
    source = pd.DataFrame([_standard_row(), _standard_row()])

    actual = pcd_flow.transform_pcd_external_company_submission(source)

    assert len(actual) == 1
    assert actual.loc[0, "businessDomain"] == "PCD"
    assert actual.loc[0, "partyCode"] == "HDD"
    assert actual.loc[0, "createdDate"].date().isoformat() == "2026-10-21"


def test_expenditure_filters_dp_b2_and_groups_measures_by_all_other_fields():
    source = pd.DataFrame(
        [
            _standard_row(datapointValue="2"),
            _standard_row(datapointValue="3"),
            _standard_row(tabName="DPW2", datapointTypeCode="PCD_OUTTURN_O", datapointValue="7"),
            _standard_row(tabName="DPB1"),
        ]
    )

    actual = pcd_flow.transform_pcd_expenditure(source)

    assert len(actual) == 2
    allowance = actual.loc[actual["bonCode"] == "PCD_TARGET"].iloc[0]
    outturn = actual.loc[actual["bonCode"] == "PCD_OUTTURN_O"].iloc[0]
    assert allowance["cumulativeExpenditureAllowance"] == 5
    assert allowance["pcdCategory"] == "Base"
    assert outturn["cumulativeExpenditureOutturnForecast"] == 7
    assert outturn["pcdCategory"] == "Water"


def test_expenditure_brl_dpb2_add_category_matches_sp_default():
    actual = pcd_flow.transform_pcd_expenditure(
        pd.DataFrame(
            [
                _standard_row(
                    tabName="BRL_DPB2_ADD",
                    datapointTypeCode="PCD_TARGET",
                )
            ]
        )
    )

    assert actual.loc[0, "pcdCategory"] == "N/A"


def test_expenditure_filter_accepts_exactly_the_four_sp_tabs():
    tabs = ("DPB2", "BRL_DPB2_ADD", "DPWW2", "DPW2")
    source = pd.DataFrame(
        [
            _standard_row(
                tabName=tab,
                companyCode=f"C{index}",
                datapointTypeCode=f"PCD_TARGET_{index}",
            )
            for index, tab in enumerate(tabs)
        ]
    )

    actual = pcd_flow.transform_pcd_expenditure(source)

    assert len(actual) == len(tabs)
    assert actual["tabName"].tolist() == list(tabs)


@pytest.mark.parametrize(
    "tab_name",
    ["DPB1", "BRL_DPB1_ADD", "DPWW1", "DPW1", "DPB3", "DPWW3", "DPW3", "unknown"],
)
def test_expenditure_filter_rejects_tabs_not_listed_by_sp(tab_name):
    actual = pcd_flow.transform_pcd_expenditure(
        pd.DataFrame([_standard_row(tabName=tab_name)])
    )

    assert actual.empty


@pytest.mark.parametrize("datapoint_type", ["XCD_TARGET", "APCD_TARGET", " PCD_TARGET"])
def test_expenditure_filter_requires_pcd_in_first_three_untrimmed_characters(
    datapoint_type,
):
    actual = pcd_flow.transform_pcd_expenditure(
        pd.DataFrame([_standard_row(datapointTypeCode=datapoint_type)])
    )

    assert actual.empty


def test_expenditure_filter_does_not_trim_leading_tab_whitespace():
    actual = pcd_flow.transform_pcd_expenditure(
        pd.DataFrame([_standard_row(tabName=" DPB2")])
    )

    assert actual.empty


def test_expenditure_filter_allows_sql_padded_trailing_tab_spaces():
    actual = pcd_flow.transform_pcd_expenditure(
        pd.DataFrame([_standard_row(tabName="DPB2   ")])
    )

    assert len(actual) == 1
    assert actual.loc[0, "tabName"] == "DPB2"


def test_expenditure_trims_each_trimmed_output_field():
    source = _standard_row(
        submissionNumber=" 5 ",
        companyCode=" HDD ",
        datapointPeriod=" 2025-26 ",
        dp_reference=" DP-1 ",
        datapointTypeCode="PCD_TARGET ",
        pcdOutputDescription=" Output ",
        pcdLineDescription=" Line ",
        tabName="DPB2 ",
        pcdRef=" REF-1 ",
        pcdApplicableToCompany="Y ",
        unitOfmeasure=" Ml/d ",
        decimalPlace=" 1 ",
        versionInfo=" 3 ",
        submissionNotes=" Note ",
    )

    actual = pcd_flow.transform_pcd_expenditure(pd.DataFrame([source])).iloc[0]

    assert actual["submissionNumber"] == "5"
    assert actual["partyCode"] == "HDD"
    assert actual["periodCode"] == "2025-26"
    assert actual["columnHeader"] == "2025-26"
    assert actual["deliveryPlanReference"] == "DP-1"
    assert actual["bonCode"] == "PCD_TARGET"
    assert actual["pcdOutputDescription"] == "Output"
    assert actual["pcdLineDescription"] == "Line"
    assert actual["tabName"] == "DPB2"
    assert actual["pcdReference"] == "REF-1"
    assert actual["pcdApplicableToCompany"] == 1
    assert actual["unitOfmeasure"] == "Ml/d"
    assert actual["versionNumber"] == "3"
    assert actual["submissionNote"] == "Note"


@pytest.mark.parametrize(
    ("tab_name", "expected_category"),
    [
        ("DPB2", "Base"),
        ("DPWW2", "Wastewater"),
        ("DPW2", "Water"),
        ("BRL_DPB2_ADD", "N/A"),
    ],
)
def test_expenditure_category_case_matches_each_sp_branch(tab_name, expected_category):
    actual = pcd_flow.transform_pcd_expenditure(
        pd.DataFrame([_standard_row(tabName=tab_name)])
    )

    assert actual.loc[0, "pcdCategory"] == expected_category


def test_expenditure_maps_every_business_column_for_a_regular_row():
    actual = pcd_flow.transform_pcd_expenditure(
        pd.DataFrame([_standard_row()])
    )

    assert actual.columns.tolist() == [
        "submissionNumber", "businessDomain", "partyCode", "periodCode",
        "deliveryPlanReference", "pcdOutputDescription", "pcdLineDescription",
        "pcdCategory", "tabName", "pcdReference", "pcdApplicableToCompany",
        "unitOfmeasure", "decimalPlace", "recordStatus", "versionNumber",
        "submissionNote", "columnHeader", "bonCode", "nonDeliveryPayment",
        "cumulativeExpenditureAllowance", "cumulativeExpenditureOutturnForecast",
    ]
    assert actual.iloc[0].to_dict() == {
        "submissionNumber": "5",
        "businessDomain": "PCD",
        "partyCode": "HDD",
        "periodCode": "2025-26",
        "deliveryPlanReference": "DP-1",
        "pcdOutputDescription": "Output",
        "pcdLineDescription": "Line",
        "pcdCategory": "Base",
        "tabName": "DPB2",
        "pcdReference": "REF-1",
        "pcdApplicableToCompany": 1,
        "unitOfmeasure": "Ml/d",
        "decimalPlace": 1,
        "recordStatus": "Active",
        "versionNumber": "3",
        "submissionNote": "Note",
        "columnHeader": "2025-26",
        "bonCode": "PCD_TARGET",
        "nonDeliveryPayment": 0.0,
        "cumulativeExpenditureAllowance": 12.5,
        "cumulativeExpenditureOutturnForecast": 0.0,
    }


def test_expenditure_sql_null_defaults_and_declared_output_lengths():
    actual = pcd_flow.transform_pcd_expenditure(
        pd.DataFrame(
            [
                _standard_row(
                    submissionNumber=None,
                    companyCode="C" * 12,
                    datapointPeriod=None,
                    dp_reference="D" * 40,
                    datapointTypeCode="PCD" + "T" * 300,
                    datapointValue="not-numeric",
                    pcdOutputDescription="O" * 2100,
                    pcdLineDescription="L" * 2100,
                    pcdRef="R" * 300,
                    pcdApplicableToCompany=None,
                    unitOfmeasure="U" * 300,
                    decimalPlace=None,
                    versionInfo="V" * 80,
                    submissionNotes="N" * 2100,
                )
            ]
        )
    ).iloc[0]

    assert actual["submissionNumber"] == "N/A"
    assert actual["partyCode"] == "C" * 8
    assert actual["periodCode"] == "N/A"
    assert actual["deliveryPlanReference"] == "D" * 32
    assert actual["pcdOutputDescription"] == "O" * 2048
    assert actual["pcdLineDescription"] == "L" * 2048
    assert actual["pcdReference"] == "R" * 256
    assert actual["tabName"] == "DPB2"
    assert actual["pcdApplicableToCompany"] == 0
    assert actual["unitOfmeasure"] == "U" * 256
    assert actual["decimalPlace"] == 0
    assert actual["versionNumber"] == "V" * 64
    assert actual["submissionNote"] == "N" * 2048
    assert actual["columnHeader"] == "N/A"
    assert actual["bonCode"] == "PCD" + "T" * 253
    assert actual["nonDeliveryPayment"] == 0
    assert actual["cumulativeExpenditureAllowance"] == 0
    assert actual["cumulativeExpenditureOutturnForecast"] == 0


@pytest.mark.parametrize(
    ("applicable", "expected"),
    [("Y", 1), ("1", 1), ("N", 0), ("0", 0), ("other", 0), (" Y", 0)],
)
def test_expenditure_company_applicability_matches_sp_case(applicable, expected):
    actual = pcd_flow.transform_pcd_expenditure(
        pd.DataFrame([_standard_row(pcdApplicableToCompany=applicable)])
    )

    assert actual.loc[0, "pcdApplicableToCompany"] == expected


@pytest.mark.parametrize(
    ("datapoint_type", "expected_np", "expected_allowance", "expected_outturn"),
    [
        ("PCD_TARGET", 0, 7, 0),
        ("PCD_TARGET_NP", 7, 0, 0),
        ("PCD_TARGET_O", 0, 0, 7),
        ("PCD_TARGET_OT", 0, 0, 7),
        ("PCD_TARGET_NP_EXTRA", 0, 7, 0),
    ],
)
def test_expenditure_measure_routing_matches_sp_cases(
    datapoint_type, expected_np, expected_allowance, expected_outturn
):
    actual = pcd_flow.transform_pcd_expenditure(
        pd.DataFrame(
            [_standard_row(datapointTypeCode=datapoint_type, datapointValue="7")]
        )
    ).iloc[0]

    assert actual["nonDeliveryPayment"] == expected_np
    assert actual["cumulativeExpenditureAllowance"] == expected_allowance
    assert actual["cumulativeExpenditureOutturnForecast"] == expected_outturn


def test_expenditure_suffix_match_uses_untrimmed_datapoint_type():
    actual = pcd_flow.transform_pcd_expenditure(
        pd.DataFrame(
            [_standard_row(datapointTypeCode="PCD_TARGET_O ", datapointValue="7")]
        )
    ).iloc[0]

    assert actual["bonCode"] == "PCD_TARGET_O"
    assert actual["cumulativeExpenditureAllowance"] == 7
    assert actual["cumulativeExpenditureOutturnForecast"] == 0


@pytest.mark.parametrize(
    ("suffix", "expected_label"),
    [
        ("DIR", "Lack of internal resourcing"),
        ("DSCR", "Lack of supply chain resourcing"),
        ("DCS", "Change in solution (IM2)"),
        ("DSCS", "Supply chain capacity"),
        ("DPP", "Land and planning permission"),
        ("DTPI", "Third-party interface"),
        ("DCI", "Contractual issues"),
        ("DSI", "Sites issues"),
        ("DER", "Environmental risks"),
        ("RS", "Regulatory Sign off Delay"),
        ("DPLE", "Programme level efficiency"),
    ],
)
def test_scheme_delay_code_labels_match_sp(suffix, expected_label):
    actual = pcd_flow.transform_pcd_scheme(
        pd.DataFrame(
            [
                _standard_row(
                    tabName="DPW3",
                    datapointTypeCode=f"PCD_{suffix}",
                    datapointValue="3",
                )
            ]
        ),
        period_codes=(),
    )

    assert actual.loc[0, "delayedSchemes"] == 3
    assert actual.loc[0, "delayType"] == expected_label


def test_expenditure_invalid_decimal_place_is_null_like_try_cast():
    actual = pcd_flow.transform_pcd_expenditure(
        pd.DataFrame([_standard_row(decimalPlace="not-an-int")])
    )

    assert pd.isna(actual.loc[0, "decimalPlace"])


def test_expenditure_invalid_datapoint_value_contributes_zero():
    actual = pcd_flow.transform_pcd_expenditure(
        pd.DataFrame(
            [
                _standard_row(
                    datapointTypeCode="PCD_TARGET_NP",
                    datapointValue="not-numeric",
                )
            ]
        )
    ).iloc[0]

    assert actual["nonDeliveryPayment"] == 0
    assert actual["cumulativeExpenditureAllowance"] == 0
    assert actual["cumulativeExpenditureOutturnForecast"] == 0


def test_scheme_sums_delay_measures_after_grouping_by_business_fields():
    source = pd.DataFrame(
        [
            _standard_row(tabName="DPW3", datapointTypeCode="PCD_DLY_DIR", datapointValue="2"),
            _standard_row(tabName="DPW3", datapointTypeCode="PCD_DLY_DIR", datapointValue="4"),
        ]
    )

    actual = pcd_flow.transform_pcd_scheme(source, period_codes=())

    assert len(actual) == 1
    assert actual.loc[0, "numberOfSchemes"] == 0
    assert actual.loc[0, "totalDelayedSchemes"] == 6
    assert actual.loc[0, "delayedSchemes"] == 6


def test_scheme_uses_period_lookup_and_maps_delay_logic():
    source = pd.DataFrame(
        [
            _standard_row(tabName="DPW3", datapointTypeCode="PCD_DLY_DIR", datapointValue="4"),
            _standard_row(tabName="DPW3", datapointTypeCode="PCD_DLY_DIR", datapointValue="1"),
            _standard_row(tabName="DPW3", datapointTypeCode="PCD_DLY_DIR", datapointPeriod="unknown", datapointValue="8"),
        ]
    )

    actual = pcd_flow.transform_pcd_scheme(source, period_codes={"2025-26"})

    assert len(actual) == 3
    included = actual.loc[actual["numberOfSchemes"] == 4].iloc[0]
    included_second_value = actual.loc[actual["numberOfSchemes"] == 1].iloc[0]
    excluded = actual.loc[actual["periodCode"] == "unknown"].iloc[0]
    assert included["totalDelayedSchemes"] == 4
    assert included_second_value["totalDelayedSchemes"] == 1
    assert included["delayedSchemes"] == 4
    assert included["delayType"] == "Lack of internal resourcing"
    assert excluded["numberOfSchemes"] == 0


def test_scheme_brl_dpb3_add_category_matches_sp_default():
    actual = pcd_flow.transform_pcd_scheme(
        pd.DataFrame(
            [
                _standard_row(
                    tabName="BRL_DPB3_ADD",
                    datapointTypeCode="PCD_TARGET",
                )
            ]
        ),
        period_codes=(),
    )

    assert actual.loc[0, "pcdCategory"] == "N/A"


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
