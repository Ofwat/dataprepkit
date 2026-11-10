import pandas as pd
import pytest

from dataprepkit import pcd_flow
from dataprepkit.tests.pcd.fixtures import _standard_row

from dataprepkit.tests.pcd.staging.contract_fixtures import (
    _list_row as _contract_list_row,
    _scheme_row as _contract_scheme_row,
    _standard_row as _contract_standard_row,
)

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


@pytest.mark.parametrize(
    ("tab_name", "expected_category"),
    [
        ("DPB1", "Base"),
        ("DPB2", "Base"),
        ("DPB1_ADD", "Base"),
        ("DPB2_ADD", "Base"),
        ("DPWW1", "Wastewater"),
        ("DPWW2", "Wastewater"),
        ("DPWW3", "Wastewater"),
        ("DPWW4", "Wastewater"),
        ("DPWW5", "Wastewater"),
        ("DPW1", "Water"),
        ("DPW2", "Water"),
        ("DPW3", "Water"),
        ("DPW4", "Water"),
        ("DPW5", "Water"),
        ("unmapped", "N/A"),
        (None, "N/A"),
    ],
)
def test_scheme_category_case_matches_every_sp_mapping(tab_name, expected_category):
    assert pcd_flow._pcd_scheme_category(tab_name) == expected_category


@pytest.mark.parametrize(
    ("tab_name", "expected_category"),
    [
        ("DPB3", "N/A"),
        ("BRL_DPB3_ADD", "N/A"),
        ("DPWW3", "Wastewater"),
        ("DPW3", "Water"),
    ],
)
def test_scheme_category_on_tabs_accepted_by_sp(tab_name, expected_category):
    actual = pcd_flow.transform_pcd_scheme(
        pd.DataFrame(
            [
                _standard_row(
                    tabName=tab_name,
                    datapointTypeCode="PCD_TARGET",
                )
            ]
        ),
        period_codes=(),
    )

    assert actual.loc[0, "pcdCategory"] == expected_category


class TestPcdSchemeContract:
    def test_filter_accepts_the_four_sp_tabs_and_trimmed_pcd_prefix(self):
        tabs = ("DPB3", "BRL_DPB3_ADD", "DPWW3", "DPW3")
        source = pd.DataFrame([
            _contract_standard_row(
                tabName=tab,
                companyCode=f"C{index}",
                datapointTypeCode=" PCD_TARGET",
            )
            for index, tab in enumerate(tabs)
        ])

        actual = pcd_flow.transform_pcd_scheme(source, period_codes=())

        assert len(actual) == 4
        assert actual["tabName"].tolist() == list(tabs)

    @pytest.mark.parametrize(
        "tab_name",
        ["DPB1", "BRL_DPB1_ADD", "DPWW2", "DPW2", "DPB2", " DPW3", "unknown"],
    )
    def test_filter_rejects_tabs_outside_raw_sp_list(self, tab_name):
        actual = pcd_flow.transform_pcd_scheme(
            pd.DataFrame([_contract_standard_row(tabName=tab_name)]), period_codes=()
        )

        assert actual.empty

    @pytest.mark.parametrize("datapoint_type", ["XCD_TARGET", "APCD_TARGET", "PCX_TARGET"])
    def test_filter_rejects_non_pcd_trimmed_prefix(self, datapoint_type):
        actual = pcd_flow.transform_pcd_scheme(
            pd.DataFrame([_contract_standard_row(datapointTypeCode=datapoint_type)]),
            period_codes=(),
        )

        assert actual.empty

    @pytest.mark.parametrize(
        ("tab_name", "category"),
        [
            ("DPB3", "N/A"),
            ("BRL_DPB3_ADD", "N/A"),
            ("DPWW3", "Wastewater"),
            ("DPW3", "Water"),
        ],
    )
    def test_category_case_matches_every_sp_branch(self, tab_name, category):
        actual = pcd_flow.transform_pcd_scheme(
            pd.DataFrame([_contract_standard_row(tabName=tab_name)]), period_codes=()
        )

        assert actual.loc[0, "pcdCategory"] == category

    def test_maps_all_business_columns_for_a_lookup_match(self):
        actual = pcd_flow.transform_pcd_scheme(
            pd.DataFrame([_contract_standard_row(datapointValue="4")]),
            period_codes={"2025-26"},
        )

        assert actual.columns.tolist() == [
            "submissionNumber", "businessDomain", "partyCode", "periodCode",
            "deliveryPlanReference", "pcdOutputDescription", "pcdLineDescription",
            "pcdCategory", "tabName", "pcdReference", "pcdApplicableToCompany",
            "numberOfSchemes", "delayType", "unitOfmeasure", "decimalPlace",
            "recordStatus", "versionNumber", "submissionNote", "columnHeader",
            "bonCode", "totalDelayedSchemes", "delayedSchemes",
        ]
        assert actual.iloc[0].to_dict() == {
            "submissionNumber": "5", "businessDomain": "PCD", "partyCode": "HDD",
            "periodCode": "2025-26", "deliveryPlanReference": "DP-1",
            "pcdOutputDescription": "Output", "pcdLineDescription": "Line",
            "pcdCategory": "Water", "tabName": "DPW3", "pcdReference": "REF-1",
            "pcdApplicableToCompany": 1, "numberOfSchemes": 4,
            "delayType": "N/A", "unitOfmeasure": "Ml/d", "decimalPlace": 1,
            "recordStatus": "Active", "versionNumber": "3", "submissionNote": "Note",
            "columnHeader": "2025-26", "bonCode": "PCD_TARGET",
            "totalDelayedSchemes": 0, "delayedSchemes": 0,
        }

    def test_duplicate_period_lookup_matches_sql_join_multiplicity(self):
        actual = pcd_flow.transform_pcd_scheme(
            pd.DataFrame([
                _contract_standard_row(
                    datapointTypeCode="PCD_DLY_DIR",
                    datapointValue="2",
                )
            ]),
            period_codes=["2025-26", "2025-26"],
        )

        assert len(actual) == 1
        assert actual.loc[0, "numberOfSchemes"] == 2
        assert actual.loc[0, "totalDelayedSchemes"] == 4
        assert actual.loc[0, "delayedSchemes"] == 4

    def test_applicability_does_not_trim_leading_space(self):
        actual = pcd_flow.transform_pcd_scheme(
            pd.DataFrame([_contract_standard_row(pcdApplicableToCompany=" Y")]),
            period_codes=(),
        )

        assert actual.loc[0, "pcdApplicableToCompany"] == 0

    def test_trims_all_output_strings_without_trimming_raw_filter_tab(self):
        actual = pcd_flow.transform_pcd_scheme(
            pd.DataFrame([
                _contract_standard_row(
                    submissionNumber=" 5 ", companyCode=" HDD ",
                    datapointPeriod=" 2025-26 ", dp_reference=" DP-1 ",
                    datapointTypeCode=" PCD_DLY_DIR ",
                    pcdOutputDescription=" Output ", pcdLineDescription=" Line ",
                    tabName="DPW3 ", pcdRef=" REF-1 ",
                    unitOfmeasure=" Ml/d ", versionInfo=" 3 ",
                    submissionNotes=" Note ",
                )
            ]),
            period_codes=(),
        ).iloc[0]

        assert actual["submissionNumber"] == "5"
        assert actual["partyCode"] == "HDD"
        assert actual["periodCode"] == "2025-26"
        assert actual["deliveryPlanReference"] == "DP-1"
        assert actual["pcdOutputDescription"] == "Output"
        assert actual["pcdLineDescription"] == "Line"
        assert actual["tabName"] == "DPW3"
        assert actual["pcdReference"] == "REF-1"
        assert actual["unitOfmeasure"] == "Ml/d"
        assert actual["versionNumber"] == "3"
        assert actual["submissionNote"] == "Note"
        assert actual["bonCode"] == "PCD_DLY_DIR"
        assert actual["delayType"] == "Lack of internal resourcing"

    def test_null_defaults_left_limits_and_try_cast_output_match_sp(self):
        actual = pcd_flow.transform_pcd_scheme(
            pd.DataFrame([_contract_standard_row(
                submissionNumber=None, companyCode="C" * 12,
                datapointPeriod=None, dp_reference="D" * 40,
                datapointTypeCode="PCD" + "T" * 300,
                datapointValue="invalid", pcdOutputDescription="O" * 2100,
                pcdLineDescription="L" * 2100, pcdRef="R" * 300,
                pcdApplicableToCompany=None, unitOfmeasure="U" * 300,
                decimalPlace="bad", versionInfo="V" * 80,
                submissionNotes="N" * 2100,
            )]),
            period_codes=(),
        ).iloc[0]

        assert actual["submissionNumber"] == "N/A"
        assert actual["partyCode"] == "C" * 8
        assert actual["periodCode"] == "N/A"
        assert actual["deliveryPlanReference"] == "D" * 32
        assert actual["pcdOutputDescription"] == "O" * 2048
        assert actual["pcdLineDescription"] == "L" * 2048
        assert actual["pcdReference"] == "R" * 256
        assert actual["pcdApplicableToCompany"] == 0
        assert actual["numberOfSchemes"] == 0
        assert actual["unitOfmeasure"] == "U" * 256
        assert pd.isna(actual["decimalPlace"])
        assert actual["versionNumber"] == "V" * 64
        assert actual["submissionNote"] == "N" * 2048
        assert actual["columnHeader"] == "N/A"
        assert actual["bonCode"] == "PCD" + "T" * 253
        assert actual["totalDelayedSchemes"] == 0
        assert actual["delayedSchemes"] == 0
