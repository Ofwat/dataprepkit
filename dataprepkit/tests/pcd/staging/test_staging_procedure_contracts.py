"""Executable business-column contracts for PCD staging procedures."""

from datetime import date, datetime

import pandas as pd
import pytest

from dataprepkit import pcd_flow


def _standard_row(**changes):
    row = {
        "submissionNumber": "5",
        "companyCode": "HDD",
        "dataCollection": "PCD",
        "worksheetName": "Submission",
        "datapointPeriod": "2025-26",
        "dp_reference": "DP-1",
        "datapointTypeCode": "PCD_TARGET",
        "datapointValue": "12.5",
        "DateTimeCreated": "01-02-2025 10:11:12",
        "pcdModel": "unused",
        "pcdRef": "REF-1",
        "pcdApplicableToCompany": "Y",
        "pcdLineDescription": "Line",
        "pcdOutputDescription": "Output",
        "unitOfmeasure": "Ml/d",
        "decimalPlace": "1",
        "versionInfo": "3",
        "submissionNotes": "Note",
        "tabName": "DPW3",
    }
    row.update(changes)
    return row


def _scheme_row(**changes):
    row = {
        "submissionNumber": "5",
        "companyCode": "HDD",
        "tabName": "DPW1",
        "lineDescription": "Line",
        "versionInfo": "3",
        "schemeId": "S-1",
        "typeOfScheme": "Primary",
        "imMoveReason": "Reason",
        "substitutedSchemeName": "Substitute",
        "pcdRef": "REF-1",
        "eaId": "EA-1",
        "winepId": "WINEP-1",
        "schemeName": "Scheme",
        "forecastToComplete": "10.5",
        "originalRegulatoryDate": "2024-01-02",
        "currentRegulatoryDate": "2025-02-03",
        "currentInterimMilestone": "IM2",
        "spendToDate": "5.5",
        "forecastOutturn": "20.5",
        "percentSpendToDate": "25.5",
        "criticalHighProfile": "YES",
        "schemeType": "Secondary",
        "amp": "AMP8",
        "submissionNotes": "Note",
    }
    row.update(changes)
    return row


def _list_row(**changes):
    row = _scheme_row()
    row.update({
        **{
            f"{kind}Im{index}": None
            for kind in ("baseline", "forecast")
            for index in range(1, 8)
        },
        **{
            f"{kind}{year}_{(year + 1) % 100:02d}": None
            for kind in ("baseline", "forecast")
            for year in range(2023, 2030)
        },
        **changes,
    })
    return row


class TestExternalCompanySubmissionContract:
    def test_maps_all_business_columns_and_style_105_timestamp(self):
        actual = pcd_flow.transform_pcd_external_company_submission(
            pd.DataFrame([_standard_row()])
        )

        assert actual.columns.tolist() == [
            "submissionNumber", "businessDomain", "partyCode", "pcdModel",
            "worksheetName", "createdDate", "recordStatus", "versionNumber",
            "submissionNote",
        ]
        assert actual.iloc[0].to_dict() == {
            "submissionNumber": "5",
            "businessDomain": "PCD",
            "partyCode": "HDD",
            "pcdModel": "PCD",
            "worksheetName": "Submission",
            "createdDate": datetime(2025, 2, 1, 10, 11, 12),
            "recordStatus": "Active",
            "versionNumber": "3",
            "submissionNote": "Note",
        }

    def test_groups_after_string_trimming_and_keeps_different_source_dates(self):
        source = pd.DataFrame([
            _standard_row(),
            _standard_row(
                submissionNumber=" 5 ", companyCode=" HDD ",
                dataCollection=" PCD ", worksheetName=" Submission ",
                versionInfo=" 3 ", submissionNotes=" Note ",
            ),
            _standard_row(DateTimeCreated="01-02-2025 10:11:13"),
        ])

        actual = pcd_flow.transform_pcd_external_company_submission(source)

        assert len(actual) == 2
        assert actual["createdDate"].tolist() == [
            datetime(2025, 2, 1, 10, 11, 12),
            datetime(2025, 2, 1, 10, 11, 13),
        ]

    def test_applies_sql_null_defaults_and_left_lengths(self):
        actual = pcd_flow.transform_pcd_external_company_submission(
            pd.DataFrame([_standard_row(
                submissionNumber=None,
                companyCode="C" * 12,
                dataCollection="M" * 70,
                worksheetName="W" * 70,
                versionInfo="V" * 70,
                submissionNotes="N" * 2100,
                DateTimeCreated="invalid-date",
            )])
        ).iloc[0]

        assert actual["submissionNumber"] == "N/A"
        assert actual["partyCode"] == "C" * 8
        assert actual["pcdModel"] == "M" * 64
        assert actual["worksheetName"] == "W" * 64
        assert actual["versionNumber"] == "V" * 64
        assert actual["submissionNote"] == "N" * 2048
        assert pd.isna(actual["createdDate"])

    def test_null_source_timestamp_uses_current_timestamp_default(self):
        before = datetime.now()
        actual = pcd_flow.transform_pcd_external_company_submission(
            pd.DataFrame([_standard_row(DateTimeCreated=None)])
        )
        after = datetime.now()

        created = actual.loc[0, "createdDate"]
        assert before.replace(microsecond=0) <= created <= after

    def test_missing_source_column_is_rejected(self):
        source = pd.DataFrame([_standard_row()]).drop(columns="worksheetName")

        with pytest.raises(ValueError, match="worksheetName"):
            pcd_flow.transform_pcd_external_company_submission(source)


class TestPcdSchemeContract:
    def test_filter_accepts_the_four_sp_tabs_and_trimmed_pcd_prefix(self):
        tabs = ("DPB3", "BRL_DPB3_ADD", "DPWW3", "DPW3")
        source = pd.DataFrame([
            _standard_row(
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
            pd.DataFrame([_standard_row(tabName=tab_name)]), period_codes=()
        )

        assert actual.empty

    @pytest.mark.parametrize("datapoint_type", ["XCD_TARGET", "APCD_TARGET", "PCX_TARGET"])
    def test_filter_rejects_non_pcd_trimmed_prefix(self, datapoint_type):
        actual = pcd_flow.transform_pcd_scheme(
            pd.DataFrame([_standard_row(datapointTypeCode=datapoint_type)]),
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
            pd.DataFrame([_standard_row(tabName=tab_name)]), period_codes=()
        )

        assert actual.loc[0, "pcdCategory"] == category

    def test_maps_all_business_columns_for_a_lookup_match(self):
        actual = pcd_flow.transform_pcd_scheme(
            pd.DataFrame([_standard_row(datapointValue="4")]),
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
                _standard_row(
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
            pd.DataFrame([_standard_row(pcdApplicableToCompany=" Y")]),
            period_codes=(),
        )

        assert actual.loc[0, "pcdApplicableToCompany"] == 0

    def test_trims_all_output_strings_without_trimming_raw_filter_tab(self):
        actual = pcd_flow.transform_pcd_scheme(
            pd.DataFrame([
                _standard_row(
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
            pd.DataFrame([_standard_row(
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


class TestDeliverableSchemeMilestoneContract:
    def test_unpivots_each_of_the_fourteen_date_columns_in_sql_order(self):
        row = _list_row()
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
        row = _list_row(
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
        row = pd.DataFrame([_list_row()]).drop(columns="forecastIm6")

        with pytest.raises(ValueError, match="forecastIm6"):
            pcd_flow.transform_deliverable_scheme_milestone(row)


class TestDeliverableSchemeExpenditureContract:
    def test_unpivots_all_fourteen_values_and_maps_period_and_type(self):
        row = _list_row()
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
        assert actual.iloc[0].to_dict() == {
            "submissionNumber": "5", "businessDomain": "PCD", "partyCode": "HDD",
            "recordStatus": "Active", "schemeId": "S-1",
            "milestoneExpenditure": 1.5, "periodCode": "2023-24",
            "milestoneDetailType": "Baseline",
        }

    def test_invalid_values_become_zero_and_duplicate_rows_are_preserved(self):
        row = _list_row(baseline2023_24="not-numeric")
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
        row = _list_row(submissionNumber=None, companyCode=None, schemeId=None)
        actual = pcd_flow.transform_deliverable_scheme_expenditure(
            pd.DataFrame([row])
        )

        assert actual.loc[0, "submissionNumber"] is None
        assert actual.loc[0, "partyCode"] == "N/A"
        assert actual.loc[0, "schemeId"] == "N/A"
        assert actual.loc[0, "milestoneExpenditure"] == 0

    def test_left_limits_apply_to_party_code_and_scheme_id(self):
        row = _list_row(companyCode="C" * 12, schemeId="S" * 1100)
        actual = pcd_flow.transform_deliverable_scheme_expenditure(
            pd.DataFrame([row])
        )

        assert actual.loc[0, "partyCode"] == "C" * 8
        assert actual.loc[0, "schemeId"] == "S" * 1024

    def test_requires_every_expenditure_period_column(self):
        row = pd.DataFrame([_list_row()]).drop(columns="forecast2028_29")

        with pytest.raises(ValueError, match="forecast2028_29"):
            pcd_flow.transform_deliverable_scheme_expenditure(row)
