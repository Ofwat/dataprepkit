import pandas as pd
import pytest
from datetime import datetime

from dataprepkit import pcd_flow
from dataprepkit.tests.pcd.fixtures import _standard_row

from dataprepkit.tests.pcd.staging.contract_fixtures import (
    _list_row as _contract_list_row,
    _scheme_row as _contract_scheme_row,
    _standard_row as _contract_standard_row,
)

def test_external_submission_deduplicates_same_metadata_row():
    source = pd.DataFrame([_standard_row(), _standard_row()])

    actual = pcd_flow.transform_pcd_external_company_submission(source)

    assert len(actual) == 1
    assert actual.loc[0, "businessDomain"] == "PCD"
    assert actual.loc[0, "partyCode"] == "HDD"
    assert actual.loc[0, "createdDate"].date().isoformat() == "2026-10-21"


class TestExternalCompanySubmissionContract:
    def test_maps_all_business_columns_and_style_105_timestamp(self):
        actual = pcd_flow.transform_pcd_external_company_submission(
            pd.DataFrame([_contract_standard_row()])
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
            _contract_standard_row(),
            _contract_standard_row(
                submissionNumber=" 5 ", companyCode=" HDD ",
                dataCollection=" PCD ", worksheetName=" Submission ",
                versionInfo=" 3 ", submissionNotes=" Note ",
            ),
            _contract_standard_row(DateTimeCreated="01-02-2025 10:11:13"),
        ])

        actual = pcd_flow.transform_pcd_external_company_submission(source)

        assert len(actual) == 2
        assert actual["createdDate"].tolist() == [
            datetime(2025, 2, 1, 10, 11, 12),
            datetime(2025, 2, 1, 10, 11, 13),
        ]

    def test_applies_sql_null_defaults_and_left_lengths(self):
        actual = pcd_flow.transform_pcd_external_company_submission(
            pd.DataFrame([_contract_standard_row(
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
            pd.DataFrame([_contract_standard_row(DateTimeCreated=None)])
        )
        after = datetime.now()

        created = actual.loc[0, "createdDate"]
        assert before.replace(microsecond=0) <= created <= after

    def test_missing_source_column_is_rejected(self):
        source = pd.DataFrame([_contract_standard_row()]).drop(columns="worksheetName")

        with pytest.raises(ValueError, match="worksheetName"):
            pcd_flow.transform_pcd_external_company_submission(source)
