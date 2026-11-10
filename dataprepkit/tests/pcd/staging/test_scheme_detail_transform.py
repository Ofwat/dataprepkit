from datetime import date

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from dataprepkit import metadata_loader
from dataprepkit.metadata_loader import run_dimension
from dataprepkit.pcd_scheme_detail_transform import (
    OUTPUT_COLUMNS,
    create_deliverable_scheme_detail_dimension_metadata,
    transform_deliverable_scheme_detail,
)


def _source_row(**updates):
    row = {
        "submissionNumber": " 12 ",
        "companyCode": " ABC ",
        "tabName": " DPW1 ",
        "lineDescription": " Line 1 ",
        "versionInfo": " v3 ",
        "schemeId": " S-1 ",
        "typeOfScheme": " Primary ",
        "imMoveReason": " Reason ",
        "substitutedSchemeName": " Substitute ",
        "pcdRef": " REF-1 ",
        "eaId": " EA-1 ",
        "winepId": " WINEP-1 ",
        "schemeName": " Scheme A ",
        "forecastToComplete": "123.5",
        "originalRegulatoryDate": "2024-02-29",
        "currentRegulatoryDate": "2025-03-01",
        "currentInterimMilestone": "IM2",
        "spendToDate": "bad-number",
        "forecastOutturn": "1e2",
        "percentSpendToDate": "12.5",
        "criticalHighProfile": "yes",
        "schemeType": " Secondary ",
        "amp": " AMP8 ",
        "submissionNotes": " Note ",
    }
    row.update(updates)
    return row


def test_transform_maps_landing_fields_and_sp_constants():
    actual = transform_deliverable_scheme_detail(pd.DataFrame([_source_row()]))

    assert tuple(actual.columns) == OUTPUT_COLUMNS
    assert len(actual) == 1
    assert actual.iloc[0].to_dict() == {
        "submissionNumber": "12",
        "businessDomain": "PCD",
        "partyCode": "ABC",
        "recordStatus": "Active",
        "pcdCategory": "Water",
        "tabName": "DPW1",
        "schemeLineDescription": "Line 1",
        "versionNumber": "v3",
        "schemeId": "S-1",
        "schemeType": "Primary",
        "interimMilestoneMoveReason": "Reason",
        "substitutedSchemeName": "Substitute",
        "pcdReference": "REF-1",
        "eaId": "EA-1",
        "winepId": "WINEP-1",
        "schemeName": "Scheme A",
        "forecastToComplete": 123.5,
        "originalRegulatoryDate": date(2024, 2, 29),
        "currentRegulatoryDate": date(2025, 3, 1),
        "currentInterimMilestone": "IM2",
        "spendToDate": 0.0,
        "forecastOutturnCost": 100.0,
        "percentageSpendToDate": 12.5,
        "criticalHighProfile": True,
        "schemeSecondaryType": "Secondary",
        "assetManagementPeriod": "AMP8",
        "submissionNote": "Note",
    }


@pytest.mark.parametrize(
    ("tab_name", "expected_category"),
    [
        ("DPB1", "Base"),
        (" dpb2 ", "Base"),
        ("DPWW1", "Waste Water"),
        ("dpww5", "Waste Water"),
        ("DPW1", "Water"),
        ("dpw5", "Water"),
        ("unmapped", "N/A"),
        (None, "N/A"),
    ],
)
def test_transform_maps_tab_to_pcd_category(tab_name, expected_category):
    actual = transform_deliverable_scheme_detail(
        pd.DataFrame([_source_row(tabName=tab_name)])
    )

    assert actual.loc[0, "pcdCategory"] == expected_category


def test_transform_applies_null_defaults_and_declared_truncation():
    actual = transform_deliverable_scheme_detail(
        pd.DataFrame(
            [
                _source_row(
                    submissionNumber=None,
                    companyCode=None,
                    tabName=None,
                    lineDescription=None,
                    versionInfo=None,
                    schemeId="x" * 1100,
                    pcdRef=None,
                    originalRegulatoryDate="not-a-date",
                    currentRegulatoryDate="31/12/2024",
                    forecastToComplete="not-numeric",
                    spendToDate=None,
                    forecastOutturn=None,
                    percentSpendToDate="",
                    criticalHighProfile=" YES ",
                    schemeType=None,
                    amp=None,
                    submissionNotes=None,
                )
            ]
        )
    )
    row = actual.iloc[0]

    assert row["submissionNumber"] == "N/A"
    assert row["partyCode"] == "N/A"
    assert row["tabName"] == "N/A"
    assert row["pcdCategory"] == "N/A"
    assert row["schemeLineDescription"] == "N/A"
    assert row["versionNumber"] == "N/A"
    assert row["schemeId"] == "x" * 1024
    assert row["pcdReference"] == "N/A"
    assert pd.isna(row["originalRegulatoryDate"])
    assert pd.isna(row["currentRegulatoryDate"])
    assert row["forecastToComplete"] == 0.0
    assert row["spendToDate"] == 0.0
    assert row["forecastOutturnCost"] == 0.0
    assert row["percentageSpendToDate"] == 0.0
    # The SP uppercases this field but does not trim it before testing YES.
    assert row["criticalHighProfile"] == False
    assert row["schemeSecondaryType"] == "N/A"
    assert row["assetManagementPeriod"] == "N/A"
    assert row["submissionNote"] == "N/A"


def test_transform_preserves_duplicate_source_rows_for_shared_validation():
    source = pd.DataFrame([_source_row(), _source_row()])

    actual = transform_deliverable_scheme_detail(source)

    assert len(actual) == 2


def test_transform_requires_all_columns_read_by_the_procedure():
    source = pd.DataFrame([_source_row()]).drop(columns=["forecastOutturn"])

    with pytest.raises(ValueError, match="forecastOutturn"):
        transform_deliverable_scheme_detail(source)


def test_scheme_detail_metadata_uses_declared_keys_attributes_and_policies():
    metadata = create_deliverable_scheme_detail_dimension_metadata(
        name="deliverable_scheme_detail",
        target_table="deliverable_scheme_detail_prototype",
        filepath="unused.csv",
    )

    assert metadata.natural_key_cols == [
        "submissionNumber",
        "businessDomain",
        "partyCode",
        "schemeId",
    ]
    assert list(metadata.data_columns) == [
        column for column in OUTPUT_COLUMNS if column not in metadata.natural_key_cols
    ]
    assert metadata.data_columns["pcdCategory"].type == "VARCHAR(1024)"
    assert metadata.data_columns["originalRegulatoryDate"].type == "DATE"
    assert metadata.data_columns["criticalHighProfile"].type == "BIT"
    assert metadata.processing_class is transform_deliverable_scheme_detail
    assert metadata.processing_class_input == "raw"
    assert metadata.missing_rows_action == "retain"
    assert metadata.new_key_effective_date == "execution_time"


def test_scheme_detail_runs_through_shared_scd2_loader(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    metadata = create_deliverable_scheme_detail_dimension_metadata(
        name="deliverable_scheme_detail",
        target_table="deliverable_scheme_detail_prototype",
        filepath="unused.csv",
    )
    execution_times = iter(
        pd.to_datetime(
            [
                "2026-01-01T00:00:00Z",
                "2026-01-02T00:00:00Z",
                "2026-01-03T00:00:00Z",
            ]
        ).to_pydatetime()
    )
    monkeypatch.setattr(
        metadata_loader, "_capture_execution_time", lambda: next(execution_times)
    )

    def load(source):
        run_dimension(
            engine,
            metadata.name,
            metadata_registry={metadata.name: metadata},
            override_df=pd.DataFrame([source]),
        )

    original = _source_row()
    load(original)
    load(original)
    load(_source_row(schemeName="Updated scheme"))

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT schemeName, Current_Ind, Deleted_Ind "
                "FROM deliverable_scheme_detail_prototype "
                "ORDER BY Current_Ind"
            )
        ).mappings().all()

    assert len(rows) == 2
    assert sum(row["Current_Ind"] == 1 for row in rows) == 1
    assert sum(row["Current_Ind"] == 0 for row in rows) == 1
    assert {row["schemeName"] for row in rows} == {"Scheme A", "Updated scheme"}
