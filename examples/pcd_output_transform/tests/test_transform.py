import pandas as pd
import pytest

from examples.pcd_output_transform import (
    transform_price_control_deliverable_output,
)


def _source_row(**overrides):
    row = {
        "submissionNumber": "5",
        "companyCode": "HDD",
        "datapointPeriod": " 2025-26 ",
        "dp_reference": " DP-1 ",
        "datapointTypeCode": "PCD_TARGET",
        "datapointValue": "12.5",
        "pcdModel": "unused",
        "pcdRef": " REF-1 ",
        "pcdApplicableToCompany": "Y",
        "pcdLineDescription": " Line ",
        "pcdOutputDescription": " Output ",
        "unitOfmeasure": " Ml/d ",
        "decimalPlace": "1",
        "versionInfo": "3",
        "submissionNotes": " note ",
        "tabName": "DPB1",
    }
    row.update(overrides)
    return row


def test_transform_maps_and_filters_pcd_output_rows():
    source = pd.DataFrame(
        [
            _source_row(),
            _source_row(
                tabName="DPB2",
                datapointValue="99",
            ),
            _source_row(
                tabName="DPWW1",
                datapointTypeCode="PCD_OUTTURN_O",
                datapointValue="7.25",
                pcdApplicableToCompany="N",
            ),
            _source_row(
                datapointTypeCode="PCD_RAG",
                datapointValue="Green",
                datapointPeriod="RAG Rating - Company Q1",
            ),
        ]
    )

    actual = transform_price_control_deliverable_output(source)

    assert list(actual.columns) == [
        "submissionNumber",
        "businessDomain",
        "partyCode",
        "periodCode",
        "deliveryPlanReference",
        "pcdReference",
        "pcdOutputDescription",
        "bonCode",
        "pcdCategory",
        "tabName",
        "pcdApplicableToCompany",
        "ragRatingCompany",
        "ragRatingAssurer",
        "pcdLineDescription",
        "cumulativeTarget",
        "cumulativeOutturnForecast",
        "unitOfmeasure",
        "decimalPlace",
        "versionNumber",
        "submissionNote",
        "columnHeader",
        "recordStatus",
    ]
    assert len(actual) == 3

    base, wastewater, rag = actual.iloc[0], actual.iloc[1], actual.iloc[2]
    assert base["businessDomain"] == "PCD"
    assert base["partyCode"] == "HDD"
    assert base["periodCode"] == "2025-26"
    assert base["deliveryPlanReference"] == "DP-1"
    assert base["pcdReference"] == "REF-1"
    assert base["bonCode"] == "PCD_TARGET"
    assert base["pcdCategory"] == "Base"
    assert base["pcdApplicableToCompany"] == 1
    assert base["ragRatingCompany"] == "N/A"
    assert base["ragRatingAssurer"] == "N/A"
    assert base["cumulativeTarget"] == 12.5
    assert base["cumulativeOutturnForecast"] == 0
    assert base["recordStatus"] == "Active"

    assert wastewater["pcdCategory"] == "Wastewater"
    assert wastewater["pcdApplicableToCompany"] == 0
    assert wastewater["ragRatingCompany"] == "N/A"
    assert wastewater["cumulativeTarget"] == 0
    assert wastewater["cumulativeOutturnForecast"] == 7.25

    assert rag["ragRatingCompany"] == "Green"
    assert rag["columnHeader"] == "RAG Rating - Company Q1"


def test_transform_groups_identical_business_rows_and_sums_measures():
    source = pd.DataFrame(
        [
            _source_row(datapointValue="2"),
            _source_row(datapointValue="3"),
        ]
    )

    actual = transform_price_control_deliverable_output(source)

    assert len(actual) == 1
    assert actual.loc[0, "cumulativeTarget"] == 5
    assert actual.loc[0, "cumulativeOutturnForecast"] == 0


def test_transform_does_not_choose_between_rows_sharing_the_natural_key():
    source = pd.DataFrame(
        [
            _source_row(datapointValue="2", pcdLineDescription="Line A"),
            _source_row(datapointValue="3", pcdLineDescription="Line B"),
        ]
    )

    actual = transform_price_control_deliverable_output(source)

    assert len(actual) == 2
    assert actual["pcdLineDescription"].tolist() == ["Line A", "Line B"]


def test_transform_empty_source_returns_typed_empty_business_frame():
    source = pd.DataFrame(columns=list(_source_row()))

    actual = transform_price_control_deliverable_output(source)

    assert actual.empty
    assert actual.columns.tolist() == transform_price_control_deliverable_output(
        pd.DataFrame([_source_row()])
    ).columns.tolist()


@pytest.mark.parametrize(
    ("tab_name", "expected_category"),
    [
        ("BRL_DPB1_ADD", "N/A"),
        ("DPW1", "Water"),
    ],
)
def test_transform_accepts_additional_supported_tabs(tab_name, expected_category):
    actual = transform_price_control_deliverable_output(
        pd.DataFrame([_source_row(tabName=tab_name)])
    )

    assert len(actual) == 1
    assert actual.loc[0, "pcdCategory"] == expected_category


def test_transform_applies_sql_null_defaults_and_declared_lengths():
    actual = transform_price_control_deliverable_output(
        pd.DataFrame(
            [
                _source_row(
                    submissionNumber=None,
                    companyCode="H" * 10,
                    dp_reference="D" * 40,
                    pcdRef=None,
                    datapointValue="not numeric",
                    pcdApplicableToCompany="unexpected",
                    unitOfmeasure=None,
                    decimalPlace=None,
                    versionInfo=None,
                    submissionNotes=None,
                )
            ]
        )
    )
    row = actual.iloc[0]

    assert row["submissionNumber"] == "N/A"
    assert row["partyCode"] == "H" * 8
    assert row["deliveryPlanReference"] == "D" * 32
    assert row["pcdReference"] == "N/A"
    assert row["pcdApplicableToCompany"] == 0
    assert row["unitOfmeasure"] == "N/A"
    assert row["decimalPlace"] == 0
    assert row["versionNumber"] == "N/A"
    assert row["submissionNote"] == "N/A"
    assert row["cumulativeTarget"] == 0
    assert row["cumulativeOutturnForecast"] == 0


def test_transform_rejects_missing_source_columns_without_mutating_input():
    source = pd.DataFrame([_source_row()])
    original = source.copy(deep=True)

    with pytest.raises(ValueError, match="Missing source columns: pcdRef"):
        transform_price_control_deliverable_output(source.drop(columns="pcdRef"))

    pd.testing.assert_frame_equal(source, original)


def test_transform_preserves_null_rag_value_as_null():
    actual = transform_price_control_deliverable_output(
        pd.DataFrame(
            [
                _source_row(
                    datapointPeriod="RAG Rating - Assurer Q1",
                    datapointTypeCode="PCD_RAG",
                    datapointValue=None,
                )
            ]
        )
    )

    assert pd.isna(actual.loc[0, "ragRatingAssurer"])


def test_transform_uses_full_datapoint_code_for_bon_and_measure_routing():
    datapoint_code = f"{'X' * 70}_OT"
    actual = transform_price_control_deliverable_output(
        pd.DataFrame(
            [
                _source_row(
                    datapointTypeCode=datapoint_code,
                    datapointValue="9.5",
                )
            ]
        )
    )

    assert actual.loc[0, "bonCode"] == datapoint_code
    assert actual.loc[0, "cumulativeTarget"] == 0
    assert actual.loc[0, "cumulativeOutturnForecast"] == 9.5
