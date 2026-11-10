import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from dataprepkit import metadata_loader
from dataprepkit.metadata_loader import (
    run_dimension,
)
from dataprepkit.pcd_output_transform import (
    create_pcd_output_dimension_metadata,
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


@pytest.mark.parametrize(
    "tab_name",
    ["DPB1", "BRL_DPB1_ADD", "DPWW1", "DPW1"],
)
def test_output_procedure_filter_accepts_each_tab_without_filtering_datapoint_code(
    tab_name,
):
    actual = transform_price_control_deliverable_output(
        pd.DataFrame(
            [
                _source_row(
                    tabName=tab_name,
                    datapointTypeCode="NOT_A_PCD_CODE",
                    datapointValue="5",
                )
            ]
        )
    )

    assert len(actual) == 1
    assert actual.loc[0, "tabName"] == tab_name
    assert actual.loc[0, "bonCode"] == "NOT_A_PCD_CODE"


@pytest.mark.parametrize(
    "tab_name",
    ["DPB2", "BRL_DPB2_ADD", "DPWW2", "DPW2", "DPB3", "DPWW3", "DPW3", "unknown"],
)
def test_output_procedure_filter_rejects_tabs_not_in_sp_list(tab_name):
    actual = transform_price_control_deliverable_output(
        pd.DataFrame([_source_row(tabName=tab_name)])
    )

    assert actual.empty


def test_output_procedure_filter_accepts_sql_trailing_space_padding():
    actual = transform_price_control_deliverable_output(
        pd.DataFrame([_source_row(tabName="DPB1  ")])
    )

    assert len(actual) == 1
    assert actual.loc[0, "tabName"] == "DPB1"


def test_output_procedure_filter_rejects_leading_space_padding():
    actual = transform_price_control_deliverable_output(
        pd.DataFrame([_source_row(tabName=" DPB1")])
    )

    assert actual.empty


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
        ("DPB1", "Base"),
        ("BRL_DPB1_ADD", "N/A"),
        ("DPWW1", "Wastewater"),
        ("DPW1", "Water"),
    ],
)
def test_transform_maps_each_sp_accepted_tab_to_its_category(tab_name, expected_category):
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


def _pcd_dimension_metadata(target_table):
    return create_pcd_output_dimension_metadata(
        name="pcd_output_test",
        target_table=target_table,
        filepath="unused.csv",
    )


def _load_pcd(engine, metadata, incoming):
    return run_dimension(
        engine,
        metadata.name,
        metadata_registry={metadata.name: metadata},
        override_df=incoming,
    )


def test_pcd_public_load_lifecycle_uses_shared_scd2_without_deleting_missing_keys(
    monkeypatch,
):
    target_table = "pcd_output_lifecycle"
    engine = create_engine("sqlite:///:memory:")
    metadata = _pcd_dimension_metadata(target_table)
    run_times = iter(
        pd.to_datetime(
            [
                "2026-01-01T00:00:00Z",
                "2026-01-02T00:00:00Z",
                "2026-01-03T00:00:00Z",
                "2026-01-04T00:00:00Z",
                "2026-01-05T00:00:00Z",
            ]
        ).to_pydatetime()
    )
    monkeypatch.setattr(
        metadata_loader, "_capture_execution_time", lambda: next(run_times)
    )
    first_key = _source_row(submissionNumber="5")
    second_key = _source_row(submissionNumber="6")

    _load_pcd(engine, metadata, pd.DataFrame([first_key, second_key]))
    _load_pcd(engine, metadata, pd.DataFrame([first_key, second_key]))

    with engine.connect() as conn:
        rows_after_reload = conn.execute(
            text(f"SELECT * FROM {target_table} ORDER BY submissionNumber")
        ).mappings().all()
    assert len(rows_after_reload) == 2
    assert all(row["Current_Ind"] == 1 for row in rows_after_reload)
    assert all(row["Deleted_Ind"] == 0 for row in rows_after_reload)
    assert all(
        pd.Timestamp(row["Effective_Date_Start"])
        == pd.Timestamp("2026-01-01T00:00:00Z")
        for row in rows_after_reload
    )

    changed_first_key = _source_row(submissionNumber="5", datapointValue="20")
    _load_pcd(engine, metadata, pd.DataFrame([changed_first_key, second_key]))
    _load_pcd(engine, metadata, pd.DataFrame([second_key]))

    with engine.connect() as conn:
        after_partial = conn.execute(
            text(f"SELECT * FROM {target_table} ORDER BY submissionNumber, Current_Ind")
        ).mappings().all()
    assert len(after_partial) == 3
    current_rows = [row for row in after_partial if row["Current_Ind"] == 1]
    assert len(current_rows) == 2
    assert {row["submissionNumber"] for row in current_rows} == {"5", "6"}
    assert all(row["Deleted_Ind"] == 0 for row in current_rows)
    assert sum(row["cumulativeTarget"] == 20 for row in current_rows) == 1
    assert sum(row["Current_Ind"] == 0 for row in after_partial) == 1
    old_first_key = next(
        row
        for row in after_partial
        if row["submissionNumber"] == "5" and row["Current_Ind"] == 0
    )
    new_first_key = next(
        row
        for row in current_rows
        if row["submissionNumber"] == "5"
    )
    assert old_first_key["Deleted_Ind"] == 0
    assert pd.Timestamp(old_first_key["Effective_Date_End"]) == pd.Timestamp(
        "2026-01-03T00:00:00Z"
    )
    assert pd.Timestamp(new_first_key["Effective_Date_Start"]) == pd.Timestamp(
        "2026-01-03T00:00:00Z"
    )

    empty_source = pd.DataFrame(columns=list(_source_row()))
    _load_pcd(engine, metadata, empty_source)

    with engine.connect() as conn:
        after_empty = conn.execute(
            text(f"SELECT COUNT(*) FROM {target_table} WHERE Current_Ind = 1")
        ).scalar_one()
    assert after_empty == 2


def test_pcd_duplicate_transformed_keys_fail_without_changing_history():
    target_table = "pcd_output_duplicate"
    engine = create_engine("sqlite:///:memory:")
    metadata = _pcd_dimension_metadata(target_table)
    initial = pd.DataFrame([_source_row()])
    _load_pcd(engine, metadata, initial)
    conflicting = pd.DataFrame(
        [
            _source_row(datapointValue="2", pcdLineDescription="Line A"),
            _source_row(datapointValue="3", pcdLineDescription="Line B"),
        ]
    )

    with pytest.raises(ValueError, match="duplicate natural keys"):
        _load_pcd(engine, metadata, conflicting)

    with engine.connect() as conn:
        rows = conn.execute(text(f"SELECT * FROM {target_table}")).mappings().all()
    assert len(rows) == 1
    assert rows[0]["Current_Ind"] == 1
    assert rows[0]["cumulativeTarget"] == 12.5


def test_pcd_metadata_factory_declares_output_contract_and_scd2_policies():
    metadata = create_pcd_output_dimension_metadata(
        name="pcd_output",
        target_table="eds.pcd_output",
        filepath="source.csv",
    )

    assert metadata.natural_key_cols == [
        "submissionNumber",
        "businessDomain",
        "partyCode",
        "periodCode",
        "deliveryPlanReference",
        "pcdReference",
        "pcdOutputDescription",
        "bonCode",
    ]
    assert list(metadata.data_columns) == [
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
    assert metadata.processing_class is transform_price_control_deliverable_output
    assert metadata.processing_class_input == "raw"
    assert metadata.missing_rows_action == "retain"
    assert metadata.new_key_effective_date == "execution_time"
