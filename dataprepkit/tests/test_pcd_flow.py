import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from dataprepkit import pcd_flow
from dataprepkit.pcd_output_transform import create_pcd_output_dimension_metadata


def _standard_landing():
    return pd.DataFrame(
        columns=[
            "submissionNumber",
            "companyCode",
            "dataCollection",
            "worksheetName",
            "datapointPeriod",
            "dp_reference",
            "datapointTypeCode",
            "datapointValue",
            "DateTimeCreated",
            "pcdModel",
            "pcdRef",
            "pcdApplicableToCompany",
            "pcdLineDescription",
            "pcdOutputDescription",
            "unitOfmeasure",
            "decimalPlace",
            "versionInfo",
            "submissionNotes",
            "tabName",
        ]
    )


def _list_landing():
    columns = [
        "submissionNumber",
        "companyCode",
        "tabName",
        "lineDescription",
        "versionInfo",
        "schemeId",
        "typeOfScheme",
        "imMoveReason",
        "substitutedSchemeName",
        "pcdRef",
        "eaId",
        "winepId",
        "schemeName",
        "forecastToComplete",
        "originalRegulatoryDate",
        "currentRegulatoryDate",
        "currentInterimMilestone",
        "spendToDate",
        "forecastOutturn",
        "percentSpendToDate",
        "criticalHighProfile",
        "schemeType",
        "amp",
        "submissionNotes",
    ]
    columns.extend(f"baselineIm{number}" for number in range(1, 8))
    columns.extend(f"forecastIm{number}" for number in range(1, 8))
    columns.extend(
        f"{period}{year}_{next_year % 100:02d}"
        for period in ("baseline", "forecast")
        for year, next_year in zip(range(2023, 2030), range(2024, 2031))
    )
    return pd.DataFrame(columns=columns)


def _standard_row(**updates):
    row = {column: None for column in _standard_landing().columns}
    row.update(
        submissionNumber="5",
        companyCode="HDD",
        dataCollection="PCD",
        worksheetName="Submission",
        datapointPeriod="2025-26",
        dp_reference="DP-1",
        datapointTypeCode="PCD_TARGET",
        datapointValue="12.5",
        DateTimeCreated="21-10-2026",
        pcdRef="REF-1",
        pcdApplicableToCompany="Y",
        pcdLineDescription="Line",
        pcdOutputDescription="Output",
        unitOfmeasure="Ml/d",
        decimalPlace="1",
        versionInfo="3",
        submissionNotes="Note",
        tabName="DPB2",
    )
    row.update(updates)
    return row


def _list_row(**updates):
    row = {column: None for column in _list_landing().columns}
    row.update(
        submissionNumber="5",
        companyCode="HDD",
        tabName="DPW1",
        lineDescription="Line",
        versionInfo="3",
        schemeId="S-1",
        typeOfScheme="Primary",
        imMoveReason="Reason",
        substitutedSchemeName="",
        pcdRef="REF-1",
        eaId="EA-1",
        winepId="WINEP-1",
        schemeName="Scheme",
        forecastToComplete="10",
        originalRegulatoryDate="2024-01-02",
        currentRegulatoryDate="2025-01-02",
        currentInterimMilestone="IM2",
        spendToDate="5",
        forecastOutturn="20",
        percentSpendToDate="25",
        criticalHighProfile="YES",
        schemeType="Secondary",
        amp="AMP8",
        submissionNotes="Note",
    )
    row.update(updates)
    return row


def test_stage_pcd_landing_builds_all_seven_sp_staging_outputs():
    batch = pcd_flow.stage_pcd_landing(
        _standard_landing(),
        _list_landing(),
        period_codes=(),
    )

    assert set(batch.frames) == {
        "external_company_submission",
        "price_control_deliverable_output",
        "price_control_deliverable_expenditure",
        "price_control_deliverable_scheme",
        "deliverable_scheme_detail",
        "deliverable_scheme_milestone",
        "deliverable_scheme_expenditure",
    }
    assert all(frame.empty for frame in batch.frames.values())
    assert _standard_landing().empty
    assert _list_landing().empty


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


def test_pcd_scd2_selects_implemented_pcd_and_gen_from_shared_staging(
    monkeypatch,
):
    metadata = create_pcd_output_dimension_metadata(
        name="pcd_output",
        target_table="pcd_output_test",
        filepath="unused.csv",
    )
    output = pd.DataFrame(
        columns=[*metadata.natural_key_cols, *metadata.data_columns]
    )
    party = output.copy()
    definitions = (
        pcd_flow.PcdDimension(
            metadata=metadata,
            subject_area="PCD",
            table_status="Implemented",
            staging_table="price_control_deliverable_output",
        ),
        pcd_flow.PcdDimension(
            metadata=metadata.model_copy(update={"name": "party"}),
            subject_area="GEN",
            table_status="Implemented",
            staging_table="party",
        ),
        pcd_flow.PcdDimension(
            metadata=metadata.model_copy(update={"name": "nav"}),
            subject_area="NAV",
            table_status="Implemented",
            staging_table="nav",
        ),
        pcd_flow.PcdDimension(
            metadata=metadata.model_copy(update={"name": "approved"}),
            subject_area="PCD",
            table_status="Approved",
            staging_table="approved",
        ),
    )
    calls = []
    monkeypatch.setattr(
        pcd_flow,
        "run_dimension",
        lambda engine, name, **kwargs: calls.append((name, kwargs)) or name,
    )

    result = pcd_flow.apply_pcd_scd2(
        create_engine("sqlite:///:memory:"),
        pcd_flow.PcdStagingBatch(
            frames={"price_control_deliverable_output": output}
        ),
        dimensions=definitions,
        gen_staging={"party": party},
    )

    assert [name for name, _ in calls] == ["pcd_output", "party"]
    assert all(kwargs["override_df"] is not None for _, kwargs in calls)
    assert all(
        kwargs["metadata_registry"][name].processing_class is None
        for name, kwargs in calls
    )
    assert result.loaded_tables == ("pcd_output", "party")
    assert result.skipped_tables == {
        "nav": "subject area is not PCD or GEN",
        "approved": "table status is not Implemented",
    }
    assert metadata.processing_class is not None


def test_missing_selected_gen_dependency_fails_before_any_scd2_write(monkeypatch):
    metadata = create_pcd_output_dimension_metadata(
        name="party",
        target_table="party_test",
        filepath="unused.csv",
    )
    calls = []
    monkeypatch.setattr(
        pcd_flow,
        "run_dimension",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    with pytest.raises(pcd_flow.PcdDependencyError, match="party"):
        pcd_flow.apply_pcd_scd2(
            create_engine("sqlite:///:memory:"),
            pcd_flow.PcdStagingBatch(frames={}),
            dimensions=(
                pcd_flow.PcdDimension(
                    metadata=metadata,
                    subject_area="GEN",
                    table_status="Implemented",
                    staging_table="party",
                ),
            ),
            gen_staging={},
        )

    assert calls == []


def test_pcd_flow_sends_staged_business_frame_through_real_scd2_loader():
    engine = create_engine("sqlite:///:memory:")
    metadata = create_pcd_output_dimension_metadata(
        name="pcd_output",
        target_table="pcd_output_flow_test",
        filepath="unused.csv",
    )
    staged = pcd_flow.transform_price_control_deliverable_output(
        pd.DataFrame([_standard_row(tabName="DPB1", datapointTypeCode="PCD_TARGET")])
    )
    dimension = pcd_flow.PcdDimension(
        metadata=metadata,
        subject_area="PCD",
        table_status="Implemented",
        staging_table="price_control_deliverable_output",
    )
    batch = pcd_flow.PcdStagingBatch(
        frames={"price_control_deliverable_output": staged}
    )

    pcd_flow.apply_pcd_scd2(engine, batch, dimensions=(dimension,))
    pcd_flow.apply_pcd_scd2(engine, batch, dimensions=(dimension,))

    with engine.connect() as connection:
        rows = connection.execute(
            text(
                "SELECT COUNT(*) AS total, SUM(Current_Ind) AS current_rows "
                "FROM pcd_output_flow_test"
            )
        ).mappings().one()
    assert rows["total"] == 1
    assert rows["current_rows"] == 1
