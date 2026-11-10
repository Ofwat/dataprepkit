import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from dataprepkit import pcd_flow
from dataprepkit.pcd_output_transform import create_pcd_output_dimension_metadata
from dataprepkit.tests.pcd.fixtures import (
    _list_landing,
    _list_row,
    _standard_landing,
    _standard_row,
)

def test_stage_pcd_landing_builds_all_seven_sp_staging_outputs():
    batch = pcd_flow.stage_pcd_landing(
        _standard_landing(),
        _list_landing(),
        period_codes=(),
    )

    expected_order = (
        "external_company_submission",
        "price_control_deliverable_output",
        "price_control_deliverable_expenditure",
        "price_control_deliverable_scheme",
        "deliverable_scheme_detail",
        "deliverable_scheme_milestone",
        "deliverable_scheme_expenditure",
    )
    assert tuple(batch.frames) == expected_order
    assert all(frame.empty for frame in batch.frames.values())
    assert _standard_landing().empty
    assert _list_landing().empty


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
