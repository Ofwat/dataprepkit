from examples.pcd_staging_parity import make_pcd_parity_inputs


def test_generated_landing_inputs_cover_all_staging_category_tabs():
    _, standard_landing, scheme_detail_landing = make_pcd_parity_inputs(
        matched_period_code="2025-26"
    )

    standard_tabs = set(standard_landing["tabName"])
    assert {"DPB1", "BRL_DPB1_ADD", "DPWW1", "DPW1"} <= standard_tabs
    assert {"DPB2", "BRL_DPB2_ADD", "DPWW2", "DPW2"} <= standard_tabs
    assert {"DPB3", "BRL_DPB3_ADD", "DPWW3", "DPW3"} <= standard_tabs

    detail_tabs = {
        value.strip().upper()
        for value in scheme_detail_landing["tabName"].dropna()
    }
    assert {"DPB1", "DPB2", "DPB1_ADD"} <= detail_tabs
    assert {f"DPWW{number}" for number in range(1, 6)} <= detail_tabs
    assert {f"DPW{number}" for number in range(1, 6)} <= detail_tabs
    assert "UNKNOWN" in detail_tabs
    assert scheme_detail_landing["tabName"].isna().any()
