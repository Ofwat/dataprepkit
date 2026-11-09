"""Prototype implementation of the PCD output staging transformation."""

from __future__ import annotations

import math
from typing import Any

import pandas as pd

from dataprepkit.metadata_loader import ColumnSpec, DimensionMetadata


SOURCE_COLUMNS = (
    "submissionNumber",
    "companyCode",
    "datapointPeriod",
    "dp_reference",
    "datapointTypeCode",
    "datapointValue",
    "pcdRef",
    "pcdApplicableToCompany",
    "pcdLineDescription",
    "pcdOutputDescription",
    "unitOfmeasure",
    "decimalPlace",
    "versionInfo",
    "submissionNotes",
    "tabName",
)

OUTPUT_COLUMNS = (
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
)

_GROUP_COLUMNS = tuple(
    column
    for column in OUTPUT_COLUMNS
    if column not in {"cumulativeTarget", "cumulativeOutturnForecast"}
)
_ACCEPTED_TABS = {"DPB1", "BRL_DPB1_ADD", "DPWW1", "DPW1"}


def _is_missing(value: Any) -> bool:
    return value is None or (not isinstance(value, (list, dict)) and pd.isna(value))


def _trim_or_na(value: Any, length: int) -> str:
    if _is_missing(value):
        return "N/A"
    return str(value).strip(" ")[:length]


def _trim(value: Any) -> str:
    return "" if _is_missing(value) else str(value).strip(" ")


def _trim_nullable(value: Any, length: int) -> str | None:
    return None if _is_missing(value) else str(value).strip(" ")[:length]


def _number_or_zero(value: Any) -> float:
    if _is_missing(value):
        return 0.0
    try:
        number = float(str(value).strip(" "))
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) else 0.0


def _decimal_place(value: Any) -> int | None:
    if _is_missing(value):
        return 0
    try:
        return int(str(value).strip(" "))
    except (TypeError, ValueError):
        return None


def _category(tab_name: str) -> str:
    upper_tab = tab_name.upper()
    if upper_tab in {"DPB1", "DPB2", "DPB1_ADD", "DPB2_ADD"}:
        return "Base"
    if upper_tab in {"DPWW1", "DPWW2", "DPWW3", "DPWW4", "DPWW5"}:
        return "Wastewater"
    if upper_tab in {"DPW1", "DPW2", "DPW3", "DPW4", "DPW5"}:
        return "Water"
    return "N/A"


def transform_price_control_deliverable_output(
    incoming: pd.DataFrame,
) -> pd.DataFrame:
    """Map raw PCD landing rows to the output table's business columns.

    This is a pure transformation: it does not read or write a database and it
    deliberately leaves natural-key conflict selection to the load contract.
    """
    if not isinstance(incoming, pd.DataFrame):
        raise TypeError("incoming must be a pandas DataFrame")
    missing = [column for column in SOURCE_COLUMNS if column not in incoming]
    if missing:
        raise ValueError(f"Missing source columns: {', '.join(missing)}")

    accepted = incoming.loc[
        incoming["tabName"].map(
            lambda value: not _is_missing(value)
            and str(value).rstrip(" ").upper() in _ACCEPTED_TABS
        )
    ]
    rows = []
    for source in accepted.to_dict(orient="records"):
        tab = _trim_or_na(source["tabName"], 64)
        period = _trim_or_na(source["datapointPeriod"], 256)
        datapoint_type = _trim_or_na(source["datapointTypeCode"], 256)
        value = source["datapointValue"]
        numeric_value = _number_or_zero(value)
        type_upper = _trim(datapoint_type).upper()
        is_outturn = type_upper.endswith(("_O", "_OT"))
        is_company_rag = _trim(period).upper().startswith(
            "RAG RATING - COMPANY"
        )
        is_assurer_rag = _trim(period).upper().startswith(
            "RAG RATING - ASSURER"
        )
        applicable = _trim(source["pcdApplicableToCompany"]).upper()

        rows.append(
            {
                "submissionNumber": _trim_or_na(source["submissionNumber"], 64),
                "businessDomain": "PCD",
                "partyCode": _trim_or_na(source["companyCode"], 8),
                "periodCode": period,
                "deliveryPlanReference": _trim_or_na(
                    source["dp_reference"], 32
                ),
                "pcdReference": _trim_or_na(source["pcdRef"], 256),
                "pcdOutputDescription": _trim_or_na(
                    source["pcdOutputDescription"], 2048
                ),
                "bonCode": _trim_or_na(datapoint_type, 256),
                "pcdCategory": _category(_trim(tab)),
                "tabName": tab,
                "pcdApplicableToCompany": int(applicable in {"Y", "1"}),
                "ragRatingCompany": (
                    _trim_nullable(value, 16) if is_company_rag else "N/A"
                ),
                "ragRatingAssurer": (
                    _trim_nullable(value, 16) if is_assurer_rag else "N/A"
                ),
                "pcdLineDescription": _trim_or_na(
                    source["pcdLineDescription"], 2048
                ),
                "cumulativeTarget": (
                    numeric_value if not is_outturn else 0.0
                ),
                "cumulativeOutturnForecast": (
                    numeric_value if is_outturn else 0.0
                ),
                "unitOfmeasure": _trim_or_na(source["unitOfmeasure"], 256),
                "decimalPlace": _decimal_place(source["decimalPlace"]),
                "versionNumber": _trim_or_na(source["versionInfo"], 64),
                "submissionNote": _trim_or_na(source["submissionNotes"], 2048),
                "columnHeader": period,
                "recordStatus": "Active",
            }
        )

    mapped = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    if mapped.empty:
        return mapped

    # SQL groups every non-measure output value and sums null-safe measures.
    grouped = (
        mapped.groupby(list(_GROUP_COLUMNS), dropna=False, sort=False)[
            ["cumulativeTarget", "cumulativeOutturnForecast"]
        ]
        .sum()
        .reset_index()
    )
    return grouped.loc[:, OUTPUT_COLUMNS]


def create_pcd_output_dimension_metadata(
    *,
    name: str,
    target_table: str,
    filepath: str,
) -> DimensionMetadata:
    """Create metadata for loading transformed PCD output rows with SCD2."""
    natural_key_types = {
        "submissionNumber": "VARCHAR(64)",
        "businessDomain": "VARCHAR(1024)",
        "partyCode": "VARCHAR(8)",
        "periodCode": "VARCHAR(256)",
        "deliveryPlanReference": "VARCHAR(32)",
        "pcdReference": "VARCHAR(256)",
        "pcdOutputDescription": "VARCHAR(2048)",
        "bonCode": "VARCHAR(256)",
    }
    data_types = {
        "pcdCategory": "VARCHAR(16)",
        "tabName": "VARCHAR(64)",
        "pcdApplicableToCompany": "BIT",
        "ragRatingCompany": "VARCHAR(16)",
        "ragRatingAssurer": "VARCHAR(16)",
        "pcdLineDescription": "VARCHAR(2048)",
        "cumulativeTarget": "FLOAT",
        "cumulativeOutturnForecast": "FLOAT",
        "unitOfmeasure": "VARCHAR(256)",
        "decimalPlace": "INT",
        "versionNumber": "VARCHAR(64)",
        "submissionNote": "VARCHAR(2048)",
        "columnHeader": "VARCHAR(256)",
        "recordStatus": "VARCHAR(16)",
    }
    return DimensionMetadata(
        name=name,
        target_table=target_table,
        natural_key_cols=list(natural_key_types),
        natural_key_specs={
            column: ColumnSpec(type=sql_type)
            for column, sql_type in natural_key_types.items()
        },
        data_columns={
            column: ColumnSpec(type=sql_type)
            for column, sql_type in data_types.items()
        },
        surrogate_key="surrogate_key",
        join_numeric_key="join_numeric_key",
        filepath=filepath,
        reserved_source_members=[],
        processing_class=transform_price_control_deliverable_output,
        processing_class_input="raw",
        missing_rows_action="retain",
        new_key_effective_date="execution_time",
    )
