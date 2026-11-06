"""Python transform for the PCD Deliverable Scheme Detail procedure."""

from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any

import pandas as pd


SOURCE_COLUMNS = (
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
)

OUTPUT_COLUMNS = (
    "submissionNumber",
    "businessDomain",
    "partyCode",
    "recordStatus",
    "pcdCategory",
    "tabName",
    "schemeLineDescription",
    "versionNumber",
    "schemeId",
    "schemeType",
    "interimMilestoneMoveReason",
    "substitutedSchemeName",
    "pcdReference",
    "eaId",
    "winepId",
    "schemeName",
    "forecastToComplete",
    "originalRegulatoryDate",
    "currentRegulatoryDate",
    "currentInterimMilestone",
    "spendToDate",
    "forecastOutturnCost",
    "percentageSpendToDate",
    "criticalHighProfile",
    "schemeSecondaryType",
    "assetManagementPeriod",
    "submissionNote",
)

_TAB_CATEGORIES = {
    "DPB1": "Base",
    "DPB2": "Base",
    **{f"DPWW{number}": "Waste Water" for number in range(1, 6)},
    **{f"DPW{number}": "Water" for number in range(1, 6)},
}


def _is_missing(value: Any) -> bool:
    return value is None or (not isinstance(value, (list, dict)) and pd.isna(value))


def _trim_or_na(value: Any, length: int) -> str:
    if _is_missing(value):
        return "N/A"
    return str(value).strip(" ")[:length]


def _category_for_tab(value: Any) -> str:
    if _is_missing(value):
        return "N/A"
    return _TAB_CATEGORIES.get(str(value).strip(" ").upper(), "N/A")


def _sql_float_or_zero(value: Any) -> float:
    """Handle ordinary SQL numeric text used by the landing columns.

    The stored procedure gates TRY_CAST with SQL Server ISNUMERIC. This
    portable parser covers standard decimal and exponent forms; unusual
    ISNUMERIC-accepted currency or locale forms require SQL Server parity
    validation before claiming exact conversion equivalence.
    """
    if _is_missing(value):
        return 0.0
    try:
        parsed = float(str(value).strip(" "))
    except (TypeError, ValueError, OverflowError):
        return 0.0
    return parsed if math.isfinite(parsed) else 0.0


def _date_style_23(value: Any) -> date | None:
    if _is_missing(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw = str(value).strip(" ")
    try:
        # SQL TRY_CONVERT(datetime2, value, 23) is used before insertion into
        # a DATE column. ISO timestamps retain only their calendar date here.
        return date.fromisoformat(raw[:10]) if len(raw) >= 10 else None
    except ValueError:
        return None


def transform_deliverable_scheme_detail(incoming: pd.DataFrame) -> pd.DataFrame:
    """Transform landing rows using ``cde.sp_stg_deliverable_scheme_detail``.

    The returned frame contains business/output fields only. Batch numbers,
    staging timestamps, change reason, created-by values, and landing archive
    writes belong to the load/orchestration layer.
    """
    if incoming is None:
        raise ValueError("Incoming data must be a DataFrame.")
    source = pd.DataFrame(incoming).copy()
    missing = [column for column in SOURCE_COLUMNS if column not in source.columns]
    if missing:
        raise ValueError(f"Incoming data is missing required columns: {missing}")

    output: list[dict[str, Any]] = []
    for row in source.loc[:, list(SOURCE_COLUMNS)].to_dict(orient="records"):
        tab_name = _trim_or_na(row["tabName"], 64)
        critical_value = row["criticalHighProfile"]
        critical_text = "" if _is_missing(critical_value) else str(critical_value).upper()
        output.append(
            {
                "submissionNumber": _trim_or_na(row["submissionNumber"], 64),
                "businessDomain": "PCD",
                "partyCode": _trim_or_na(row["companyCode"], 8),
                "recordStatus": "Active",
                "pcdCategory": _category_for_tab(row["tabName"]),
                "tabName": tab_name,
                "schemeLineDescription": _trim_or_na(row["lineDescription"], 2048),
                "versionNumber": _trim_or_na(row["versionInfo"], 64),
                "schemeId": _trim_or_na(row["schemeId"], 1024),
                "schemeType": _trim_or_na(row["typeOfScheme"], 64),
                "interimMilestoneMoveReason": _trim_or_na(row["imMoveReason"], 2048),
                "substitutedSchemeName": _trim_or_na(row["substitutedSchemeName"], 2048),
                "pcdReference": _trim_or_na(row["pcdRef"], 256),
                "eaId": _trim_or_na(row["eaId"], 256),
                "winepId": _trim_or_na(row["winepId"], 8000),
                "schemeName": _trim_or_na(row["schemeName"], 2048),
                "forecastToComplete": _sql_float_or_zero(row["forecastToComplete"]),
                "originalRegulatoryDate": _date_style_23(
                    row["originalRegulatoryDate"]
                ),
                "currentRegulatoryDate": _date_style_23(
                    row["currentRegulatoryDate"]
                ),
                "currentInterimMilestone": _trim_or_na(
                    row["currentInterimMilestone"], 256
                ),
                "spendToDate": _sql_float_or_zero(row["spendToDate"]),
                "forecastOutturnCost": _sql_float_or_zero(row["forecastOutturn"]),
                "percentageSpendToDate": _sql_float_or_zero(
                    row["percentSpendToDate"]
                ),
                "criticalHighProfile": (
                    critical_text.rstrip(" ") in {"1", "YES"}
                ),
                "schemeSecondaryType": _trim_or_na(row["schemeType"], 64),
                "assetManagementPeriod": _trim_or_na(row["amp"], 64),
                "submissionNote": _trim_or_na(row["submissionNotes"], 2048),
            }
        )

    return pd.DataFrame(output, columns=OUTPUT_COLUMNS)
