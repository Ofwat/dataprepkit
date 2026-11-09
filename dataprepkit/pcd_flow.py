"""Frame based implementation of the PCD landing, staging and SCD2 flow.

Landing and staging frames contain only business columns.  Fabric-specific
ingestion, batch bookkeeping and landing archive writes remain the caller's
responsibility; ``run_dimension`` owns SCD2 persistence.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import math
from typing import Any, Iterable, Mapping

import pandas as pd

from dataprepkit.metadata_loader import DimensionMetadata, run_dimension
from dataprepkit.pcd_output_transform import (
    transform_price_control_deliverable_output,
)
from dataprepkit.pcd_scheme_detail_transform import (
    transform_deliverable_scheme_detail,
)


_STANDARD_COLUMNS = (
    "submissionNumber", "companyCode", "dataCollection", "worksheetName",
    "datapointPeriod", "dp_reference", "datapointTypeCode", "datapointValue",
    "DateTimeCreated", "pcdModel", "pcdRef", "pcdApplicableToCompany",
    "pcdLineDescription", "pcdOutputDescription", "unitOfmeasure",
    "decimalPlace", "versionInfo", "submissionNotes", "tabName",
)
_LIST_COLUMNS = (
    "submissionNumber", "companyCode", "tabName", "lineDescription",
    "versionInfo", "schemeId", "typeOfScheme", "imMoveReason",
    "substitutedSchemeName", "pcdRef", "eaId", "winepId", "schemeName",
    "forecastToComplete", "originalRegulatoryDate", "currentRegulatoryDate",
    "currentInterimMilestone", "spendToDate", "forecastOutturn",
    "percentSpendToDate", "criticalHighProfile", "schemeType", "amp",
    "submissionNotes",
)
_MILESTONE_COLUMNS = tuple(
    f"{kind}Im{i}" for kind in ("baseline", "forecast") for i in range(1, 8)
)
_EXPENDITURE_COLUMNS = tuple(
    f"{kind}{year}_{(year + 1) % 100:02d}"
    for kind in ("baseline", "forecast")
    for year in range(2023, 2030)
)
_DELAY_REASONS = (
    ("_DIR", "Lack of internal resourcing"),
    ("_DSCR", "Lack of supply chain resourcing"),
    ("_DCS", "Change in solution (IM2)"),
    ("_DSCS", "Supply chain capacity"),
    ("_DPP", "Land and planning permission"),
    ("_DTPI", "Third-party interface"),
    ("_DCI", "Contractual issues"),
    ("_DSI", "Sites issues"),
    ("_DER", "Environmental risks"),
    ("_RS", "Regulatory Sign off Delay"),
    ("_DPLE", "Programme level efficiency"),
)
_SCHEME_INPUTS = _STANDARD_COLUMNS
_EXPENDITURE_INPUTS = _STANDARD_COLUMNS


class PcdDependencyError(ValueError):
    """A required staging or lookup input was not supplied."""


@dataclass(frozen=True)
class PcdDimension:
    metadata: DimensionMetadata
    subject_area: str
    table_status: str
    staging_table: str


@dataclass(frozen=True)
class PcdStagingBatch:
    frames: Mapping[str, pd.DataFrame]


@dataclass(frozen=True)
class PcdRunResult:
    staging: PcdStagingBatch
    loaded_tables: tuple[str, ...]
    load_results: Mapping[str, Any]
    skipped_tables: Mapping[str, str]


def _require_columns(frame: pd.DataFrame, required: Iterable[str], label: str) -> None:
    if not isinstance(frame, pd.DataFrame):
        raise TypeError(f"{label} must be a pandas DataFrame")
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"{label} is missing required columns: {', '.join(missing)}")


def _missing(value: Any) -> bool:
    return value is None or (not isinstance(value, (list, dict)) and bool(pd.isna(value)))


def _text(value: Any, length: int, default: str = "N/A") -> str:
    return default if _missing(value) else str(value).strip(" ")[:length]


def _number(value: Any, default: float = 0.0) -> float:
    if _missing(value):
        return default
    try:
        number = float(str(value).strip())
    except (ValueError, TypeError, OverflowError):
        return default
    return number if math.isfinite(number) else default


def _try_integer(value: Any, null_default: int | None = 0) -> int | None:
    if _missing(value):
        return null_default
    try:
        return int(str(value).strip())
    except (ValueError, TypeError, OverflowError):
        return None


def _date23(value: Any) -> date | None:
    if _missing(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value).strip()[:10])
    except ValueError:
        return None


def _datetime105(value: Any) -> datetime | None:
    """Parse the common style-105 landing representation and ISO timestamps."""
    if _missing(value):
        return None
    if isinstance(value, datetime):
        return value.replace(microsecond=0)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    raw = str(value).strip()
    for fmt in (
        "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M", "%d-%m-%Y",
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def _group_sum(frame: pd.DataFrame, keys: list[str], measures: list[str]) -> pd.DataFrame:
    if frame.empty:
        return frame.loc[:, keys + measures].copy()
    return frame.groupby(keys, dropna=False, sort=False, as_index=False)[measures].sum()


def transform_pcd_external_company_submission(incoming: pd.DataFrame) -> pd.DataFrame:
    """Build distinct PCD submission rows from standard landing metadata.

    Mirrors the submission projection and deduplication in
    ``cde.sp_stg_price_control_deliverable``.
    """
    _require_columns(incoming, _STANDARD_COLUMNS, "standard_landing")
    rows = []
    null_created_default = datetime.now().replace(microsecond=0)
    for row in incoming.to_dict("records"):
        created = null_created_default if _missing(row["DateTimeCreated"]) else row["DateTimeCreated"]
        rows.append({
            "submissionNumber": _text(row["submissionNumber"], 64),
            "businessDomain": "PCD",
            "partyCode": _text(row["companyCode"], 8),
            "pcdModel": _text(row["dataCollection"], 64),
            "worksheetName": _text(row["worksheetName"], 64),
            "_sourceCreatedDate": created,
            "recordStatus": "Active",
            "versionNumber": _text(row["versionInfo"], 64),
            "submissionNote": _text(row["submissionNotes"], 2048),
        })
    output = pd.DataFrame(rows)
    if output.empty:
        return pd.DataFrame(columns=["submissionNumber", "businessDomain", "partyCode", "pcdModel", "worksheetName", "createdDate", "recordStatus", "versionNumber", "submissionNote"])
    # The procedure groups on the original datetime value, then converts it.
    output = output.drop_duplicates(ignore_index=True)
    output = output.rename(columns={"_sourceCreatedDate": "createdDate"})
    output["createdDate"] = output["createdDate"].map(_datetime105)
    return output


def transform_pcd_expenditure(incoming: pd.DataFrame) -> pd.DataFrame:
    """Filter and aggregate PCD expenditure datapoints from standard landing.

    Mirrors the tab/code filters, category and measure mapping, and grouping in
    ``cde.sp_stg_price_control_deliverable_expenditure``.
    """
    _require_columns(incoming, _EXPENDITURE_INPUTS, "standard_landing")
    rows = []
    for row in incoming.to_dict("records"):
        raw_tab = row["tabName"]
        raw_code = row["datapointTypeCode"]
        # The SP filters the untrimmed tab and the first three raw code chars.
        # SQL string equality ignores trailing spaces, but not leading spaces.
        if _missing(raw_tab) or str(raw_tab).rstrip(" ") not in {"DPB2", "BRL_DPB2_ADD", "DPWW2", "DPW2"}:
            continue
        if _missing(raw_code) or str(raw_code)[:3] != "PCD":
            continue
        tab = _text(raw_tab, 64)
        code = _text(raw_code, 256)
        numeric = _number(row["datapointValue"])
        # LIKE in the procedure sees the original value; trim only for its
        # separately projected bonCode column.
        upper = str(raw_code).upper()
        np_value = numeric if upper.endswith("_NP") else 0.0
        outturn = numeric if upper.endswith(("_O", "_OT")) else 0.0
        allowance = numeric if not upper.endswith(("_O", "_OT", "_NP")) else 0.0
        tab_upper = tab.upper()
        category = "Base" if tab_upper in {"DPB2", "DPB1_ADD", "DPB2_ADD"} else "Wastewater" if tab_upper in {"DPWW1", "DPWW2", "DPWW3", "DPWW4", "DPWW5"} else "Water" if tab_upper in {"DPW1", "DPW2", "DPW3", "DPW4", "DPW5"} else "N/A"
        raw_applicable = row["pcdApplicableToCompany"]
        applicable = (
            not _missing(raw_applicable)
            and str(raw_applicable).rstrip(" ").upper() in {"Y", "1"}
        )
        rows.append({
            "submissionNumber": _text(row["submissionNumber"], 64), "businessDomain": "PCD",
            "partyCode": _text(row["companyCode"], 8), "periodCode": _text(row["datapointPeriod"], 256),
            "deliveryPlanReference": _text(row["dp_reference"], 32),
            "pcdOutputDescription": _text(row["pcdOutputDescription"], 2048),
            "pcdLineDescription": _text(row["pcdLineDescription"], 2048), "pcdCategory": category,
            "tabName": _text(row["tabName"], 64), "pcdReference": _text(row["pcdRef"], 256),
            "pcdApplicableToCompany": int(applicable), "nonDeliveryPayment": np_value,
            "cumulativeExpenditureAllowance": allowance,
            "cumulativeExpenditureOutturnForecast": outturn,
            "unitOfmeasure": _text(row["unitOfmeasure"], 256), "decimalPlace": _try_integer(row["decimalPlace"]),
            "recordStatus": "Active", "versionNumber": _text(row["versionInfo"], 64),
            "submissionNote": _text(row["submissionNotes"], 2048),
            "columnHeader": _text(row["datapointPeriod"], 256), "bonCode": code,
        })
    output_columns = ["submissionNumber", "businessDomain", "partyCode", "periodCode", "deliveryPlanReference", "pcdOutputDescription", "pcdLineDescription", "pcdCategory", "tabName", "pcdReference", "pcdApplicableToCompany", "unitOfmeasure", "decimalPlace", "recordStatus", "versionNumber", "submissionNote", "columnHeader", "bonCode", "nonDeliveryPayment", "cumulativeExpenditureAllowance", "cumulativeExpenditureOutturnForecast"]
    frame = pd.DataFrame(rows, columns=output_columns)
    keys = output_columns[:-3]
    return _group_sum(frame, keys, output_columns[-3:]).loc[:, output_columns]


def transform_pcd_scheme(incoming: pd.DataFrame, *, period_codes: Iterable[str]) -> pd.DataFrame:
    """Filter, classify, and aggregate PCD scheme datapoints.

    Uses the supplied EDS period codes for the scheme-count lookup and mirrors
    the staging rules in ``cde.sp_stg_price_control_deliverable_scheme``.
    """
    _require_columns(incoming, _SCHEME_INPUTS, "standard_landing")
    period_counts: dict[Any, int] = {}
    for period_code in period_codes:
        if not _missing(period_code):
            period_counts[period_code] = period_counts.get(period_code, 0) + 1
    delay_suffixes = tuple(suffix for suffix, _ in _DELAY_REASONS)
    rows = []
    for row in incoming.to_dict("records"):
        raw_tab = row["tabName"]
        if _missing(raw_tab) or str(raw_tab).rstrip(" ") not in {
            "DPB3", "BRL_DPB3_ADD", "DPWW3", "DPW3"
        }:
            continue
        tab = _text(raw_tab, 64)
        code = _text(row["datapointTypeCode"], 256)
        if code[:3].strip() != "PCD":
            continue
        raw_period = row["datapointPeriod"]
        period = _text(raw_period, 256)
        value = _number(row["datapointValue"])
        upper_code = str(row["datapointTypeCode"]).upper()
        total_delayed = int(value) if "_DLY" in upper_code else 0
        delay_type = next((label for suffix, label in _DELAY_REASONS if suffix in upper_code), "N/A")
        delayed = int(value) if any(suffix in upper_code for suffix in delay_suffixes) else 0
        tab_upper = tab.upper()
        category = "Base" if tab_upper in {"DPB1", "DPB2", "DPB1_ADD", "DPB2_ADD"} else "Wastewater" if tab_upper in {"DPWW1", "DPWW2", "DPWW3", "DPWW4", "DPWW5"} else "Water" if tab_upper in {"DPW1", "DPW2", "DPW3", "DPW4", "DPW5"} else "N/A"
        lookup_matches = (
            0 if _missing(raw_period) else period_counts.get(raw_period, 0)
        )
        row_output = {
            "submissionNumber": _text(row["submissionNumber"], 64), "businessDomain": "PCD",
            "partyCode": _text(row["companyCode"], 8), "periodCode": period,
            "deliveryPlanReference": _text(row["dp_reference"], 32),
            "pcdOutputDescription": _text(row["pcdOutputDescription"], 2048),
            "pcdLineDescription": _text(row["pcdLineDescription"], 2048), "pcdCategory": category,
            "tabName": _text(row["tabName"], 64), "pcdReference": _text(row["pcdRef"], 256),
            "pcdApplicableToCompany": int(
                not _missing(row["pcdApplicableToCompany"])
                and str(row["pcdApplicableToCompany"]).rstrip(" ").upper() in {"Y", "1"}
            ),
            "numberOfSchemes": int(value) if lookup_matches else 0,
            "totalDelayedSchemes": total_delayed, "delayedSchemes": delayed,
            "delayType": delay_type, "unitOfmeasure": _text(row["unitOfmeasure"], 256),
            "decimalPlace": _try_integer(row["decimalPlace"]), "recordStatus": "Active",
            "versionNumber": _text(row["versionInfo"], 64), "submissionNote": _text(row["submissionNotes"], 2048),
            "columnHeader": period, "bonCode": code,
        }
        # The SP uses a LEFT JOIN, so a missing code yields one row while
        # duplicate lookup codes multiply the source row before aggregation.
        for _ in range(max(lookup_matches, 1)):
            rows.append(row_output.copy())
    columns = ["submissionNumber", "businessDomain", "partyCode", "periodCode", "deliveryPlanReference", "pcdOutputDescription", "pcdLineDescription", "pcdCategory", "tabName", "pcdReference", "pcdApplicableToCompany", "numberOfSchemes", "delayType", "unitOfmeasure", "decimalPlace", "recordStatus", "versionNumber", "submissionNote", "columnHeader", "bonCode", "totalDelayedSchemes", "delayedSchemes"]
    frame = pd.DataFrame(rows, columns=columns)
    keys = columns[:-2]
    return _group_sum(frame, keys, columns[-2:]).loc[:, columns]


def transform_deliverable_scheme_milestone(incoming: pd.DataFrame) -> pd.DataFrame:
    """Unpivot baseline and forecast milestone dates into typed rows.

    Produces 14 milestone rows per landing row, following
    ``cde.sp_stg_deliverable_scheme_milestone``.
    """
    required = ("submissionNumber", "companyCode", "schemeId", *_MILESTONE_COLUMNS)
    _require_columns(incoming, required, "list_landing")
    rows = []
    for row in incoming.to_dict("records"):
        for column in _MILESTONE_COLUMNS:
            rows.append({
                "submissionNumber": row["submissionNumber"], "businessDomain": "PCD",
                "partyCode": _text(row["companyCode"], 8), "recordStatus": "Active",
                "schemeId": _text(row["schemeId"], 1024), "milestoneDate": _date23(row[column]),
                "periodCode": column, "milestoneDetailType": "Baseline" if column.startswith("baseline") else "Forecast",
            })
    return pd.DataFrame(rows, columns=["submissionNumber", "businessDomain", "partyCode", "recordStatus", "schemeId", "milestoneDate", "periodCode", "milestoneDetailType"])


def transform_deliverable_scheme_expenditure(incoming: pd.DataFrame) -> pd.DataFrame:
    """Unpivot baseline and forecast expenditure columns into period rows.

    Produces 14 rows per landing row with normalized period and milestone type,
    following ``cde.sp_stg_deliverable_scheme_expenditure``.
    """
    required = ("submissionNumber", "companyCode", "schemeId", *_EXPENDITURE_COLUMNS)
    _require_columns(incoming, required, "list_landing")
    rows = []
    for row in incoming.to_dict("records"):
        for column in _EXPENDITURE_COLUMNS:
            rows.append({
                "submissionNumber": row["submissionNumber"], "businessDomain": "PCD",
                "partyCode": _text(row["companyCode"], 8), "recordStatus": "Active",
                "schemeId": _text(row["schemeId"], 1024), "milestoneExpenditure": _number(row[column]),
                "periodCode": column[-7:].replace("_", "-"),
                "milestoneDetailType": "Baseline" if column.startswith("baseline") else "Forecast",
            })
    return pd.DataFrame(rows, columns=["submissionNumber", "businessDomain", "partyCode", "recordStatus", "schemeId", "milestoneExpenditure", "periodCode", "milestoneDetailType"])


def stage_pcd_landing(
    standard_landing: pd.DataFrame,
    list_landing: pd.DataFrame,
    *,
    period_codes: Iterable[str],
) -> PcdStagingBatch:
    """Build all PCD business staging frames from standard and list landing.

    This is the Python counterpart to ``cde.sp_stg_tbl_pcd``. It delegates
    each output to a dedicated transform that names its corresponding staging
    procedure.
    """
    _require_columns(standard_landing, _STANDARD_COLUMNS, "standard_landing")
    _require_columns(list_landing, (*_LIST_COLUMNS, *_MILESTONE_COLUMNS, *_EXPENDITURE_COLUMNS), "list_landing")
    frames = {
        "external_company_submission": transform_pcd_external_company_submission(standard_landing),
        "price_control_deliverable_output": transform_price_control_deliverable_output(standard_landing),
        "price_control_deliverable_expenditure": transform_pcd_expenditure(standard_landing),
        "price_control_deliverable_scheme": transform_pcd_scheme(standard_landing, period_codes=period_codes),
        "deliverable_scheme_detail": transform_deliverable_scheme_detail(list_landing),
        "deliverable_scheme_milestone": transform_deliverable_scheme_milestone(list_landing),
        "deliverable_scheme_expenditure": transform_deliverable_scheme_expenditure(list_landing),
    }
    return PcdStagingBatch(frames=frames)


def apply_pcd_scd2(
    engine: Any,
    staging: PcdStagingBatch,
    *,
    dimensions: Iterable[PcdDimension],
    gen_staging: Mapping[str, pd.DataFrame] | None = None,
) -> PcdRunResult:
    """Validate dependencies, then apply SCD2 to the selected PCD and GEN tables.

    This implements the metadata-selected behavior corresponding to
    ``cde.sp_run_scd2_etl``. Every dependency is validated before the first
    database mutation. PCD frames already contain the staging transforms;
    callers provide GEN frames from their corresponding staging nodes.
    """
    gen_staging = gen_staging or {}
    selected: list[PcdDimension] = []
    skipped: dict[str, str] = {}
    for definition in dimensions:
        name = definition.metadata.name
        if definition.table_status.casefold() != "implemented":
            skipped[name] = "table status is not Implemented"
        elif definition.subject_area.upper() not in {"PCD", "GEN"}:
            skipped[name] = "subject area is not PCD or GEN"
        else:
            selected.append(definition)

    inputs: dict[str, pd.DataFrame] = {}
    for definition in selected:
        table = definition.staging_table
        if definition.subject_area.upper() == "PCD":
            frame = staging.frames.get(table)
        else:
            frame = staging.frames.get(table, gen_staging.get(table))
        if frame is None:
            raise PcdDependencyError(f"Missing staging dependency for {definition.metadata.name}: {table}")
        inputs[definition.metadata.name] = frame

    results: dict[str, Any] = {}
    for definition in selected:
        original = definition.metadata
        load_metadata = original.model_copy(update={
            "processing_class": None,
            "dependencies": [],
            "column_renames": {},
        })
        results[original.name] = run_dimension(
            engine,
            original.name,
            metadata_registry={original.name: load_metadata},
            override_df=inputs[original.name],
        )
    return PcdRunResult(
        staging=staging,
        loaded_tables=tuple(results),
        load_results=results,
        skipped_tables=skipped,
    )


def run_pcd_flow(
    engine: Any,
    standard_landing: pd.DataFrame,
    list_landing: pd.DataFrame,
    *,
    period_codes: Iterable[str],
    dimensions: Iterable[PcdDimension],
    gen_staging: Mapping[str, pd.DataFrame] | None = None,
) -> PcdRunResult:
    """Run the PCD landing-to-SCD2 route represented by ``cde.sp_load_eds``.

    Builds the seven staging frames, then loads the selected dimensions with
    SCD2. It composes ``cde.sp_stg_tbl_pcd`` and the configured PCD branch of
    ``cde.sp_run_scd2_etl``.
    """
    staging = stage_pcd_landing(standard_landing, list_landing, period_codes=period_codes)
    return apply_pcd_scd2(engine, staging, dimensions=dimensions, gen_staging=gen_staging)
