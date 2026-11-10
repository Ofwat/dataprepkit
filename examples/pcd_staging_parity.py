"""Synthetic PCD landing inputs and rollback-safe SQL staging parity helpers.

Use these helpers from a notebook against a disposable/test SQL database. The
SQL runner invokes ``cde.sp_stg_tbl_pcd``, which replaces staging rows; it reads
the synthetic outputs and always rolls the transaction back afterward.
"""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime
import math
from numbers import Integral, Real
from uuid import uuid4

import pandas as pd
from sqlalchemy import text


STANDARD_COLUMNS = (
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
)

LIST_COLUMNS = (
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

MILESTONE_COLUMNS = tuple(
    f"{kind}Im{number}"
    for kind in ("baseline", "forecast")
    for number in range(1, 8)
)
EXPENDITURE_COLUMNS = tuple(
    f"{kind}{year}_{(year + 1) % 100:02d}"
    for kind in ("baseline", "forecast")
    for year in range(2023, 2030)
)

STAGING_TABLES = {
    "external_company_submission": "external_company_submission",
    "price_control_deliverable_output": "price_control_deliverable_output",
    "price_control_deliverable_expenditure": "price_control_deliverable_expenditure",
    "price_control_deliverable_scheme": "price_control_deliverable_scheme",
    "deliverable_scheme_detail": "deliverable_scheme_detail",
    "deliverable_scheme_milestone": "deliverable_scheme_milestone",
    "deliverable_scheme_expenditure": "deliverable_scheme_expenditure",
}

RUNTIME_DATE_NOTE = "__PARITY_RUNTIME_DATE__"


def load_eds_period_codes(engine) -> list[str]:
    """Read period codes, including duplicates, used by the SQL scheme join."""
    rows = pd.read_sql_query(
        text(
            "SELECT periodCode "
            "FROM eds.price_control_deliverable_period "
            "WHERE periodCode IS NOT NULL"
        ),
        engine,
    )
    return rows["periodCode"].tolist()


def _standard_row(submission_number: str, **updates) -> dict:
    row = {
        "submissionNumber": submission_number,
        "companyCode": "HDD",
        "dataCollection": "PCD",
        "worksheetName": "Parity test",
        "datapointPeriod": "2025-26",
        "dp_reference": "DP-PARITY",
        "datapointTypeCode": "PCD_TARGET",
        "datapointValue": "1",
        "DateTimeCreated": "10-11-2026",
        "pcdModel": "PCD",
        "pcdRef": "PCD-PARITY",
        "pcdApplicableToCompany": "Y",
        "pcdLineDescription": "Parity line",
        "pcdOutputDescription": "Parity output",
        "unitOfmeasure": "Ml/d",
        "decimalPlace": "1",
        "versionInfo": "PARITY-1",
        "submissionNotes": "Synthetic PCD parity input",
        "tabName": "UNUSED",
    }
    row.update(updates)
    return row


def _list_row(submission_number: str, scheme_id: str, **updates) -> dict:
    row = {
        "submissionNumber": submission_number,
        "companyCode": "HDD",
        "tabName": "DPW1",
        "lineDescription": "Parity scheme line",
        "versionInfo": "PARITY-1",
        "schemeId": scheme_id,
        "typeOfScheme": "Primary",
        "imMoveReason": "Parity reason",
        "substitutedSchemeName": "Substitute scheme",
        "pcdRef": "PCD-PARITY",
        "eaId": "EA-PARITY",
        "winepId": "WINEP-PARITY",
        "schemeName": f"Scheme {scheme_id}",
        "forecastToComplete": "10.5",
        "originalRegulatoryDate": "2024-01-02",
        "currentRegulatoryDate": "2025-02-03",
        "currentInterimMilestone": "IM2",
        "spendToDate": "5.25",
        "forecastOutturn": "20.75",
        "percentSpendToDate": "25.5",
        "criticalHighProfile": "YES",
        "schemeType": "Secondary",
        "amp": "AMP8",
        "submissionNotes": "Synthetic PCD parity input",
    }
    for index, column in enumerate(MILESTONE_COLUMNS, start=1):
        year = 2024 + (index - 1) // 2
        row[column] = f"{year}-06-15"
    row["forecastIm7"] = "not-a-date"
    for index, column in enumerate(EXPENDITURE_COLUMNS, start=1):
        row[column] = str(index + 0.5)
    row["baseline2029_30"] = "not-a-number"
    row["forecast2029_30"] = None
    row.update(updates)
    return row


def make_pcd_parity_inputs(
    matched_period_code: str,
    *,
    submission_number: str | None = None,
) -> tuple[str, pd.DataFrame, pd.DataFrame]:
    """Build standard and list landing frames covering PCD staging branches.

    ``matched_period_code`` must be a value currently present in
    ``eds.price_control_deliverable_period``. The generated frames share a
    unique submission number so SQL staging rows can be selected without
    including unrelated landing data.
    """
    if not matched_period_code:
        raise ValueError("A non-empty EDS period code is required for the matched lookup branch.")
    marker = submission_number or f"PCD_PARITY_{uuid4().hex[:12].upper()}"

    standard_rows = []

    # Submission projection: exact duplicate metadata, a changed metadata row,
    # and a NULL source-created date to exercise the runtime default.
    metadata_row = _standard_row(
        marker,
        tabName="IGNORED_FOR_BUSINESS_OUTPUTS",
        dp_reference="META-DUP",
        datapointTypeCode="XCD_METADATA",
        datapointValue="0",
    )
    standard_rows.extend([metadata_row.copy(), metadata_row.copy()])
    standard_rows.append(
        _standard_row(
            marker,
            tabName="IGNORED_FOR_BUSINESS_OUTPUTS",
            worksheetName="Alternate worksheet",
            versionInfo="PARITY-2",
            submissionNotes="Alternate submission metadata",
            DateTimeCreated="11-11-2026",
            dp_reference="META-ALT",
            datapointTypeCode="XCD_METADATA",
            datapointValue="0",
        )
    )
    standard_rows.append(
        _standard_row(
            marker,
            tabName="IGNORED_FOR_BUSINESS_OUTPUTS",
            worksheetName="Runtime date worksheet",
            submissionNotes=RUNTIME_DATE_NOTE,
            DateTimeCreated=None,
            dp_reference="META-RUNTIME-DATE",
            datapointTypeCode="XCD_METADATA",
            datapointValue="0",
        )
    )

    def add_standard(tab, code, value, *, period="2025-26", reference=None, **updates):
        standard_rows.append(
            _standard_row(
                marker,
                tabName=tab,
                datapointTypeCode=code,
                datapointValue=value,
                datapointPeriod=period,
                dp_reference=reference or f"DP-{len(standard_rows):03d}",
                **updates,
            )
        )

    # Output procedure: each accepted tab/category, target/outturn suffixes,
    # RAG routing, applicability values, invalid numeric data, and grouping.
    add_standard("DPB1", "PCD_TARGET", "10", reference="OUT-SUM")
    add_standard("DPB1", "PCD_TARGET", "2", reference="OUT-SUM")
    add_standard("DPB1", "PCD_OUT_O", "3", reference="OUT-O")
    add_standard("BRL_DPB1_ADD", "PCD_OUT_OT", "4", reference="OUT-OT", pcdApplicableToCompany="1")
    add_standard("DPWW1", "PCD_TARGET", "5", reference="OUT-WW", pcdApplicableToCompany="N")
    add_standard("DPW1", "XCD_RAG", "Green", period="RAG RATING - COMPANY - 2025", reference="OUT-RAG-COMPANY", pcdApplicableToCompany="0")
    add_standard("DPW1", "PCD_RAG", "Amber", period="RAG RATING - ASSURER - 2025", reference="OUT-RAG-ASSURER", pcdApplicableToCompany=None)
    add_standard("DPW1", "PCD_BAD_NUMBER", "not-a-number", reference="OUT-BAD-NUMBER", pcdApplicableToCompany="other", decimalPlace="bad-int")
    add_standard(
        "DPB1", "PCD_NULL_FIELDS", None, reference="OUT-NULL-FIELDS",
        dataCollection=None, worksheetName=None, datapointPeriod=None,
        companyCode=None, pcdRef=None,
        pcdOutputDescription=None, pcdLineDescription=None, unitOfmeasure=None,
        decimalPlace=None, versionInfo=None, submissionNotes=None,
        pcdApplicableToCompany=None,
    )
    add_standard("DPB2", "PCD_NOT_SELECTED", "8", reference="OUT-EXCLUDED-TAB")
    add_standard(" DPB1", "PCD_NOT_SELECTED", "8", reference="OUT-LEADING-SPACE")

    # Expenditure procedure: accepted tabs, all measure suffix routes, category
    # cases, duplicate aggregation, bad prefixes/tabs, and padded tab equality.
    add_standard("DPB2", "PCD_ALLOWANCE", "5", reference="EXP-SUM")
    add_standard("DPB2", "PCD_ALLOWANCE", "7", reference="EXP-SUM")
    add_standard("DPB2", "PCD_NDP_NP", "2", reference="EXP-NP")
    add_standard("DPB2", "PCD_OUT_O", "3", reference="EXP-O")
    add_standard("DPB2", "PCD_OUT_OT", "4", reference="EXP-OT")
    add_standard("DPB2", "PCD_BAD_NUMBER", "not-a-number", reference="EXP-BAD-NUMBER")
    add_standard("BRL_DPB2_ADD", "PCD_ALLOWANCE", "6", reference="EXP-BRL")
    add_standard("DPWW2", "PCD_ALLOWANCE", "8", reference="EXP-WW")
    add_standard("DPW2", "PCD_ALLOWANCE", "9", reference="EXP-WATER")
    add_standard("DPB2   ", "PCD_PADDED_TAB", "1", reference="EXP-TRAILING-SPACE")
    add_standard(" DPB2", "PCD_LEADING_SPACE", "1", reference="EXP-LEADING-SPACE")
    add_standard("DPW2", "XCD_NOT_PCD", "1", reference="EXP-BAD-PREFIX")
    add_standard("DPW2", " PCD_LEADING_CODE_SPACE", "1", reference="EXP-BAD-PREFIX-SPACE")
    add_standard("DPB1", "PCD_NOT_SELECTED", "1", reference="EXP-EXCLUDED-TAB")

    # Scheme procedure: the period join's matched/unmatched cases, all delay
    # reason labels, delayed/non-delayed codes, and the reachable tab cases.
    delay_suffixes = (
        "DIR", "DSCR", "DCS", "DSCS", "DPP", "DTPI", "DCI", "DSI",
        "DER", "RS", "DPLE",
    )
    for suffix in delay_suffixes:
        reference = f"SCHEME-{suffix}"
        add_standard("DPW3", f"PCD_DLY_{suffix}", "2", period=matched_period_code, reference=reference)
        if suffix == "DIR":
            add_standard("DPW3", f"PCD_DLY_{suffix}", "2", period=matched_period_code, reference=reference)
    add_standard("DPW3", "PCD_TOTAL", "3", period=matched_period_code, reference="SCHEME-NO-DELAY")
    add_standard("DPW3", "PCD_DLY_OTHER", "2", period=matched_period_code, reference="SCHEME-DLY-OTHER")
    add_standard("DPW3", "PCD_DLY_DIR", "4", period=f"{marker}-NO-EDS-MATCH", reference="SCHEME-NO-MATCH")
    add_standard("DPW3", "PCD_DIR", "4", period=matched_period_code, reference="SCHEME-REASON-WITHOUT-DLY")
    add_standard("DPB3", "PCD_TOTAL", "1", period=matched_period_code, reference="SCHEME-BASE-TAB")
    add_standard("BRL_DPB3_ADD", "PCD_TOTAL", "1", period=matched_period_code, reference="SCHEME-BRL-TAB")
    add_standard("DPWW3", "PCD_TOTAL", "1", period=matched_period_code, reference="SCHEME-WW-TAB")
    add_standard("DPW3   ", "PCD_TOTAL", "1", period=matched_period_code, reference="SCHEME-TRAILING-SPACE")
    add_standard(" DPW3", "PCD_TOTAL", "1", period=matched_period_code, reference="SCHEME-LEADING-SPACE")
    add_standard("DPW3", "XCD_DLY_DIR", "1", period=matched_period_code, reference="SCHEME-BAD-PREFIX")
    add_standard("DPW1", "PCD_DLY_DIR", "1", period=matched_period_code, reference="SCHEME-EXCLUDED-TAB")

    standard = pd.DataFrame(standard_rows, columns=STANDARD_COLUMNS)

    # Scheme Detail: category mapping, valid/invalid casts, dates, booleans,
    # plus populated and invalid values in each milestone/expenditure period.
    detail_rows = [
        _list_row(marker, "SCHEME-BASE", tabName="DPB1", criticalHighProfile="YES"),
        _list_row(
            marker,
            "SCHEME-BASE-2",
            tabName="DPB2",
            forecastToComplete="invalid",
            originalRegulatoryDate="not-a-date",
            currentRegulatoryDate="2025/02/03",
            spendToDate=None,
            forecastOutturn="invalid",
            percentSpendToDate="",
            criticalHighProfile="1",
        ),
        _list_row(marker, "SCHEME-WW", tabName="DPWW5", criticalHighProfile="yes"),
        _list_row(marker, "SCHEME-WATER", tabName="DPW5", criticalHighProfile=" yes "),
        _list_row(marker, "SCHEME-UNMAPPED", tabName="DPB1_ADD", criticalHighProfile="TRUE"),
        _list_row(marker, "SCHEME-UNKNOWN", tabName="UNKNOWN", originalRegulatoryDate="invalid"),
        _list_row(
            marker,
            "SCHEME-NULLS",
            tabName=None,
            lineDescription=None,
            versionInfo=None,
            typeOfScheme=None,
            imMoveReason=None,
            substitutedSchemeName=None,
            pcdRef=None,
            eaId=None,
            winepId=None,
            schemeName=None,
            forecastToComplete=None,
            originalRegulatoryDate=None,
            currentRegulatoryDate=None,
            currentInterimMilestone=None,
            spendToDate=None,
            forecastOutturn=None,
            percentSpendToDate=None,
            criticalHighProfile=None,
            schemeType=None,
            amp=None,
            submissionNotes=None,
        ),
    ]
    detail = pd.DataFrame(
        detail_rows,
        columns=(*LIST_COLUMNS, *MILESTONE_COLUMNS, *EXPENDITURE_COLUMNS),
    )
    return marker, standard, detail


def run_sql_staging_in_rollback_transaction(
    engine,
    standard_landing: pd.DataFrame,
    scheme_detail_landing: pd.DataFrame,
    *,
    submission_number: str,
    expected_frames: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """Run ``cde.sp_stg_tbl_pcd`` and capture fixture rows, then roll back.

    The stored procedure deletes and repopulates staging tables and writes
    landing archives. Use a disposable/test database. This helper places all
    writes in one SQL transaction and always rolls them back after reading the
    staged results.
    """
    if not submission_number:
        raise ValueError("submission_number must be non-empty for output filtering.")
    if set(expected_frames) != set(STAGING_TABLES):
        raise ValueError("expected_frames must contain all seven PCD staging outputs.")
    for name, frame in (
        ("standard_landing", standard_landing),
        ("scheme_detail_landing", scheme_detail_landing),
    ):
        if "submissionNumber" not in frame:
            raise ValueError(f"{name} must include submissionNumber.")
        values = set(frame["submissionNumber"].dropna().astype(str))
        if values != {submission_number}:
            raise ValueError(f"All {name} rows must use {submission_number!r}.")

    required_tables = (
        "lnd.price_control_deliverable",
        "lnd.price_control_deliverable_scheme_detail",
        *(f"stg.{table}" for table in STAGING_TABLES.values()),
        "etl.batch_table_run",
    )
    with engine.connect() as connection:
        for table_name in required_tables:
            exists = connection.execute(
                text("SELECT OBJECT_ID(:table_name, 'U')"),
                {"table_name": table_name},
            ).scalar_one()
            if exists is None:
                connection.rollback()
                raise RuntimeError(f"Required SQL table does not exist: {table_name}")
        connection.rollback()

        transaction = connection.begin()
        try:
            standard_landing.to_sql(
                "price_control_deliverable",
                connection,
                schema="lnd",
                if_exists="append",
                index=False,
            )
            scheme_detail_landing.to_sql(
                "price_control_deliverable_scheme_detail",
                connection,
                schema="lnd",
                if_exists="append",
                index=False,
            )

            result = connection.exec_driver_sql("EXEC cde.sp_stg_tbl_pcd")
            result.close()

            captured = {}
            for frame_name, table_name in STAGING_TABLES.items():
                expected_columns = list(expected_frames[frame_name].columns)
                columns_sql = ", ".join(f"[{column}]" for column in expected_columns)
                query = text(
                    f"SELECT {columns_sql} FROM [stg].[{table_name}] "
                    "WHERE [submissionNumber] = :submission_number"
                )
                captured[frame_name] = pd.read_sql_query(
                    query,
                    connection,
                    params={"submission_number": submission_number},
                )
            transaction.rollback()
            return captured
        except Exception:
            if transaction.is_active:
                transaction.rollback()
            raise



def _canonical_value(value, *, volatile_created_date: bool = False):
    if pd.isna(value):
        return None
    if volatile_created_date:
        return "<SQL/PYTHON RUNTIME DEFAULT>"
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool) or type(value).__name__ == "bool_":
        return int(value)
    if isinstance(value, Integral):
        return int(value)
    if isinstance(value, Real):
        number = float(value)
        return 0.0 if math.isclose(number, 0.0, abs_tol=1e-12) else round(number, 10)
    return value


def compare_pcd_staging_frames(
    expected: dict[str, pd.DataFrame],
    actual: dict[str, pd.DataFrame],
) -> dict[str, dict]:
    """Compare Python and SQL business columns, ignoring generated metadata.

    For the synthetic row marked by ``RUNTIME_DATE_NOTE``, source
    ``DateTimeCreated`` is NULL and both systems substitute their own current
    timestamp. That single value is checked for non-nullness, then normalized
    before comparing; all explicitly supplied timestamps are compared normally.
    """
    results = {}
    for table_name, expected_frame in expected.items():
        actual_frame = actual[table_name]
        columns = list(expected_frame.columns)
        if list(actual_frame.columns) != columns:
            results[table_name] = {
                "matches": False,
                "reason": "business column lists differ",
                "python_columns": columns,
                "sql_columns": list(actual_frame.columns),
            }
            continue

        def rows_as_counter(frame):
            rows = []
            for record in frame.to_dict(orient="records"):
                note = record.get("submissionNote")
                runtime_created_date = (
                    table_name == "external_company_submission"
                    and note == RUNTIME_DATE_NOTE
                )
                if runtime_created_date and pd.isna(record.get("createdDate")):
                    raise AssertionError("Runtime-default createdDate was unexpectedly NULL.")
                rows.append(
                    tuple(
                        _canonical_value(
                            record[column],
                            volatile_created_date=(
                                runtime_created_date and column == "createdDate"
                            ),
                        )
                        for column in columns
                    )
                )
            return Counter(rows)

        python_rows = rows_as_counter(expected_frame)
        sql_rows = rows_as_counter(actual_frame)
        python_only = list((python_rows - sql_rows).elements())
        sql_only = list((sql_rows - python_rows).elements())
        results[table_name] = {
            "matches": not python_only and not sql_only,
            "python_rows": sum(python_rows.values()),
            "sql_rows": sum(sql_rows.values()),
            "python_only_examples": python_only[:5],
            "sql_only_examples": sql_only[:5],
        }
    return results
