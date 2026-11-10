import pandas as pd


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
