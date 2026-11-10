import pandas as pd

def _standard_row(**changes):
    row = {
        "submissionNumber": "5",
        "companyCode": "HDD",
        "dataCollection": "PCD",
        "worksheetName": "Submission",
        "datapointPeriod": "2025-26",
        "dp_reference": "DP-1",
        "datapointTypeCode": "PCD_TARGET",
        "datapointValue": "12.5",
        "DateTimeCreated": "01-02-2025 10:11:12",
        "pcdModel": "unused",
        "pcdRef": "REF-1",
        "pcdApplicableToCompany": "Y",
        "pcdLineDescription": "Line",
        "pcdOutputDescription": "Output",
        "unitOfmeasure": "Ml/d",
        "decimalPlace": "1",
        "versionInfo": "3",
        "submissionNotes": "Note",
        "tabName": "DPW3",
    }
    row.update(changes)
    return row


def _scheme_row(**changes):
    row = {
        "submissionNumber": "5",
        "companyCode": "HDD",
        "tabName": "DPW1",
        "lineDescription": "Line",
        "versionInfo": "3",
        "schemeId": "S-1",
        "typeOfScheme": "Primary",
        "imMoveReason": "Reason",
        "substitutedSchemeName": "Substitute",
        "pcdRef": "REF-1",
        "eaId": "EA-1",
        "winepId": "WINEP-1",
        "schemeName": "Scheme",
        "forecastToComplete": "10.5",
        "originalRegulatoryDate": "2024-01-02",
        "currentRegulatoryDate": "2025-02-03",
        "currentInterimMilestone": "IM2",
        "spendToDate": "5.5",
        "forecastOutturn": "20.5",
        "percentSpendToDate": "25.5",
        "criticalHighProfile": "YES",
        "schemeType": "Secondary",
        "amp": "AMP8",
        "submissionNotes": "Note",
    }
    row.update(changes)
    return row


def _list_row(**changes):
    row = _scheme_row()
    row.update({
        **{
            f"{kind}Im{index}": None
            for kind in ("baseline", "forecast")
            for index in range(1, 8)
        },
        **{
            f"{kind}{year}_{(year + 1) % 100:02d}": None
            for kind in ("baseline", "forecast")
            for year in range(2023, 2030)
        },
        **changes,
    })
    return row

