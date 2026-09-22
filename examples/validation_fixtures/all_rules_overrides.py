"""Comprehensive override example for the validation fixtures."""


overrides = {
    "sheet_policy": {
        "required_selectors": [
            {"mode": "exact", "value": "Data"},
            {"mode": "exact", "value": "Codes"},
        ],
        "ignored_selectors": [],
        "selector_match_action": "all",
    },
    "tables": [
        {
            "name": "process_data",
            "sheet_selector": {"mode": "exact", "value": "Data"},
            "required": True,
            "header_row": 1,
            "header_policy": {
                "required_columns": [
                    "Measure_Cd",
                    "Unit",
                    "Measure_Value",
                    "Comment",
                    "Organisation_Cd",
                    "Business_Unit_Cd",
                    "Observation_Period_Cd",
                    "Status",
                ],
                "match_mode": "exact_order",
                "extra_column_action": "allowed",
                "missing_column_action": "error",
            },
            "data_boundary": {
                "mode": "last_non_empty_row",
                "infer_columns": True,
            },
            "data_presence": "require_one_usable_row",
            "load_policy": {
                "enabled": True,
                "infer_types": True,
                "preserve_empty_values": True,
            },
            "empty_row_rules": [
                {
                    "rows": [10],
                    "blank_policy": "blank_strings_and_nulls",
                },
            ],
            "column_validations": [
                {
                    "column": "Measure_Cd",
                    "unique": True,
                    "severity": "error",
                },
                {
                    "column": "Measure_Value",
                    "required": True,
                    "max_length": 4,
                    "length_mode": "characters",
                    "severity": "error",
                },
                {
                    "column": "Measure_Value",
                    "value_type": "text",
                    "when": {"column": "Unit", "equals": "Text"},
                    "severity": "error",
                },
                {
                    "column": "Measure_Value",
                    "value_type": "numeric",
                    "when": {"column": "Unit", "not_equals": "Text"},
                    "severity": "error",
                },
                {
                    "column": "Comment",
                    "forbidden_patterns": [r"¬¬", r"^FORBIDDEN$"],
                    "severity": "error",
                },
                {
                    "column": "Status",
                    "allowed_values": ["Open", "Closed"],
                    "severity": "error",
                },
            ],
            "table_validations": [
                {
                    "rule_code": "conflicting_duplicate",
                    "key_columns": {
                        "mode": "pattern",
                        "pattern": r".*_Cd$",
                    },
                    "value_columns": ["Measure_Value"],
                    "severity": "error",
                },
            ],
        },
        {
            "name": "codes",
            "sheet_selector": {"mode": "exact", "value": "Codes"},
            "header_row": 1,
            "header_policy": {
                "required_columns": ["Code"],
            },
            "data_boundary": {
                "mode": "last_non_empty_row",
                "infer_columns": True,
            },
            "data_presence": "require_one_usable_row",
        },
    ],
    "cross_table_checks": [
        {
            "name": "measure_codes_exist",
            "source_table": "process_data",
            "source_column": "Measure_Cd",
            "reference_table": "codes",
            "reference_column": "Code",
            "null_policy": "ignore",
            "duplicate_reference_action": "allow",
            "severity": "error",
        },
    ],
    "expected_cells": [
        {
            "name": "data_header",
            "locations": [
                {
                    "sheet_selector": {"mode": "exact", "value": "Data"},
                    "cell_reference": "A1",
                },
            ],
            "expected_value": "Measure_Cd",
            "comparison": {"mode": "exact"},
            "severity": "error",
        },
        {
            "name": "organisation_input",
            "locations": [
                {
                    "sheet_selector": {"mode": "exact", "value": "Data"},
                    "cell_reference": "Z11",
                },
            ],
            "expected_value": "organisation-code",
            "comparison": {"mode": "exact"},
            "severity": "error",
        },
    ],
    "workbook_checks": [
        {
            "rule_code": "extra_sheet",
            "enabled": True,
            "scope": "all_sheets",
            "severity": "error",
        },
        {
            "rule_code": "missing_reference_sheet",
            "enabled": True,
            "scope": "all_sheets",
            "severity": "error",
        },
        {
            "rule_code": "sheet_structure",
            "enabled": True,
            "scope": "overlapping_sheets",
            "options": {"compare_headers": True},
            "severity": "error",
        },
        {
            "rule_code": "formula_difference",
            "enabled": True,
            "scope": "overlapping_sheets",
            "options": {"whitespace_policy": "normalised"},
            "severity": "error",
        },
        {
            "rule_code": "formula_error",
            "enabled": True,
            "scope": "all_sheets",
            "options": {
                "error_tokens": [
                    "#DIV/0!",
                    "#N/A",
                    "#NAME?",
                    "#REF!",
                    "#VALUE!",
                ],
            },
            "severity": "error",
        },
        {
            "rule_code": "forbidden_values",
            "enabled": True,
            "scope": {
                "type": "selected_sheets",
                "sheets": ["Data"],
            },
            "options": {
                "forbidden_patterns": [r"^FORBIDDEN$", r"¬¬"],
            },
            "severity": "error",
        },
        {
            "rule_code": "required_filled_cells",
            "enabled": True,
            "scope": "all_sheets",
            "options": {
                "fill_colors": ["#FFFF00"],
                "tolerance_percent": 0,
            },
            "severity": "error",
        },
        {
            "rule_code": "unexpected_formula",
            "enabled": True,
            "scope": "all_sheets",
            "options": {
                "fill_colors": ["#FFFF00"],
                "tolerance_percent": 0,
            },
            "severity": "error",
        },
    ],
    "database_lookups": [
        {
            "name": "measure_dimension",
            "schema": "dbo",
            "table": "dim_measure",
            "key_columns": {"Measure_Cd": "Measure_Cd"},
            "value_columns": ["Expected_Value_Type", "Measure_Id"],
        },
        {
            "name": "organisation_dimension",
            "schema": "dbo",
            "table": "dim_organisation",
            "key_columns": {"Organisation_Cd": "Organisation_Cd"},
            "value_columns": ["Organisation_Id"],
        },
        {
            "name": "business_unit_dimension",
            "schema": "dbo",
            "table": "dim_business_unit",
            "key_columns": {"Business_Unit_Cd": "Business_Unit_Cd"},
            "value_columns": ["Business_Unit_Id"],
        },
        {
            "name": "period_dimension",
            "schema": "dbo",
            "table": "dim_observation_period",
            "key_columns": {
                "Observation_Period_Cd": "Observation_Period_Cd",
            },
            "value_columns": ["Observation_Period_Id"],
        },
    ],
    "database_checks": [
        {
            "name": "measure_value_type",
            "rule_code": "value_type",
            "source_table": "process_data",
            "lookup": "measure_dimension",
            "column_validations": [
                {
                    "column": "Measure_Value",
                    "value_type_from": "Expected_Value_Type",
                },
            ],
            "severity": "error",
        },
        {
            "name": "conflicting_database_records",
            "rule_code": "conflicting_duplicate",
            "source_table": "process_data",
            "dimensions": [
                {
                    "source_column": "Measure_Cd",
                    "lookup": "measure_dimension",
                    "canonical_column": "Measure_Id",
                },
                {
                    "source_column": "Organisation_Cd",
                    "lookup": "organisation_dimension",
                    "canonical_column": "Organisation_Id",
                },
                {
                    "source_column": "Business_Unit_Cd",
                    "lookup": "business_unit_dimension",
                    "canonical_column": "Business_Unit_Id",
                },
                {
                    "source_column": "Observation_Period_Cd",
                    "lookup": "period_dimension",
                    "canonical_column": "Observation_Period_Id",
                },
            ],
            "value_columns": ["Measure_Value"],
            "severity": "error",
        },
    ],
    "runtime": {
        "max_cells_scanned": 10_000_000,
        "read_only": False,
        "macro_policy": "reject",
        "missing_formula_cache_action": "not_run",
        "feature_policy": {
            "unavailable_action": "warning",
            "external_links": "warning",
            "charts": "warning",
            "pivot_tables": "warning",
            "named_ranges": "warning",
            "merged_cells": {
                "action": "warning",
                "scope": {"type": "all_sheets"},
            },
            "macros": "error",
        },
    },
    "enabled_rules": None,
    "rule_severity": {
        "required_sheet": "error",
        "extra_sheet": "error",
        "expected_cell": "error",
        "table_resolution": "error",
        "column_header": "error",
        "non_empty_data": "error",
        "empty_row_pattern": "error",
        "missing_value": "error",
        "duplicate_value": "error",
        "allowed_values": "error",
        "forbidden_values": "error",
        "pandas_load": "error",
        "missing_column": "error",
        "empty_table": "error",
        "data_boundary": "error",
        "max_length": "error",
        "value_type": "error",
        "conflicting_duplicate": "error",
        "values_in_reference": "error",
        "required_filled_cells": "error",
        "unexpected_formula": "error",
        "missing_reference_sheet": "error",
        "sheet_structure": "error",
        "formula_difference": "error",
        "formula_error": "error",
        "feature_policy": "warning",
        "feature_detection_unavailable": "warning",
    },
}
