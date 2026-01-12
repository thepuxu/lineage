#!/usr/bin/env python3
"""
Mock Data Generator for Lineage Explorer - CSV Version (No External Dependencies)

Creates sample CSV files for testing. Uses only built-in Python modules.

Usage:
    python generate_mock_data_csv.py
"""

import csv
from pathlib import Path
from typing import List, Dict


# OFSAA-style table definitions with columns
TABLES = {
    # Source Layer
    "src_loan_contracts": [
        ("n_contract_skey", "NUMBER", True, False),
        ("v_contract_id", "VARCHAR2", False, False),
        ("n_customer_skey", "NUMBER", False, True),
        ("d_origination_date", "DATE", False, False),
        ("d_maturity_date", "DATE", False, False),
        ("n_principal_amount", "NUMBER", False, False),
        ("n_interest_rate", "NUMBER", False, False),
        ("v_currency_code", "VARCHAR2", False, True),
        ("v_product_code", "VARCHAR2", False, True),
        ("v_branch_code", "VARCHAR2", False, False),
        ("d_approve_date", "DATE", False, False),
    ],
    "src_collateral": [
        ("n_collateral_skey", "NUMBER", True, False),
        ("v_collateral_id", "VARCHAR2", False, False),
        ("n_contract_skey", "NUMBER", False, True),
        ("v_collateral_type", "VARCHAR2", False, False),
        ("n_collateral_value", "NUMBER", False, False),
        ("d_valuation_date", "DATE", False, False),
        ("v_currency_code", "VARCHAR2", False, True),
    ],
    "src_counterparty": [
        ("n_customer_skey", "NUMBER", True, False),
        ("v_customer_id", "VARCHAR2", False, False),
        ("v_customer_name", "VARCHAR2", False, False),
        ("v_customer_type", "VARCHAR2", False, False),
        ("v_country_code", "VARCHAR2", False, False),
        ("v_segment_code", "VARCHAR2", False, False),
        ("n_credit_rating", "NUMBER", False, False),
        ("d_onboard_date", "DATE", False, False),
    ],
    "src_gl_balances": [
        ("n_gl_skey", "NUMBER", True, False),
        ("v_gl_account", "VARCHAR2", False, False),
        ("v_cost_center", "VARCHAR2", False, False),
        ("d_posting_date", "DATE", False, False),
        ("n_debit_amount", "NUMBER", False, False),
        ("n_credit_amount", "NUMBER", False, False),
        ("v_currency_code", "VARCHAR2", False, True),
    ],
    "src_fx_rates": [
        ("v_from_currency", "VARCHAR2", True, False),
        ("v_to_currency", "VARCHAR2", True, False),
        ("d_rate_date", "DATE", True, False),
        ("n_exchange_rate", "NUMBER", False, False),
    ],
    # Staging Layer
    "stg_loan_contracts": [
        ("n_contract_skey", "NUMBER", True, False),
        ("v_contract_id", "VARCHAR2", False, False),
        ("n_customer_skey", "NUMBER", False, True),
        ("d_origination_date", "DATE", False, False),
        ("d_maturity_date", "DATE", False, False),
        ("n_principal_amount", "NUMBER", False, False),
        ("n_principal_amount_usd", "NUMBER", False, False),
        ("n_interest_rate", "NUMBER", False, False),
        ("v_currency_code", "VARCHAR2", False, True),
        ("v_product_code", "VARCHAR2", False, True),
        ("v_branch_code", "VARCHAR2", False, False),
        ("d_approve_date", "DATE", False, False),
        ("n_days_to_maturity", "NUMBER", False, False),
    ],
    "stg_collateral": [
        ("n_collateral_skey", "NUMBER", True, False),
        ("v_collateral_id", "VARCHAR2", False, False),
        ("n_contract_skey", "NUMBER", False, True),
        ("v_collateral_type", "VARCHAR2", False, False),
        ("n_collateral_value", "NUMBER", False, False),
        ("n_collateral_value_usd", "NUMBER", False, False),
        ("d_valuation_date", "DATE", False, False),
        ("v_currency_code", "VARCHAR2", False, True),
    ],
    "stg_counterparty": [
        ("n_customer_skey", "NUMBER", True, False),
        ("v_customer_id", "VARCHAR2", False, False),
        ("v_customer_name", "VARCHAR2", False, False),
        ("v_customer_type", "VARCHAR2", False, False),
        ("v_country_code", "VARCHAR2", False, False),
        ("v_segment_code", "VARCHAR2", False, False),
        ("n_credit_rating", "NUMBER", False, False),
        ("v_risk_grade", "VARCHAR2", False, False),
        ("d_onboard_date", "DATE", False, False),
    ],
    "stg_gl_balances": [
        ("n_gl_skey", "NUMBER", True, False),
        ("v_gl_account", "VARCHAR2", False, False),
        ("v_cost_center", "VARCHAR2", False, False),
        ("d_posting_date", "DATE", False, False),
        ("n_debit_amount", "NUMBER", False, False),
        ("n_credit_amount", "NUMBER", False, False),
        ("n_net_amount", "NUMBER", False, False),
        ("n_net_amount_usd", "NUMBER", False, False),
        ("v_currency_code", "VARCHAR2", False, True),
    ],
    # Dimension Layer
    "dim_counterparty": [
        ("n_counterparty_key", "NUMBER", True, False),
        ("n_customer_skey", "NUMBER", False, False),
        ("v_customer_id", "VARCHAR2", False, False),
        ("v_customer_name", "VARCHAR2", False, False),
        ("v_customer_type", "VARCHAR2", False, False),
        ("v_country_code", "VARCHAR2", False, False),
        ("v_segment_code", "VARCHAR2", False, False),
        ("n_credit_rating", "NUMBER", False, False),
        ("v_risk_grade", "VARCHAR2", False, False),
        ("d_effective_from", "DATE", False, False),
        ("d_effective_to", "DATE", False, False),
        ("f_current_flag", "VARCHAR2", False, False),
    ],
    "dim_product": [
        ("n_product_key", "NUMBER", True, False),
        ("v_product_code", "VARCHAR2", False, False),
        ("v_product_name", "VARCHAR2", False, False),
        ("v_product_category", "VARCHAR2", False, False),
        ("v_product_type", "VARCHAR2", False, False),
    ],
    "dim_currency": [
        ("n_currency_key", "NUMBER", True, False),
        ("v_currency_code", "VARCHAR2", False, False),
        ("v_currency_name", "VARCHAR2", False, False),
        ("n_decimal_places", "NUMBER", False, False),
    ],
    "dim_geography": [
        ("n_geography_key", "NUMBER", True, False),
        ("v_country_code", "VARCHAR2", False, False),
        ("v_country_name", "VARCHAR2", False, False),
        ("v_region", "VARCHAR2", False, False),
    ],
    "dim_time": [
        ("n_time_key", "NUMBER", True, False),
        ("d_date", "DATE", False, False),
        ("v_month", "VARCHAR2", False, False),
        ("v_quarter", "VARCHAR2", False, False),
        ("n_year", "NUMBER", False, False),
    ],
    "dim_gl_account": [
        ("n_gl_account_key", "NUMBER", True, False),
        ("v_gl_account", "VARCHAR2", False, False),
        ("v_account_name", "VARCHAR2", False, False),
        ("v_account_type", "VARCHAR2", False, False),
    ],
    # Fact Layer
    "fct_loan_positions": [
        ("n_position_key", "NUMBER", True, False),
        ("n_contract_skey", "NUMBER", False, True),
        ("n_counterparty_key", "NUMBER", False, True),
        ("n_product_key", "NUMBER", False, True),
        ("n_currency_key", "NUMBER", False, True),
        ("n_time_key", "NUMBER", False, True),
        ("n_principal_amount", "NUMBER", False, False),
        ("n_principal_amount_usd", "NUMBER", False, False),
        ("n_interest_accrued", "NUMBER", False, False),
        ("n_exposure_amount", "NUMBER", False, False),
    ],
    "fct_collateral_positions": [
        ("n_position_key", "NUMBER", True, False),
        ("n_collateral_skey", "NUMBER", False, True),
        ("n_contract_skey", "NUMBER", False, True),
        ("n_currency_key", "NUMBER", False, True),
        ("n_time_key", "NUMBER", False, True),
        ("n_collateral_value", "NUMBER", False, False),
        ("n_collateral_value_usd", "NUMBER", False, False),
        ("n_haircut_pct", "NUMBER", False, False),
        ("n_adjusted_value", "NUMBER", False, False),
    ],
    "fct_non_sec_exposures": [
        ("n_exposure_key", "NUMBER", True, False),
        ("n_contract_skey", "NUMBER", False, True),
        ("n_counterparty_key", "NUMBER", False, True),
        ("n_product_key", "NUMBER", False, True),
        ("n_time_key", "NUMBER", False, True),
        ("n_ead", "NUMBER", False, False),
        ("n_lgd", "NUMBER", False, False),
        ("n_pd", "NUMBER", False, False),
        ("n_rwa", "NUMBER", False, False),
        ("n_expected_loss", "NUMBER", False, False),
    ],
    "fct_gl_summary": [
        ("n_gl_summary_key", "NUMBER", True, False),
        ("n_gl_account_key", "NUMBER", False, True),
        ("n_time_key", "NUMBER", False, True),
        ("n_currency_key", "NUMBER", False, True),
        ("n_debit_total", "NUMBER", False, False),
        ("n_credit_total", "NUMBER", False, False),
        ("n_net_balance", "NUMBER", False, False),
        ("n_net_balance_usd", "NUMBER", False, False),
    ],
    # Report Layer
    "rpt_exposure_summary": [
        ("n_report_key", "NUMBER", True, False),
        ("d_report_date", "DATE", False, False),
        ("v_segment", "VARCHAR2", False, False),
        ("v_product_category", "VARCHAR2", False, False),
        ("n_total_exposure", "NUMBER", False, False),
        ("n_total_rwa", "NUMBER", False, False),
        ("n_total_expected_loss", "NUMBER", False, False),
    ],
    "rpt_risk_weighted_assets": [
        ("n_report_key", "NUMBER", True, False),
        ("d_report_date", "DATE", False, False),
        ("v_risk_category", "VARCHAR2", False, False),
        ("n_exposure_amount", "NUMBER", False, False),
        ("n_risk_weight", "NUMBER", False, False),
        ("n_rwa_amount", "NUMBER", False, False),
    ],
    "rpt_regulatory_capital": [
        ("n_report_key", "NUMBER", True, False),
        ("d_report_date", "DATE", False, False),
        ("v_capital_type", "VARCHAR2", False, False),
        ("n_capital_amount", "NUMBER", False, False),
        ("n_capital_ratio", "NUMBER", False, False),
    ],
    "rpt_customer_360": [
        ("n_report_key", "NUMBER", True, False),
        ("n_counterparty_key", "NUMBER", False, True),
        ("v_customer_name", "VARCHAR2", False, False),
        ("n_total_exposure", "NUMBER", False, False),
        ("n_total_collateral", "NUMBER", False, False),
        ("n_net_exposure", "NUMBER", False, False),
        ("v_risk_grade", "VARCHAR2", False, False),
    ],
}


def create_mapping(
    object_name: str,
    dest_table: str,
    dest_field: str,
    source_table: str,
    source_field: str,
    usage_type: str = "MAPPING",
    usage_role: str = "VALUE",
    source_type: str = "PHYSICAL",
    derived_expression: str = "",
    join_alias: str = "",
    join_filters: str = "",
    constant_value: str = "",
    notes: str = ""
) -> dict:
    """Helper to create a mapping row in the OFSAA format."""
    if usage_type == "JOIN":
        trace_path = f"JOIN->{source_table}.{source_field}"
    else:
        trace_path = f"->{source_field}->{source_table}.{source_field}"

    return {
        "object_name": object_name,
        "destination_table": dest_table,
        "destination_field": dest_field,
        "usage_type": usage_type,
        "usage_role": usage_role,
        "source_type": source_type,
        "source_table": source_table,
        "source_field": source_field,
        "constant_value": constant_value,
        "derived_output": dest_field,
        "derived_expression": derived_expression if derived_expression else source_field,
        "join_alias": join_alias,
        "join_keys": "",
        "join_filters": join_filters,
        "dm_match": "Y",
        "trace_path": trace_path,
        "notes": notes,
    }


def generate_v1_mappings() -> List[Dict]:
    """Generate V1 mappings."""
    mappings = []

    # T2T_STG_LOAN_CONTRACTS: src_loan_contracts -> stg_loan_contracts
    obj = "T2T_STG_LOAN_CONTRACTS"
    for field in ["n_contract_skey", "v_contract_id", "n_customer_skey", "d_origination_date",
                  "d_maturity_date", "n_principal_amount", "n_interest_rate", "v_currency_code",
                  "v_product_code", "v_branch_code", "d_approve_date"]:
        mappings.append(create_mapping(obj, "stg_loan_contracts", field, "src_loan_contracts", field))

    # Derived field
    mappings.append(create_mapping(obj, "stg_loan_contracts", "n_principal_amount_usd",
                                   "src_loan_contracts", "n_principal_amount",
                                   derived_expression="n_principal_amount * FX_RATE",
                                   source_type="DERIVED"))
    mappings.append(create_mapping(obj, "stg_loan_contracts", "n_days_to_maturity",
                                   "src_loan_contracts", "d_maturity_date",
                                   derived_expression="d_maturity_date - SYSDATE",
                                   source_type="DERIVED"))

    # Join to FX rates
    mappings.append(create_mapping(obj, "stg_loan_contracts", "n_principal_amount_usd",
                                   "src_fx_rates", "n_exchange_rate",
                                   usage_type="JOIN", usage_role="JOIN_KEY",
                                   join_alias="FX", join_filters="FX.v_from_currency = SRC.v_currency_code"))

    # T2T_STG_COLLATERAL: src_collateral -> stg_collateral
    obj = "T2T_STG_COLLATERAL"
    for field in ["n_collateral_skey", "v_collateral_id", "n_contract_skey", "v_collateral_type",
                  "n_collateral_value", "d_valuation_date", "v_currency_code"]:
        mappings.append(create_mapping(obj, "stg_collateral", field, "src_collateral", field))

    mappings.append(create_mapping(obj, "stg_collateral", "n_collateral_value_usd",
                                   "src_collateral", "n_collateral_value",
                                   derived_expression="n_collateral_value * FX_RATE",
                                   source_type="DERIVED"))

    # T2T_STG_COUNTERPARTY: src_counterparty -> stg_counterparty
    obj = "T2T_STG_COUNTERPARTY"
    for field in ["n_customer_skey", "v_customer_id", "v_customer_name", "v_customer_type",
                  "v_country_code", "v_segment_code", "n_credit_rating", "d_onboard_date"]:
        mappings.append(create_mapping(obj, "stg_counterparty", field, "src_counterparty", field))

    mappings.append(create_mapping(obj, "stg_counterparty", "v_risk_grade",
                                   "src_counterparty", "n_credit_rating",
                                   derived_expression="CASE WHEN n_credit_rating >= 7 THEN 'LOW' WHEN n_credit_rating >= 4 THEN 'MEDIUM' ELSE 'HIGH' END",
                                   source_type="DERIVED"))

    # T2T_STG_GL_BALANCES: src_gl_balances -> stg_gl_balances
    obj = "T2T_STG_GL_BALANCES"
    for field in ["n_gl_skey", "v_gl_account", "v_cost_center", "d_posting_date",
                  "n_debit_amount", "n_credit_amount", "v_currency_code"]:
        mappings.append(create_mapping(obj, "stg_gl_balances", field, "src_gl_balances", field))

    mappings.append(create_mapping(obj, "stg_gl_balances", "n_net_amount",
                                   "src_gl_balances", "n_debit_amount",
                                   derived_expression="n_debit_amount - n_credit_amount",
                                   source_type="DERIVED"))

    # SQL_DIM_COUNTERPARTY: stg_counterparty -> dim_counterparty (SCD Type 2)
    obj = "SQL_DIM_COUNTERPARTY"
    mappings.append(create_mapping(obj, "dim_counterparty", "n_counterparty_key",
                                   "stg_counterparty", "n_customer_skey",
                                   derived_expression="SEQ_DIM_COUNTERPARTY.NEXTVAL",
                                   source_type="DERIVED"))
    for field in ["n_customer_skey", "v_customer_id", "v_customer_name", "v_customer_type",
                  "v_country_code", "v_segment_code", "n_credit_rating", "v_risk_grade"]:
        mappings.append(create_mapping(obj, "dim_counterparty", field, "stg_counterparty", field))

    # T2T_DIM_PRODUCT: hardcoded/lookup
    obj = "T2T_DIM_PRODUCT"
    mappings.append(create_mapping(obj, "dim_product", "n_product_key",
                                   "stg_loan_contracts", "v_product_code",
                                   derived_expression="SEQ_DIM_PRODUCT.NEXTVAL",
                                   source_type="DERIVED"))
    mappings.append(create_mapping(obj, "dim_product", "v_product_code",
                                   "stg_loan_contracts", "v_product_code"))

    # T2T_FCT_LOAN_POSITIONS: stg_loan_contracts -> fct_loan_positions
    obj = "T2T_FCT_LOAN_POSITIONS"
    mappings.append(create_mapping(obj, "fct_loan_positions", "n_contract_skey",
                                   "stg_loan_contracts", "n_contract_skey"))
    mappings.append(create_mapping(obj, "fct_loan_positions", "n_principal_amount",
                                   "stg_loan_contracts", "n_principal_amount"))
    mappings.append(create_mapping(obj, "fct_loan_positions", "n_principal_amount_usd",
                                   "stg_loan_contracts", "n_principal_amount_usd"))
    mappings.append(create_mapping(obj, "fct_loan_positions", "n_counterparty_key",
                                   "dim_counterparty", "n_counterparty_key",
                                   usage_type="JOIN", usage_role="JOIN_KEY",
                                   join_filters="DIM.n_customer_skey = STG.n_customer_skey"))
    mappings.append(create_mapping(obj, "fct_loan_positions", "n_product_key",
                                   "dim_product", "n_product_key",
                                   usage_type="JOIN", usage_role="JOIN_KEY"))

    # T2T_FCT_COLLATERAL_POSITIONS
    obj = "T2T_FCT_COLLATERAL_POSITIONS"
    mappings.append(create_mapping(obj, "fct_collateral_positions", "n_collateral_skey",
                                   "stg_collateral", "n_collateral_skey"))
    mappings.append(create_mapping(obj, "fct_collateral_positions", "n_contract_skey",
                                   "stg_collateral", "n_contract_skey"))
    mappings.append(create_mapping(obj, "fct_collateral_positions", "n_collateral_value",
                                   "stg_collateral", "n_collateral_value"))
    mappings.append(create_mapping(obj, "fct_collateral_positions", "n_collateral_value_usd",
                                   "stg_collateral", "n_collateral_value_usd"))

    # T2T_FCT_NON_SEC_EXPOSURES
    obj = "T2T_FCT_NON_SEC_EXPOSURES"
    mappings.append(create_mapping(obj, "fct_non_sec_exposures", "n_contract_skey",
                                   "fct_loan_positions", "n_contract_skey"))
    mappings.append(create_mapping(obj, "fct_non_sec_exposures", "n_counterparty_key",
                                   "fct_loan_positions", "n_counterparty_key"))
    mappings.append(create_mapping(obj, "fct_non_sec_exposures", "n_ead",
                                   "fct_loan_positions", "n_principal_amount_usd"))
    mappings.append(create_mapping(obj, "fct_non_sec_exposures", "n_lgd",
                                   "fct_collateral_positions", "n_adjusted_value",
                                   derived_expression="1 - (n_adjusted_value / n_ead)",
                                   source_type="DERIVED"))

    # SQL_FCT_GL_SUMMARY
    obj = "SQL_FCT_GL_SUMMARY"
    mappings.append(create_mapping(obj, "fct_gl_summary", "n_debit_total",
                                   "stg_gl_balances", "n_debit_amount",
                                   derived_expression="SUM(n_debit_amount)",
                                   source_type="DERIVED"))
    mappings.append(create_mapping(obj, "fct_gl_summary", "n_credit_total",
                                   "stg_gl_balances", "n_credit_amount",
                                   derived_expression="SUM(n_credit_amount)",
                                   source_type="DERIVED"))
    mappings.append(create_mapping(obj, "fct_gl_summary", "n_net_balance",
                                   "stg_gl_balances", "n_net_amount",
                                   derived_expression="SUM(n_net_amount)",
                                   source_type="DERIVED"))

    # DIH_RPT_EXPOSURE_SUMMARY
    obj = "DIH_RPT_EXPOSURE_SUMMARY"
    mappings.append(create_mapping(obj, "rpt_exposure_summary", "n_total_exposure",
                                   "fct_non_sec_exposures", "n_ead",
                                   derived_expression="SUM(n_ead)",
                                   source_type="DERIVED"))
    mappings.append(create_mapping(obj, "rpt_exposure_summary", "n_total_rwa",
                                   "fct_non_sec_exposures", "n_rwa",
                                   derived_expression="SUM(n_rwa)",
                                   source_type="DERIVED"))
    mappings.append(create_mapping(obj, "rpt_exposure_summary", "v_segment",
                                   "dim_counterparty", "v_segment_code"))

    # DIH_RPT_RWA
    obj = "DIH_RPT_RWA"
    mappings.append(create_mapping(obj, "rpt_risk_weighted_assets", "n_exposure_amount",
                                   "fct_non_sec_exposures", "n_ead"))
    mappings.append(create_mapping(obj, "rpt_risk_weighted_assets", "n_rwa_amount",
                                   "fct_non_sec_exposures", "n_rwa"))

    # DIH_RPT_CUSTOMER_360
    obj = "DIH_RPT_CUSTOMER_360"
    mappings.append(create_mapping(obj, "rpt_customer_360", "n_counterparty_key",
                                   "dim_counterparty", "n_counterparty_key"))
    mappings.append(create_mapping(obj, "rpt_customer_360", "v_customer_name",
                                   "dim_counterparty", "v_customer_name"))
    mappings.append(create_mapping(obj, "rpt_customer_360", "v_risk_grade",
                                   "dim_counterparty", "v_risk_grade"))
    mappings.append(create_mapping(obj, "rpt_customer_360", "n_total_exposure",
                                   "fct_non_sec_exposures", "n_ead",
                                   derived_expression="SUM(n_ead)",
                                   source_type="DERIVED"))
    mappings.append(create_mapping(obj, "rpt_customer_360", "n_total_collateral",
                                   "fct_collateral_positions", "n_collateral_value_usd",
                                   derived_expression="SUM(n_collateral_value_usd)",
                                   source_type="DERIVED"))

    return mappings


def generate_v2_mappings(v1_mappings: List[Dict]) -> List[Dict]:
    """Generate V2 mappings with some changes for delta testing."""
    v2_mappings = []

    for m in v1_mappings:
        new_m = m.copy()

        # Modify some rules
        if m["object_name"] == "T2T_STG_COUNTERPARTY" and m["destination_field"] == "v_risk_grade":
            new_m["derived_expression"] = "CASE WHEN n_credit_rating >= 8 THEN 'LOW' WHEN n_credit_rating >= 5 THEN 'MEDIUM' ELSE 'HIGH' END"
            new_m["notes"] = "Updated risk grade thresholds"

        # Modify another rule
        if m["object_name"] == "T2T_FCT_NON_SEC_EXPOSURES" and m["destination_field"] == "n_lgd":
            new_m["derived_expression"] = "GREATEST(0, 1 - (n_adjusted_value / NULLIF(n_ead, 0)))"
            new_m["notes"] = "Added null safety"

        v2_mappings.append(new_m)

    # Add new mappings (new object)
    obj = "DIH_RPT_REG_CAPITAL"
    v2_mappings.append(create_mapping(obj, "rpt_regulatory_capital", "n_capital_amount",
                                      "fct_non_sec_exposures", "n_rwa",
                                      derived_expression="SUM(n_rwa) * 0.08",
                                      source_type="DERIVED",
                                      notes="New regulatory capital calculation"))
    v2_mappings.append(create_mapping(obj, "rpt_regulatory_capital", "d_report_date",
                                      "dim_time", "d_date"))
    v2_mappings.append(create_mapping(obj, "rpt_regulatory_capital", "v_capital_type",
                                      "fct_non_sec_exposures", "n_ead",
                                      derived_expression="'CET1'",
                                      source_type="CONSTANT",
                                      constant_value="CET1"))
    v2_mappings.append(create_mapping(obj, "rpt_regulatory_capital", "n_capital_ratio",
                                      "fct_non_sec_exposures", "n_rwa",
                                      derived_expression="n_capital_amount / NULLIF(SUM(n_rwa), 0)",
                                      source_type="DERIVED"))

    # Remove one mapping (simulate deletion)
    v2_mappings = [m for m in v2_mappings if not (
        m["object_name"] == "T2T_STG_LOAN_CONTRACTS" and
        m["destination_field"] == "v_branch_code"
    )]

    return v2_mappings


def generate_data_model() -> List[Dict]:
    """Generate data model from TABLES definition."""
    data_model = []
    for table_name, columns in TABLES.items():
        for col_name, data_type, is_pk, is_fk in columns:
            data_model.append({
                "table_name": table_name,
                "column_name": col_name,
                "data_type": data_type,
                "is_pk": "TRUE" if is_pk else "FALSE",
                "is_fk": "TRUE" if is_fk else "FALSE",
                "description": "",
            })
    return data_model


def write_csv(path: Path, data: List[Dict], fieldnames: List[str]):
    """Write data to CSV file."""
    with open(path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


def main():
    print("Generating OFSAA-style mock data (CSV format)...")

    # Create output directory
    output_dir = Path(__file__).parent / "sample_data_csv"
    output_dir.mkdir(exist_ok=True)

    # Generate data
    v1_mappings = generate_v1_mappings()
    v2_mappings = generate_v2_mappings(v1_mappings)
    data_model = generate_data_model()

    # Define column order for mappings CSV
    mapping_columns = [
        "object_name", "destination_table", "destination_field", "usage_type",
        "usage_role", "source_type", "source_table", "source_field", "constant_value",
        "derived_output", "derived_expression", "join_alias", "join_keys",
        "join_filters", "dm_match", "trace_path", "notes"
    ]

    # Define column order for data model CSV
    dm_columns = ["table_name", "column_name", "data_type", "is_pk", "is_fk", "description"]

    # Write files
    dm_path = output_dir / "data_model.csv"
    write_csv(dm_path, data_model, dm_columns)
    print(f"Created: {dm_path} ({len(data_model)} columns)")

    v1_path = output_dir / "mappings_v1.csv"
    write_csv(v1_path, v1_mappings, mapping_columns)
    print(f"Created: {v1_path} ({len(v1_mappings)} mappings)")

    v2_path = output_dir / "mappings_v2.csv"
    write_csv(v2_path, v2_mappings, mapping_columns)
    print(f"Created: {v2_path} ({len(v2_mappings)} mappings)")

    print(f"\nDone! Files created in: {output_dir}")
    print("\nTo generate visualization, run:")
    print(f"  python generate_lineage_csv.py --v1 {v1_path} --v2 {v2_path} --data-model {dm_path} --output output")


if __name__ == "__main__":
    main()
