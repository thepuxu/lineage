#!/usr/bin/env python3
"""
Generate mock Excel data for testing the Lineage Explorer.

Creates OFSAA-style mapping files with realistic column structure:
- sample_data/mappings_v1.xlsx - Version 1 of field mappings
- sample_data/mappings_v2.xlsx - Version 2 with changes (for delta testing)
- sample_data/data_model.xlsx - Full data model with all tables/columns

Column structure matches real OFSAA exports:
object_name, destination_table, destination_field, usage_type, usage_role,
source_type, source_table, source_field, constant_value, derived_output,
derived_expression, join_alias, join_keys, join_filters, dm_match, trace_path, notes
"""

import pandas as pd
import random
import argparse
from pathlib import Path

# Output directory
OUTPUT_DIR = Path(__file__).parent / "sample_data"
OUTPUT_DIR.mkdir(exist_ok=True)


# =============================================================================
# Case Randomization for Testing Case Normalization
# =============================================================================

def randomize_case(name: str, seed: int = None) -> str:
    """
    Randomly capitalize parts of the name to test case normalization.

    Examples:
        "src_orders" → "SRC_Orders", "src_ORDERS", "Src_orders"
        "customer_id" → "CUSTOMER_Id", "Customer_ID", "customer_ID"
    """
    if seed is not None:
        random.seed(seed)

    result = []
    for char in name:
        if random.random() < 0.3:  # 30% chance to flip case
            result.append(char.swapcase())
        else:
            result.append(char)
    return ''.join(result)


# Mapping types for variety (tests the toggle feature)
MAPPING_TYPES = ['MAP', 'MAP', 'MAP', 'MAP', 'JOIN', 'LOOKUP', 'TRANSFORM', 'CALC']


# =============================================================================
# Data Model Definition - Realistic OFSAA/Banking Tables
# =============================================================================

TABLES = {
    # -------------------------------------------------------------------------
    # Source Layer (src_) - Raw data from upstream systems
    # -------------------------------------------------------------------------
    "src_loan_contracts": [
        ("N_CONTRACT_SKEY", "NUMBER(20)", True, False, "Contract surrogate key"),
        ("V_CONTRACT_ID", "VARCHAR2(50)", False, False, "Contract natural key"),
        ("V_CUSTOMER_ID", "VARCHAR2(50)", False, True, "Customer reference"),
        ("V_PRODUCT_CODE", "VARCHAR2(20)", False, True, "Product code"),
        ("D_ORIGINATION_DATE", "DATE", False, False, "Loan origination date"),
        ("D_MATURITY_DATE", "DATE", False, False, "Loan maturity date"),
        ("N_ORIGINAL_AMOUNT", "NUMBER(18,2)", False, False, "Original loan amount"),
        ("N_OUTSTANDING_AMOUNT", "NUMBER(18,2)", False, False, "Current outstanding"),
        ("V_CURRENCY_CODE", "VARCHAR2(3)", False, True, "Currency code"),
        ("N_INTEREST_RATE", "NUMBER(10,6)", False, False, "Interest rate"),
        ("V_RATE_TYPE", "VARCHAR2(20)", False, False, "Fixed/Floating"),
        ("V_STATUS", "VARCHAR2(20)", False, False, "Contract status"),
        ("D_LAST_PAYMENT_DATE", "DATE", False, False, "Last payment date"),
        ("N_DAYS_PAST_DUE", "NUMBER(5)", False, False, "Days past due"),
        ("V_COLLATERAL_FLAG", "VARCHAR2(1)", False, False, "Has collateral Y/N"),
        ("D_APPROVE_DATE", "DATE", False, False, "Approval date"),
    ],
    "src_collateral": [
        ("N_COLLATERAL_SKEY", "NUMBER(20)", True, False, "Collateral surrogate key"),
        ("V_COLLATERAL_ID", "VARCHAR2(50)", False, False, "Collateral natural key"),
        ("V_CONTRACT_ID", "VARCHAR2(50)", False, True, "Linked contract"),
        ("V_COLLATERAL_TYPE", "VARCHAR2(30)", False, False, "Type of collateral"),
        ("N_MARKET_VALUE", "NUMBER(18,2)", False, False, "Market value"),
        ("N_HAIRCUT_PCT", "NUMBER(5,2)", False, False, "Haircut percentage"),
        ("N_ELIGIBLE_VALUE", "NUMBER(18,2)", False, False, "Eligible value"),
        ("V_CURRENCY_CODE", "VARCHAR2(3)", False, True, "Currency"),
        ("D_VALUATION_DATE", "DATE", False, False, "Last valuation date"),
    ],
    "src_counterparty": [
        ("N_PARTY_SKEY", "NUMBER(20)", True, False, "Party surrogate key"),
        ("V_PARTY_ID", "VARCHAR2(50)", False, False, "Party natural key"),
        ("V_PARTY_NAME", "VARCHAR2(200)", False, False, "Party name"),
        ("V_PARTY_TYPE", "VARCHAR2(30)", False, False, "Individual/Corporate"),
        ("V_COUNTRY_CODE", "VARCHAR2(3)", False, False, "Country of residence"),
        ("V_INDUSTRY_CODE", "VARCHAR2(10)", False, False, "Industry classification"),
        ("V_RISK_RATING", "VARCHAR2(10)", False, False, "Internal rating"),
        ("V_PD_GRADE", "VARCHAR2(5)", False, False, "PD grade"),
        ("N_ANNUAL_REVENUE", "NUMBER(18,2)", False, False, "Annual revenue"),
        ("D_RELATIONSHIP_START", "DATE", False, False, "Relationship start"),
    ],
    "src_gl_balances": [
        ("N_GL_SKEY", "NUMBER(20)", True, False, "GL surrogate key"),
        ("V_GL_ACCOUNT", "VARCHAR2(20)", False, False, "GL account number"),
        ("V_COST_CENTER", "VARCHAR2(20)", False, False, "Cost center"),
        ("V_ENTITY_CODE", "VARCHAR2(10)", False, False, "Legal entity"),
        ("D_ACCOUNTING_DATE", "DATE", False, False, "Accounting date"),
        ("N_DEBIT_AMOUNT", "NUMBER(18,2)", False, False, "Debit amount"),
        ("N_CREDIT_AMOUNT", "NUMBER(18,2)", False, False, "Credit amount"),
        ("N_BALANCE", "NUMBER(18,2)", False, False, "Net balance"),
        ("V_CURRENCY_CODE", "VARCHAR2(3)", False, True, "Currency"),
    ],
    "src_fx_rates": [
        ("V_FROM_CURRENCY", "VARCHAR2(3)", True, False, "From currency"),
        ("V_TO_CURRENCY", "VARCHAR2(3)", True, False, "To currency"),
        ("D_RATE_DATE", "DATE", True, False, "Rate date"),
        ("N_EXCHANGE_RATE", "NUMBER(18,10)", False, False, "Exchange rate"),
        ("V_RATE_TYPE", "VARCHAR2(20)", False, False, "Spot/Forward"),
    ],
    "src_audit_control": [
        ("N_LOAD_RUN_ID", "NUMBER(20)", True, False, "Load run ID"),
        ("V_TARGET_NAME", "VARCHAR2(100)", False, False, "Target table name"),
        ("D_FIC_MIS_DATE", "DATE", False, False, "MIS date"),
        ("V_DATA_LOAD_TYPE", "VARCHAR2(20)", False, False, "INBOUND/OUTBOUND"),
        ("D_START_TIME", "TIMESTAMP", False, False, "Load start time"),
        ("D_END_TIME", "TIMESTAMP", False, False, "Load end time"),
        ("N_ROWS_LOADED", "NUMBER(12)", False, False, "Rows loaded"),
        ("V_STATUS", "VARCHAR2(20)", False, False, "SUCCESS/FAILED"),
    ],

    # -------------------------------------------------------------------------
    # Staging Layer (stg_) - Cleansed and standardized
    # -------------------------------------------------------------------------
    "stg_loan_contracts": [
        ("N_CONTRACT_SKEY", "NUMBER(20)", True, False, "Contract surrogate key"),
        ("V_CONTRACT_ID", "VARCHAR2(50)", False, False, "Contract ID"),
        ("N_CUSTOMER_SKEY", "NUMBER(20)", False, True, "Customer surrogate key"),
        ("N_PRODUCT_SKEY", "NUMBER(20)", False, True, "Product surrogate key"),
        ("D_ORIGINATION_DATE", "DATE", False, False, "Origination date"),
        ("D_MATURITY_DATE", "DATE", False, False, "Maturity date"),
        ("N_ORIGINAL_AMOUNT", "NUMBER(18,2)", False, False, "Original amount"),
        ("N_OUTSTANDING_AMOUNT", "NUMBER(18,2)", False, False, "Outstanding amount"),
        ("N_OUTSTANDING_AMOUNT_RC", "NUMBER(18,2)", False, False, "Outstanding in reporting currency"),
        ("V_CURRENCY_CODE", "VARCHAR2(3)", False, True, "Currency"),
        ("N_INTEREST_RATE", "NUMBER(10,6)", False, False, "Interest rate"),
        ("V_RATE_TYPE", "VARCHAR2(20)", False, False, "Rate type"),
        ("V_STATUS", "VARCHAR2(20)", False, False, "Status"),
        ("N_DAYS_PAST_DUE", "NUMBER(5)", False, False, "Days past due"),
        ("V_PERFORMING_FLAG", "VARCHAR2(1)", False, False, "Performing Y/N"),
        ("D_APPROVE_DATE", "DATE", False, False, "Approval date"),
        ("N_LOAD_RUN_ID", "NUMBER(20)", False, False, "Load run ID"),
    ],
    "stg_collateral": [
        ("N_COLLATERAL_SKEY", "NUMBER(20)", True, False, "Collateral surrogate key"),
        ("V_COLLATERAL_ID", "VARCHAR2(50)", False, False, "Collateral ID"),
        ("N_CONTRACT_SKEY", "NUMBER(20)", False, True, "Contract surrogate key"),
        ("V_COLLATERAL_TYPE", "VARCHAR2(30)", False, False, "Collateral type"),
        ("N_MARKET_VALUE", "NUMBER(18,2)", False, False, "Market value"),
        ("N_MARKET_VALUE_RC", "NUMBER(18,2)", False, False, "Market value in RC"),
        ("N_HAIRCUT_PCT", "NUMBER(5,2)", False, False, "Haircut percentage"),
        ("N_ELIGIBLE_VALUE", "NUMBER(18,2)", False, False, "Eligible value"),
        ("N_ELIGIBLE_VALUE_RC", "NUMBER(18,2)", False, False, "Eligible value in RC"),
        ("D_VALUATION_DATE", "DATE", False, False, "Valuation date"),
        ("N_LOAD_RUN_ID", "NUMBER(20)", False, False, "Load run ID"),
    ],
    "stg_counterparty": [
        ("N_PARTY_SKEY", "NUMBER(20)", True, False, "Party surrogate key"),
        ("V_PARTY_ID", "VARCHAR2(50)", False, False, "Party ID"),
        ("V_PARTY_NAME", "VARCHAR2(200)", False, False, "Party name"),
        ("V_PARTY_TYPE", "VARCHAR2(30)", False, False, "Party type"),
        ("V_COUNTRY_CODE", "VARCHAR2(3)", False, False, "Country"),
        ("N_COUNTRY_SKEY", "NUMBER(20)", False, True, "Country surrogate key"),
        ("V_INDUSTRY_CODE", "VARCHAR2(10)", False, False, "Industry"),
        ("N_INDUSTRY_SKEY", "NUMBER(20)", False, True, "Industry surrogate key"),
        ("V_RISK_RATING", "VARCHAR2(10)", False, False, "Risk rating"),
        ("N_PD_PERCENT", "NUMBER(10,6)", False, False, "PD percentage"),
        ("N_LOAD_RUN_ID", "NUMBER(20)", False, False, "Load run ID"),
    ],
    "stg_gl_balances": [
        ("N_GL_SKEY", "NUMBER(20)", True, False, "GL surrogate key"),
        ("V_GL_ACCOUNT", "VARCHAR2(20)", False, False, "GL account"),
        ("N_GL_ACCOUNT_SKEY", "NUMBER(20)", False, True, "GL account surrogate"),
        ("V_COST_CENTER", "VARCHAR2(20)", False, False, "Cost center"),
        ("V_ENTITY_CODE", "VARCHAR2(10)", False, False, "Entity code"),
        ("N_ENTITY_SKEY", "NUMBER(20)", False, True, "Entity surrogate"),
        ("D_ACCOUNTING_DATE", "DATE", False, False, "Accounting date"),
        ("N_BALANCE", "NUMBER(18,2)", False, False, "Net balance"),
        ("N_BALANCE_RC", "NUMBER(18,2)", False, False, "Balance in RC"),
        ("N_LOAD_RUN_ID", "NUMBER(20)", False, False, "Load run ID"),
    ],

    # -------------------------------------------------------------------------
    # Dimension Layer (dim_) - Reference data
    # -------------------------------------------------------------------------
    "dim_counterparty": [
        ("N_PARTY_SKEY", "NUMBER(20)", True, False, "Party surrogate key"),
        ("V_PARTY_ID", "VARCHAR2(50)", False, False, "Party ID"),
        ("V_PARTY_NAME", "VARCHAR2(200)", False, False, "Party name"),
        ("V_PARTY_TYPE", "VARCHAR2(30)", False, False, "Party type"),
        ("N_COUNTRY_SKEY", "NUMBER(20)", False, True, "Country key"),
        ("N_INDUSTRY_SKEY", "NUMBER(20)", False, True, "Industry key"),
        ("V_RISK_RATING", "VARCHAR2(10)", False, False, "Risk rating"),
        ("N_PD_PERCENT", "NUMBER(10,6)", False, False, "PD percent"),
        ("V_SEGMENT", "VARCHAR2(30)", False, False, "Customer segment"),
        ("D_EFFECTIVE_FROM", "DATE", False, False, "SCD2 start"),
        ("D_EFFECTIVE_TO", "DATE", False, False, "SCD2 end"),
        ("V_CURRENT_FLAG", "VARCHAR2(1)", False, False, "Current record flag"),
    ],
    "dim_product": [
        ("N_PRODUCT_SKEY", "NUMBER(20)", True, False, "Product surrogate key"),
        ("V_PRODUCT_CODE", "VARCHAR2(20)", False, False, "Product code"),
        ("V_PRODUCT_NAME", "VARCHAR2(100)", False, False, "Product name"),
        ("V_PRODUCT_TYPE", "VARCHAR2(30)", False, False, "Product type"),
        ("V_PRODUCT_GROUP", "VARCHAR2(30)", False, False, "Product group"),
        ("V_ASSET_CLASS", "VARCHAR2(30)", False, False, "Asset class"),
        ("N_RISK_WEIGHT_PCT", "NUMBER(5,2)", False, False, "Risk weight"),
        ("D_EFFECTIVE_FROM", "DATE", False, False, "SCD2 start"),
        ("D_EFFECTIVE_TO", "DATE", False, False, "SCD2 end"),
    ],
    "dim_currency": [
        ("N_CURRENCY_SKEY", "NUMBER(20)", True, False, "Currency surrogate key"),
        ("V_CURRENCY_CODE", "VARCHAR2(3)", False, False, "ISO currency code"),
        ("V_CURRENCY_NAME", "VARCHAR2(50)", False, False, "Currency name"),
        ("V_REPORTING_FLAG", "VARCHAR2(1)", False, False, "Reporting currency Y/N"),
    ],
    "dim_geography": [
        ("N_COUNTRY_SKEY", "NUMBER(20)", True, False, "Country surrogate key"),
        ("V_COUNTRY_CODE", "VARCHAR2(3)", False, False, "ISO country code"),
        ("V_COUNTRY_NAME", "VARCHAR2(100)", False, False, "Country name"),
        ("V_REGION", "VARCHAR2(50)", False, False, "Region"),
        ("V_DEVELOPED_FLAG", "VARCHAR2(1)", False, False, "Developed market Y/N"),
    ],
    "dim_time": [
        ("N_DATE_SKEY", "NUMBER(8)", True, False, "Date key YYYYMMDD"),
        ("D_CALENDAR_DATE", "DATE", False, False, "Calendar date"),
        ("N_YEAR", "NUMBER(4)", False, False, "Year"),
        ("N_QUARTER", "NUMBER(1)", False, False, "Quarter"),
        ("N_MONTH", "NUMBER(2)", False, False, "Month"),
        ("V_MONTH_NAME", "VARCHAR2(20)", False, False, "Month name"),
        ("N_WEEK", "NUMBER(2)", False, False, "Week of year"),
        ("V_FISCAL_PERIOD", "VARCHAR2(20)", False, False, "Fiscal period"),
    ],
    "dim_gl_account": [
        ("N_GL_ACCOUNT_SKEY", "NUMBER(20)", True, False, "GL account surrogate"),
        ("V_GL_ACCOUNT", "VARCHAR2(20)", False, False, "GL account number"),
        ("V_GL_ACCOUNT_NAME", "VARCHAR2(100)", False, False, "Account name"),
        ("V_GL_CATEGORY", "VARCHAR2(30)", False, False, "Account category"),
        ("V_BS_PL_FLAG", "VARCHAR2(2)", False, False, "BS or PL"),
    ],

    # -------------------------------------------------------------------------
    # Fact Layer (fct_) - Transaction/Position facts
    # -------------------------------------------------------------------------
    "fct_loan_positions": [
        ("N_POSITION_SKEY", "NUMBER(20)", True, False, "Position surrogate key"),
        ("N_CONTRACT_SKEY", "NUMBER(20)", False, True, "Contract key"),
        ("N_CUSTOMER_SKEY", "NUMBER(20)", False, True, "Customer key"),
        ("N_PRODUCT_SKEY", "NUMBER(20)", False, True, "Product key"),
        ("N_DATE_SKEY", "NUMBER(8)", False, True, "Date key"),
        ("N_CURRENCY_SKEY", "NUMBER(20)", False, True, "Currency key"),
        ("N_OUTSTANDING_AMOUNT", "NUMBER(18,2)", False, False, "Outstanding amount"),
        ("N_OUTSTANDING_AMOUNT_RC", "NUMBER(18,2)", False, False, "Outstanding in RC"),
        ("N_INTEREST_RATE", "NUMBER(10,6)", False, False, "Interest rate"),
        ("N_ACCRUED_INTEREST", "NUMBER(18,2)", False, False, "Accrued interest"),
        ("N_DAYS_PAST_DUE", "NUMBER(5)", False, False, "Days past due"),
        ("V_PERFORMING_FLAG", "VARCHAR2(1)", False, False, "Performing flag"),
    ],
    "fct_collateral_positions": [
        ("N_COLL_POS_SKEY", "NUMBER(20)", True, False, "Collateral position key"),
        ("N_COLLATERAL_SKEY", "NUMBER(20)", False, True, "Collateral key"),
        ("N_CONTRACT_SKEY", "NUMBER(20)", False, True, "Contract key"),
        ("N_DATE_SKEY", "NUMBER(8)", False, True, "Date key"),
        ("N_MARKET_VALUE", "NUMBER(18,2)", False, False, "Market value"),
        ("N_MARKET_VALUE_RC", "NUMBER(18,2)", False, False, "Market value RC"),
        ("N_ELIGIBLE_VALUE", "NUMBER(18,2)", False, False, "Eligible value"),
        ("N_ELIGIBLE_VALUE_RC", "NUMBER(18,2)", False, False, "Eligible value RC"),
    ],
    "fct_non_sec_exposures": [
        ("N_EXPOSURE_SKEY", "NUMBER(20)", True, False, "Exposure surrogate key"),
        ("N_CONTRACT_SKEY", "NUMBER(20)", False, True, "Contract key"),
        ("N_CUSTOMER_SKEY", "NUMBER(20)", False, True, "Customer key"),
        ("N_PRODUCT_SKEY", "NUMBER(20)", False, True, "Product key"),
        ("N_DATE_SKEY", "NUMBER(8)", False, True, "Date key"),
        ("N_EAD", "NUMBER(18,2)", False, False, "Exposure at default"),
        ("N_EAD_RC", "NUMBER(18,2)", False, False, "EAD in reporting currency"),
        ("N_PD_PERCENT", "NUMBER(10,6)", False, False, "Probability of default"),
        ("N_LGD_PERCENT", "NUMBER(10,6)", False, False, "Loss given default"),
        ("N_RISK_WEIGHT", "NUMBER(5,2)", False, False, "Risk weight"),
        ("N_RWA", "NUMBER(18,2)", False, False, "Risk weighted assets"),
        ("N_ECL", "NUMBER(18,2)", False, False, "Expected credit loss"),
        ("V_STAGE", "VARCHAR2(10)", False, False, "IFRS9 stage"),
        ("D_DESJ_APPROVE_DATE", "DATE", False, False, "DESJ approval date"),
    ],
    "fct_gl_summary": [
        ("N_GL_SUMMARY_SKEY", "NUMBER(20)", True, False, "Summary surrogate key"),
        ("N_GL_ACCOUNT_SKEY", "NUMBER(20)", False, True, "GL account key"),
        ("N_ENTITY_SKEY", "NUMBER(20)", False, True, "Entity key"),
        ("N_DATE_SKEY", "NUMBER(8)", False, True, "Date key"),
        ("N_BALANCE", "NUMBER(18,2)", False, False, "Balance"),
        ("N_BALANCE_RC", "NUMBER(18,2)", False, False, "Balance in RC"),
    ],

    # -------------------------------------------------------------------------
    # Report Layer (rpt_) - Aggregated reports
    # -------------------------------------------------------------------------
    "rpt_exposure_summary": [
        ("N_DATE_SKEY", "NUMBER(8)", True, False, "Date key"),
        ("V_SEGMENT", "VARCHAR2(30)", True, False, "Segment"),
        ("V_PRODUCT_GROUP", "VARCHAR2(30)", True, False, "Product group"),
        ("N_TOTAL_EXPOSURE", "NUMBER(18,2)", False, False, "Total exposure"),
        ("N_TOTAL_RWA", "NUMBER(18,2)", False, False, "Total RWA"),
        ("N_TOTAL_ECL", "NUMBER(18,2)", False, False, "Total ECL"),
        ("N_CONTRACT_COUNT", "NUMBER(10)", False, False, "Contract count"),
        ("N_AVG_PD", "NUMBER(10,6)", False, False, "Average PD"),
        ("N_AVG_LGD", "NUMBER(10,6)", False, False, "Average LGD"),
    ],
    "rpt_risk_weighted_assets": [
        ("N_DATE_SKEY", "NUMBER(8)", True, False, "Date key"),
        ("V_ASSET_CLASS", "VARCHAR2(30)", True, False, "Asset class"),
        ("V_REGION", "VARCHAR2(50)", True, False, "Region"),
        ("N_EXPOSURE", "NUMBER(18,2)", False, False, "Exposure"),
        ("N_RWA", "NUMBER(18,2)", False, False, "RWA"),
        ("N_CAPITAL_REQUIREMENT", "NUMBER(18,2)", False, False, "Capital requirement"),
    ],
    "rpt_regulatory_capital": [
        ("N_DATE_SKEY", "NUMBER(8)", True, False, "Date key"),
        ("V_CAPITAL_TYPE", "VARCHAR2(30)", True, False, "Capital type"),
        ("N_AMOUNT", "NUMBER(18,2)", False, False, "Amount"),
        ("N_RATIO", "NUMBER(10,6)", False, False, "Ratio"),
    ],
    "rpt_customer_360": [
        ("N_CUSTOMER_SKEY", "NUMBER(20)", True, False, "Customer key"),
        ("V_PARTY_NAME", "VARCHAR2(200)", False, False, "Customer name"),
        ("V_SEGMENT", "VARCHAR2(30)", False, False, "Segment"),
        ("N_TOTAL_EXPOSURE", "NUMBER(18,2)", False, False, "Total exposure"),
        ("N_TOTAL_COLLATERAL", "NUMBER(18,2)", False, False, "Total collateral"),
        ("N_NET_EXPOSURE", "NUMBER(18,2)", False, False, "Net exposure"),
        ("N_CONTRACT_COUNT", "NUMBER(10)", False, False, "Contract count"),
        ("V_RISK_RATING", "VARCHAR2(10)", False, False, "Risk rating"),
        ("N_AVG_PD", "NUMBER(10,6)", False, False, "Average PD"),
    ],
}


def generate_data_model():
    """Generate a comprehensive data model with tables and columns."""
    rows = []
    for table_name, columns in TABLES.items():
        for col_name, data_type, is_pk, is_fk, description in columns:
            rows.append({
                "Table Name": table_name,
                "Column Name": col_name,
                "Data Type": data_type,
                "Is PK": is_pk,
                "Is FK": is_fk,
                "Description": description,
            })

    df = pd.DataFrame(rows)
    output_path = OUTPUT_DIR / "data_model.xlsx"
    df.to_excel(output_path, index=False)
    print(f"Created: {output_path} ({len(df)} columns)")
    return df


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
    """Helper to create a mapping row in the new format."""
    # Build trace path
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


def generate_mappings_v1():
    """Generate version 1 of field mappings in OFSAA format."""
    mappings = []

    # =========================================================================
    # T2T_STG_LOAN_CONTRACTS - Source to Staging for Loan Contracts
    # =========================================================================
    obj = "T2T_STG_LOAN_CONTRACTS"
    mappings.extend([
        create_mapping(obj, "stg_loan_contracts", "N_CONTRACT_SKEY", "src_loan_contracts", "N_CONTRACT_SKEY"),
        create_mapping(obj, "stg_loan_contracts", "V_CONTRACT_ID", "src_loan_contracts", "V_CONTRACT_ID"),
        create_mapping(obj, "stg_loan_contracts", "D_ORIGINATION_DATE", "src_loan_contracts", "D_ORIGINATION_DATE"),
        create_mapping(obj, "stg_loan_contracts", "D_MATURITY_DATE", "src_loan_contracts", "D_MATURITY_DATE"),
        create_mapping(obj, "stg_loan_contracts", "N_ORIGINAL_AMOUNT", "src_loan_contracts", "N_ORIGINAL_AMOUNT"),
        create_mapping(obj, "stg_loan_contracts", "N_OUTSTANDING_AMOUNT", "src_loan_contracts", "N_OUTSTANDING_AMOUNT"),
        create_mapping(obj, "stg_loan_contracts", "V_CURRENCY_CODE", "src_loan_contracts", "V_CURRENCY_CODE"),
        create_mapping(obj, "stg_loan_contracts", "N_INTEREST_RATE", "src_loan_contracts", "N_INTEREST_RATE"),
        create_mapping(obj, "stg_loan_contracts", "V_RATE_TYPE", "src_loan_contracts", "V_RATE_TYPE"),
        create_mapping(obj, "stg_loan_contracts", "V_STATUS", "src_loan_contracts", "V_STATUS"),
        create_mapping(obj, "stg_loan_contracts", "N_DAYS_PAST_DUE", "src_loan_contracts", "N_DAYS_PAST_DUE"),
        create_mapping(obj, "stg_loan_contracts", "D_APPROVE_DATE", "src_loan_contracts", "D_APPROVE_DATE"),
        # Derived field: Performing flag
        create_mapping(obj, "stg_loan_contracts", "V_PERFORMING_FLAG", "src_loan_contracts", "N_DAYS_PAST_DUE",
                      derived_expression="CASE WHEN N_DAYS_PAST_DUE <= 90 THEN 'Y' ELSE 'N' END"),
        # Derived field: Outstanding in reporting currency
        create_mapping(obj, "stg_loan_contracts", "N_OUTSTANDING_AMOUNT_RC", "src_loan_contracts", "N_OUTSTANDING_AMOUNT",
                      derived_expression="N_OUTSTANDING_AMOUNT * FX_RATE"),
        # Join to get customer surrogate key
        create_mapping(obj, "stg_loan_contracts", "N_CUSTOMER_SKEY", "stg_counterparty", "N_PARTY_SKEY",
                      usage_type="JOIN", usage_role="JOIN_KEY",
                      join_alias="CUST", join_filters="src_loan_contracts.V_CUSTOMER_ID = stg_counterparty.V_PARTY_ID"),
        # Join to audit control for load run ID
        create_mapping(obj, "stg_loan_contracts", "N_LOAD_RUN_ID", "src_audit_control", "N_LOAD_RUN_ID",
                      usage_type="JOIN", usage_role="JOIN_FILTER",
                      join_alias="AUDIT",
                      join_filters="src_audit_control.V_TARGET_NAME = 'STG_LOAN_CONTRACTS' AND src_audit_control.V_DATA_LOAD_TYPE = 'INBOUND'"),
    ])

    # =========================================================================
    # T2T_STG_COLLATERAL - Source to Staging for Collateral
    # =========================================================================
    obj = "T2T_STG_COLLATERAL"
    mappings.extend([
        create_mapping(obj, "stg_collateral", "N_COLLATERAL_SKEY", "src_collateral", "N_COLLATERAL_SKEY"),
        create_mapping(obj, "stg_collateral", "V_COLLATERAL_ID", "src_collateral", "V_COLLATERAL_ID"),
        create_mapping(obj, "stg_collateral", "V_COLLATERAL_TYPE", "src_collateral", "V_COLLATERAL_TYPE"),
        create_mapping(obj, "stg_collateral", "N_MARKET_VALUE", "src_collateral", "N_MARKET_VALUE"),
        create_mapping(obj, "stg_collateral", "N_HAIRCUT_PCT", "src_collateral", "N_HAIRCUT_PCT"),
        create_mapping(obj, "stg_collateral", "N_ELIGIBLE_VALUE", "src_collateral", "N_ELIGIBLE_VALUE"),
        create_mapping(obj, "stg_collateral", "D_VALUATION_DATE", "src_collateral", "D_VALUATION_DATE"),
        # Derived: Market value in reporting currency
        create_mapping(obj, "stg_collateral", "N_MARKET_VALUE_RC", "src_collateral", "N_MARKET_VALUE",
                      derived_expression="N_MARKET_VALUE * FX_RATE"),
        create_mapping(obj, "stg_collateral", "N_ELIGIBLE_VALUE_RC", "src_collateral", "N_ELIGIBLE_VALUE",
                      derived_expression="N_ELIGIBLE_VALUE * FX_RATE"),
        # Join to get contract surrogate key
        create_mapping(obj, "stg_collateral", "N_CONTRACT_SKEY", "stg_loan_contracts", "N_CONTRACT_SKEY",
                      usage_type="JOIN", usage_role="JOIN_KEY",
                      join_alias="LOAN", join_filters="src_collateral.V_CONTRACT_ID = stg_loan_contracts.V_CONTRACT_ID"),
        create_mapping(obj, "stg_collateral", "N_LOAD_RUN_ID", "src_audit_control", "N_LOAD_RUN_ID",
                      usage_type="JOIN", usage_role="JOIN_FILTER",
                      join_alias="AUDIT",
                      join_filters="src_audit_control.V_TARGET_NAME = 'STG_COLLATERAL' AND src_audit_control.V_DATA_LOAD_TYPE = 'INBOUND'"),
    ])

    # =========================================================================
    # T2T_STG_COUNTERPARTY - Source to Staging for Counterparty
    # =========================================================================
    obj = "T2T_STG_COUNTERPARTY"
    mappings.extend([
        create_mapping(obj, "stg_counterparty", "N_PARTY_SKEY", "src_counterparty", "N_PARTY_SKEY"),
        create_mapping(obj, "stg_counterparty", "V_PARTY_ID", "src_counterparty", "V_PARTY_ID"),
        create_mapping(obj, "stg_counterparty", "V_PARTY_NAME", "src_counterparty", "V_PARTY_NAME"),
        create_mapping(obj, "stg_counterparty", "V_PARTY_TYPE", "src_counterparty", "V_PARTY_TYPE"),
        create_mapping(obj, "stg_counterparty", "V_COUNTRY_CODE", "src_counterparty", "V_COUNTRY_CODE"),
        create_mapping(obj, "stg_counterparty", "V_INDUSTRY_CODE", "src_counterparty", "V_INDUSTRY_CODE"),
        create_mapping(obj, "stg_counterparty", "V_RISK_RATING", "src_counterparty", "V_RISK_RATING"),
        # Derived: Convert PD grade to percentage
        create_mapping(obj, "stg_counterparty", "N_PD_PERCENT", "src_counterparty", "V_PD_GRADE",
                      derived_expression="DECODE_PD_GRADE(V_PD_GRADE)"),
        # Join to get country surrogate key
        create_mapping(obj, "stg_counterparty", "N_COUNTRY_SKEY", "dim_geography", "N_COUNTRY_SKEY",
                      usage_type="JOIN", usage_role="JOIN_KEY",
                      join_alias="GEO", join_filters="src_counterparty.V_COUNTRY_CODE = dim_geography.V_COUNTRY_CODE"),
        create_mapping(obj, "stg_counterparty", "N_LOAD_RUN_ID", "src_audit_control", "N_LOAD_RUN_ID",
                      usage_type="JOIN", usage_role="JOIN_FILTER",
                      join_alias="AUDIT",
                      join_filters="src_audit_control.V_TARGET_NAME = 'STG_COUNTERPARTY' AND src_audit_control.V_DATA_LOAD_TYPE = 'INBOUND'"),
    ])

    # =========================================================================
    # T2T_STG_GL_BALANCES - Source to Staging for GL Balances
    # =========================================================================
    obj = "T2T_STG_GL_BALANCES"
    mappings.extend([
        create_mapping(obj, "stg_gl_balances", "N_GL_SKEY", "src_gl_balances", "N_GL_SKEY"),
        create_mapping(obj, "stg_gl_balances", "V_GL_ACCOUNT", "src_gl_balances", "V_GL_ACCOUNT"),
        create_mapping(obj, "stg_gl_balances", "V_COST_CENTER", "src_gl_balances", "V_COST_CENTER"),
        create_mapping(obj, "stg_gl_balances", "V_ENTITY_CODE", "src_gl_balances", "V_ENTITY_CODE"),
        create_mapping(obj, "stg_gl_balances", "D_ACCOUNTING_DATE", "src_gl_balances", "D_ACCOUNTING_DATE"),
        create_mapping(obj, "stg_gl_balances", "N_BALANCE", "src_gl_balances", "N_BALANCE",
                      derived_expression="N_DEBIT_AMOUNT - N_CREDIT_AMOUNT"),
        create_mapping(obj, "stg_gl_balances", "N_BALANCE_RC", "src_gl_balances", "N_BALANCE",
                      derived_expression="(N_DEBIT_AMOUNT - N_CREDIT_AMOUNT) * FX_RATE"),
        # Join to get GL account surrogate key
        create_mapping(obj, "stg_gl_balances", "N_GL_ACCOUNT_SKEY", "dim_gl_account", "N_GL_ACCOUNT_SKEY",
                      usage_type="JOIN", usage_role="JOIN_KEY",
                      join_alias="GL", join_filters="src_gl_balances.V_GL_ACCOUNT = dim_gl_account.V_GL_ACCOUNT"),
        create_mapping(obj, "stg_gl_balances", "N_LOAD_RUN_ID", "src_audit_control", "N_LOAD_RUN_ID",
                      usage_type="JOIN", usage_role="JOIN_FILTER",
                      join_alias="AUDIT",
                      join_filters="src_audit_control.V_TARGET_NAME = 'STG_GL_BALANCES' AND src_audit_control.V_DATA_LOAD_TYPE = 'INBOUND'"),
    ])

    # =========================================================================
    # SQL_DIM_COUNTERPARTY - Staging to Dimension for Counterparty
    # =========================================================================
    obj = "SQL_DIM_COUNTERPARTY"
    mappings.extend([
        create_mapping(obj, "dim_counterparty", "N_PARTY_SKEY", "stg_counterparty", "N_PARTY_SKEY"),
        create_mapping(obj, "dim_counterparty", "V_PARTY_ID", "stg_counterparty", "V_PARTY_ID"),
        create_mapping(obj, "dim_counterparty", "V_PARTY_NAME", "stg_counterparty", "V_PARTY_NAME"),
        create_mapping(obj, "dim_counterparty", "V_PARTY_TYPE", "stg_counterparty", "V_PARTY_TYPE"),
        create_mapping(obj, "dim_counterparty", "N_COUNTRY_SKEY", "stg_counterparty", "N_COUNTRY_SKEY"),
        create_mapping(obj, "dim_counterparty", "V_RISK_RATING", "stg_counterparty", "V_RISK_RATING"),
        create_mapping(obj, "dim_counterparty", "N_PD_PERCENT", "stg_counterparty", "N_PD_PERCENT"),
        # Derived: Customer segment based on party type and rating
        create_mapping(obj, "dim_counterparty", "V_SEGMENT", "stg_counterparty", "V_PARTY_TYPE",
                      derived_expression="CASE WHEN V_PARTY_TYPE = 'CORPORATE' AND V_RISK_RATING IN ('AAA','AA','A') THEN 'PRIME' ELSE 'STANDARD' END"),
        # SCD2 fields
        create_mapping(obj, "dim_counterparty", "D_EFFECTIVE_FROM", "stg_counterparty", "N_LOAD_RUN_ID",
                      source_type="DERIVED", derived_expression="SYSDATE"),
        create_mapping(obj, "dim_counterparty", "V_CURRENT_FLAG", "stg_counterparty", "N_LOAD_RUN_ID",
                      source_type="CONSTANT", constant_value="Y", derived_expression="'Y'"),
    ])

    # =========================================================================
    # T2T_DIM_PRODUCT - Product dimension load
    # =========================================================================
    obj = "T2T_DIM_PRODUCT"
    mappings.extend([
        create_mapping(obj, "dim_product", "N_PRODUCT_SKEY", "src_loan_contracts", "V_PRODUCT_CODE",
                      source_type="DERIVED", derived_expression="SEQUENCE_PRODUCT.NEXTVAL"),
        create_mapping(obj, "dim_product", "V_PRODUCT_CODE", "src_loan_contracts", "V_PRODUCT_CODE"),
        create_mapping(obj, "dim_product", "V_PRODUCT_NAME", "src_loan_contracts", "V_PRODUCT_CODE",
                      derived_expression="LOOKUP_PRODUCT_NAME(V_PRODUCT_CODE)"),
        create_mapping(obj, "dim_product", "V_PRODUCT_TYPE", "src_loan_contracts", "V_PRODUCT_CODE",
                      derived_expression="SUBSTR(V_PRODUCT_CODE, 1, 2)"),
        create_mapping(obj, "dim_product", "V_ASSET_CLASS", "src_loan_contracts", "V_PRODUCT_CODE",
                      derived_expression="GET_ASSET_CLASS(V_PRODUCT_CODE)"),
        create_mapping(obj, "dim_product", "N_RISK_WEIGHT_PCT", "src_loan_contracts", "V_PRODUCT_CODE",
                      derived_expression="GET_RISK_WEIGHT(V_PRODUCT_CODE)"),
    ])

    # =========================================================================
    # T2T_FCT_LOAN_POSITIONS - Fact table for loan positions
    # =========================================================================
    obj = "T2T_FCT_LOAN_POSITIONS"
    mappings.extend([
        create_mapping(obj, "fct_loan_positions", "N_POSITION_SKEY", "stg_loan_contracts", "N_CONTRACT_SKEY",
                      source_type="DERIVED", derived_expression="SEQUENCE_POSITION.NEXTVAL"),
        create_mapping(obj, "fct_loan_positions", "N_CONTRACT_SKEY", "stg_loan_contracts", "N_CONTRACT_SKEY"),
        create_mapping(obj, "fct_loan_positions", "N_CUSTOMER_SKEY", "stg_loan_contracts", "N_CUSTOMER_SKEY"),
        create_mapping(obj, "fct_loan_positions", "N_OUTSTANDING_AMOUNT", "stg_loan_contracts", "N_OUTSTANDING_AMOUNT"),
        create_mapping(obj, "fct_loan_positions", "N_OUTSTANDING_AMOUNT_RC", "stg_loan_contracts", "N_OUTSTANDING_AMOUNT_RC"),
        create_mapping(obj, "fct_loan_positions", "N_INTEREST_RATE", "stg_loan_contracts", "N_INTEREST_RATE"),
        create_mapping(obj, "fct_loan_positions", "N_DAYS_PAST_DUE", "stg_loan_contracts", "N_DAYS_PAST_DUE"),
        create_mapping(obj, "fct_loan_positions", "V_PERFORMING_FLAG", "stg_loan_contracts", "V_PERFORMING_FLAG"),
        # Accrued interest calculation
        create_mapping(obj, "fct_loan_positions", "N_ACCRUED_INTEREST", "stg_loan_contracts", "N_OUTSTANDING_AMOUNT",
                      derived_expression="N_OUTSTANDING_AMOUNT * N_INTEREST_RATE * DAYS_IN_PERIOD / 360"),
        # Join to get product key
        create_mapping(obj, "fct_loan_positions", "N_PRODUCT_SKEY", "dim_product", "N_PRODUCT_SKEY",
                      usage_type="JOIN", usage_role="JOIN_KEY",
                      join_alias="PROD", join_filters="stg_loan_contracts.V_PRODUCT_CODE = dim_product.V_PRODUCT_CODE"),
        # Join to get date key
        create_mapping(obj, "fct_loan_positions", "N_DATE_SKEY", "dim_time", "N_DATE_SKEY",
                      usage_type="JOIN", usage_role="JOIN_KEY",
                      join_alias="TIME", join_filters="TRUNC(SYSDATE) = dim_time.D_CALENDAR_DATE"),
        # Join to get currency key
        create_mapping(obj, "fct_loan_positions", "N_CURRENCY_SKEY", "dim_currency", "N_CURRENCY_SKEY",
                      usage_type="JOIN", usage_role="JOIN_KEY",
                      join_alias="CCY", join_filters="stg_loan_contracts.V_CURRENCY_CODE = dim_currency.V_CURRENCY_CODE"),
    ])

    # =========================================================================
    # T2T_FCT_COLLATERAL_POSITIONS - Fact table for collateral positions
    # =========================================================================
    obj = "T2T_FCT_COLLATERAL_POSITIONS"
    mappings.extend([
        create_mapping(obj, "fct_collateral_positions", "N_COLL_POS_SKEY", "stg_collateral", "N_COLLATERAL_SKEY",
                      source_type="DERIVED", derived_expression="SEQUENCE_COLL_POS.NEXTVAL"),
        create_mapping(obj, "fct_collateral_positions", "N_COLLATERAL_SKEY", "stg_collateral", "N_COLLATERAL_SKEY"),
        create_mapping(obj, "fct_collateral_positions", "N_CONTRACT_SKEY", "stg_collateral", "N_CONTRACT_SKEY"),
        create_mapping(obj, "fct_collateral_positions", "N_MARKET_VALUE", "stg_collateral", "N_MARKET_VALUE"),
        create_mapping(obj, "fct_collateral_positions", "N_MARKET_VALUE_RC", "stg_collateral", "N_MARKET_VALUE_RC"),
        create_mapping(obj, "fct_collateral_positions", "N_ELIGIBLE_VALUE", "stg_collateral", "N_ELIGIBLE_VALUE"),
        create_mapping(obj, "fct_collateral_positions", "N_ELIGIBLE_VALUE_RC", "stg_collateral", "N_ELIGIBLE_VALUE_RC"),
        # Join to get date key
        create_mapping(obj, "fct_collateral_positions", "N_DATE_SKEY", "dim_time", "N_DATE_SKEY",
                      usage_type="JOIN", usage_role="JOIN_KEY",
                      join_alias="TIME", join_filters="TRUNC(SYSDATE) = dim_time.D_CALENDAR_DATE"),
    ])

    # =========================================================================
    # T2T_FCT_NON_SEC_EXPOSURES - Non-securitized exposures fact
    # =========================================================================
    obj = "T2T_FCT_NON_SEC_EXPOSURES"
    mappings.extend([
        create_mapping(obj, "fct_non_sec_exposures", "N_EXPOSURE_SKEY", "fct_loan_positions", "N_POSITION_SKEY",
                      source_type="DERIVED", derived_expression="SEQUENCE_EXPOSURE.NEXTVAL"),
        create_mapping(obj, "fct_non_sec_exposures", "N_CONTRACT_SKEY", "fct_loan_positions", "N_CONTRACT_SKEY"),
        create_mapping(obj, "fct_non_sec_exposures", "N_CUSTOMER_SKEY", "fct_loan_positions", "N_CUSTOMER_SKEY"),
        create_mapping(obj, "fct_non_sec_exposures", "N_PRODUCT_SKEY", "fct_loan_positions", "N_PRODUCT_SKEY"),
        create_mapping(obj, "fct_non_sec_exposures", "N_DATE_SKEY", "fct_loan_positions", "N_DATE_SKEY"),
        # EAD calculation
        create_mapping(obj, "fct_non_sec_exposures", "N_EAD", "fct_loan_positions", "N_OUTSTANDING_AMOUNT",
                      derived_expression="N_OUTSTANDING_AMOUNT + N_ACCRUED_INTEREST"),
        create_mapping(obj, "fct_non_sec_exposures", "N_EAD_RC", "fct_loan_positions", "N_OUTSTANDING_AMOUNT_RC",
                      derived_expression="N_OUTSTANDING_AMOUNT_RC + (N_ACCRUED_INTEREST * FX_RATE)"),
        # Get PD from counterparty dimension
        create_mapping(obj, "fct_non_sec_exposures", "N_PD_PERCENT", "dim_counterparty", "N_PD_PERCENT",
                      usage_type="JOIN", usage_role="JOIN_KEY",
                      join_alias="CPTY", join_filters="fct_loan_positions.N_CUSTOMER_SKEY = dim_counterparty.N_PARTY_SKEY"),
        # LGD based on collateral
        create_mapping(obj, "fct_non_sec_exposures", "N_LGD_PERCENT", "fct_collateral_positions", "N_ELIGIBLE_VALUE",
                      usage_type="JOIN", usage_role="VALUE",
                      derived_expression="CASE WHEN N_ELIGIBLE_VALUE >= N_EAD THEN 0.25 ELSE 0.45 END",
                      join_alias="COLL", join_filters="fct_loan_positions.N_CONTRACT_SKEY = fct_collateral_positions.N_CONTRACT_SKEY"),
        # Risk weight from product
        create_mapping(obj, "fct_non_sec_exposures", "N_RISK_WEIGHT", "dim_product", "N_RISK_WEIGHT_PCT",
                      usage_type="JOIN", usage_role="JOIN_KEY",
                      join_alias="PROD", join_filters="fct_loan_positions.N_PRODUCT_SKEY = dim_product.N_PRODUCT_SKEY"),
        # RWA calculation
        create_mapping(obj, "fct_non_sec_exposures", "N_RWA", "fct_loan_positions", "N_OUTSTANDING_AMOUNT_RC",
                      derived_expression="N_EAD_RC * N_RISK_WEIGHT / 100"),
        # ECL calculation
        create_mapping(obj, "fct_non_sec_exposures", "N_ECL", "fct_loan_positions", "N_OUTSTANDING_AMOUNT_RC",
                      derived_expression="N_EAD_RC * N_PD_PERCENT * N_LGD_PERCENT"),
        # IFRS9 Stage
        create_mapping(obj, "fct_non_sec_exposures", "V_STAGE", "fct_loan_positions", "N_DAYS_PAST_DUE",
                      derived_expression="CASE WHEN N_DAYS_PAST_DUE = 0 THEN 'STAGE1' WHEN N_DAYS_PAST_DUE <= 90 THEN 'STAGE2' ELSE 'STAGE3' END"),
        # DESJ Approval Date from staging
        create_mapping(obj, "fct_non_sec_exposures", "D_DESJ_APPROVE_DATE", "stg_loan_contracts", "D_APPROVE_DATE",
                      usage_type="JOIN", usage_role="VALUE",
                      join_alias="STG", join_filters="fct_loan_positions.N_CONTRACT_SKEY = stg_loan_contracts.N_CONTRACT_SKEY"),
    ])

    # =========================================================================
    # SQL_FCT_GL_SUMMARY - GL Summary fact
    # =========================================================================
    obj = "SQL_FCT_GL_SUMMARY"
    mappings.extend([
        create_mapping(obj, "fct_gl_summary", "N_GL_SUMMARY_SKEY", "stg_gl_balances", "N_GL_SKEY",
                      source_type="DERIVED", derived_expression="SEQUENCE_GL_SUMMARY.NEXTVAL"),
        create_mapping(obj, "fct_gl_summary", "N_GL_ACCOUNT_SKEY", "stg_gl_balances", "N_GL_ACCOUNT_SKEY"),
        create_mapping(obj, "fct_gl_summary", "N_BALANCE", "stg_gl_balances", "N_BALANCE",
                      derived_expression="SUM(N_BALANCE)"),
        create_mapping(obj, "fct_gl_summary", "N_BALANCE_RC", "stg_gl_balances", "N_BALANCE_RC",
                      derived_expression="SUM(N_BALANCE_RC)"),
        # Join to get date key
        create_mapping(obj, "fct_gl_summary", "N_DATE_SKEY", "dim_time", "N_DATE_SKEY",
                      usage_type="JOIN", usage_role="JOIN_KEY",
                      join_alias="TIME", join_filters="stg_gl_balances.D_ACCOUNTING_DATE = dim_time.D_CALENDAR_DATE"),
    ])

    # =========================================================================
    # DIH_RPT_EXPOSURE_SUMMARY - Exposure Summary Report
    # =========================================================================
    obj = "DIH_RPT_EXPOSURE_SUMMARY"
    mappings.extend([
        create_mapping(obj, "rpt_exposure_summary", "N_DATE_SKEY", "fct_non_sec_exposures", "N_DATE_SKEY"),
        create_mapping(obj, "rpt_exposure_summary", "V_SEGMENT", "dim_counterparty", "V_SEGMENT",
                      usage_type="JOIN", usage_role="VALUE",
                      join_alias="CPTY", join_filters="fct_non_sec_exposures.N_CUSTOMER_SKEY = dim_counterparty.N_PARTY_SKEY"),
        create_mapping(obj, "rpt_exposure_summary", "V_PRODUCT_GROUP", "dim_product", "V_PRODUCT_GROUP",
                      usage_type="JOIN", usage_role="VALUE",
                      join_alias="PROD", join_filters="fct_non_sec_exposures.N_PRODUCT_SKEY = dim_product.N_PRODUCT_SKEY"),
        create_mapping(obj, "rpt_exposure_summary", "N_TOTAL_EXPOSURE", "fct_non_sec_exposures", "N_EAD_RC",
                      derived_expression="SUM(N_EAD_RC)"),
        create_mapping(obj, "rpt_exposure_summary", "N_TOTAL_RWA", "fct_non_sec_exposures", "N_RWA",
                      derived_expression="SUM(N_RWA)"),
        create_mapping(obj, "rpt_exposure_summary", "N_TOTAL_ECL", "fct_non_sec_exposures", "N_ECL",
                      derived_expression="SUM(N_ECL)"),
        create_mapping(obj, "rpt_exposure_summary", "N_CONTRACT_COUNT", "fct_non_sec_exposures", "N_CONTRACT_SKEY",
                      derived_expression="COUNT(DISTINCT N_CONTRACT_SKEY)"),
        create_mapping(obj, "rpt_exposure_summary", "N_AVG_PD", "fct_non_sec_exposures", "N_PD_PERCENT",
                      derived_expression="AVG(N_PD_PERCENT)"),
        create_mapping(obj, "rpt_exposure_summary", "N_AVG_LGD", "fct_non_sec_exposures", "N_LGD_PERCENT",
                      derived_expression="AVG(N_LGD_PERCENT)"),
    ])

    # =========================================================================
    # DIH_RPT_RWA - Risk Weighted Assets Report
    # =========================================================================
    obj = "DIH_RPT_RWA"
    mappings.extend([
        create_mapping(obj, "rpt_risk_weighted_assets", "N_DATE_SKEY", "fct_non_sec_exposures", "N_DATE_SKEY"),
        create_mapping(obj, "rpt_risk_weighted_assets", "V_ASSET_CLASS", "dim_product", "V_ASSET_CLASS",
                      usage_type="JOIN", usage_role="VALUE",
                      join_alias="PROD", join_filters="fct_non_sec_exposures.N_PRODUCT_SKEY = dim_product.N_PRODUCT_SKEY"),
        create_mapping(obj, "rpt_risk_weighted_assets", "V_REGION", "dim_geography", "V_REGION",
                      usage_type="JOIN", usage_role="VALUE",
                      join_alias="GEO",
                      join_filters="dim_counterparty.N_COUNTRY_SKEY = dim_geography.N_COUNTRY_SKEY"),
        create_mapping(obj, "rpt_risk_weighted_assets", "N_EXPOSURE", "fct_non_sec_exposures", "N_EAD_RC",
                      derived_expression="SUM(N_EAD_RC)"),
        create_mapping(obj, "rpt_risk_weighted_assets", "N_RWA", "fct_non_sec_exposures", "N_RWA",
                      derived_expression="SUM(N_RWA)"),
        create_mapping(obj, "rpt_risk_weighted_assets", "N_CAPITAL_REQUIREMENT", "fct_non_sec_exposures", "N_RWA",
                      derived_expression="SUM(N_RWA) * 0.08"),
    ])

    # =========================================================================
    # DIH_RPT_CUSTOMER_360 - Customer 360 Report
    # =========================================================================
    obj = "DIH_RPT_CUSTOMER_360"
    mappings.extend([
        create_mapping(obj, "rpt_customer_360", "N_CUSTOMER_SKEY", "dim_counterparty", "N_PARTY_SKEY"),
        create_mapping(obj, "rpt_customer_360", "V_PARTY_NAME", "dim_counterparty", "V_PARTY_NAME"),
        create_mapping(obj, "rpt_customer_360", "V_SEGMENT", "dim_counterparty", "V_SEGMENT"),
        create_mapping(obj, "rpt_customer_360", "V_RISK_RATING", "dim_counterparty", "V_RISK_RATING"),
        create_mapping(obj, "rpt_customer_360", "N_AVG_PD", "dim_counterparty", "N_PD_PERCENT"),
        # Aggregations from exposure fact
        create_mapping(obj, "rpt_customer_360", "N_TOTAL_EXPOSURE", "fct_non_sec_exposures", "N_EAD_RC",
                      usage_type="JOIN", usage_role="VALUE",
                      derived_expression="SUM(N_EAD_RC)",
                      join_alias="EXP", join_filters="dim_counterparty.N_PARTY_SKEY = fct_non_sec_exposures.N_CUSTOMER_SKEY"),
        create_mapping(obj, "rpt_customer_360", "N_CONTRACT_COUNT", "fct_non_sec_exposures", "N_CONTRACT_SKEY",
                      usage_type="JOIN", usage_role="VALUE",
                      derived_expression="COUNT(DISTINCT N_CONTRACT_SKEY)",
                      join_alias="EXP", join_filters="dim_counterparty.N_PARTY_SKEY = fct_non_sec_exposures.N_CUSTOMER_SKEY"),
        # Collateral from collateral fact
        create_mapping(obj, "rpt_customer_360", "N_TOTAL_COLLATERAL", "fct_collateral_positions", "N_ELIGIBLE_VALUE_RC",
                      usage_type="JOIN", usage_role="VALUE",
                      derived_expression="SUM(N_ELIGIBLE_VALUE_RC)",
                      join_alias="COLL", join_filters="fct_non_sec_exposures.N_CONTRACT_SKEY = fct_collateral_positions.N_CONTRACT_SKEY"),
        # Net exposure calculation
        create_mapping(obj, "rpt_customer_360", "N_NET_EXPOSURE", "fct_non_sec_exposures", "N_EAD_RC",
                      derived_expression="SUM(N_EAD_RC) - COALESCE(SUM(N_ELIGIBLE_VALUE_RC), 0)"),
    ])

    # Convert to DataFrame
    df = pd.DataFrame(mappings)

    # Reorder columns to match expected format
    column_order = [
        "object_name", "destination_table", "destination_field", "usage_type", "usage_role",
        "source_type", "source_table", "source_field", "constant_value", "derived_output",
        "derived_expression", "join_alias", "join_keys", "join_filters", "dm_match", "trace_path", "notes"
    ]
    df = df[column_order]

    output_path = OUTPUT_DIR / "mappings_v1.xlsx"
    df.to_excel(output_path, index=False)
    print(f"Created: {output_path} ({len(df)} mappings)")
    return df


def generate_mappings_v2():
    """Generate version 2 with changes from v1 (for delta testing)."""
    # Start with V1 and make modifications
    v1_df = generate_mappings_v1_data()

    # Convert to list of dicts for easier manipulation
    mappings = v1_df.to_dict('records')

    # Modifications for V2:

    # 1. MODIFIED: Change the performing flag logic in T2T_STG_LOAN_CONTRACTS
    for m in mappings:
        if m["object_name"] == "T2T_STG_LOAN_CONTRACTS" and m["destination_field"] == "V_PERFORMING_FLAG":
            m["derived_expression"] = "CASE WHEN N_DAYS_PAST_DUE <= 30 THEN 'Y' WHEN N_DAYS_PAST_DUE <= 90 THEN 'W' ELSE 'N' END"
            m["notes"] = "MODIFIED: Added Warning status for 31-90 days"

    # 2. MODIFIED: Change IFRS9 stage logic
    for m in mappings:
        if m["object_name"] == "T2T_FCT_NON_SEC_EXPOSURES" and m["destination_field"] == "V_STAGE":
            m["derived_expression"] = "CASE WHEN N_DAYS_PAST_DUE = 0 AND N_PD_PERCENT < 0.01 THEN 'STAGE1' WHEN N_DAYS_PAST_DUE <= 30 THEN 'STAGE2' ELSE 'STAGE3' END"
            m["notes"] = "MODIFIED: Added PD condition for Stage 1"

    # 3. ADDED: New field in staging - credit score band
    mappings.append(create_mapping(
        "T2T_STG_COUNTERPARTY", "stg_counterparty", "V_CREDIT_SCORE_BAND",
        "src_counterparty", "V_RISK_RATING",
        derived_expression="CASE WHEN V_RISK_RATING IN ('AAA','AA') THEN 'PRIME' WHEN V_RISK_RATING IN ('A','BBB') THEN 'NEAR_PRIME' ELSE 'SUB_PRIME' END",
        notes="ADDED: New credit score band classification"
    ))

    # 4. ADDED: New report - Regulatory Capital
    obj = "DIH_RPT_REG_CAPITAL"
    mappings.extend([
        create_mapping(obj, "rpt_regulatory_capital", "N_DATE_SKEY", "fct_non_sec_exposures", "N_DATE_SKEY",
                      notes="ADDED: New regulatory capital report"),
        create_mapping(obj, "rpt_regulatory_capital", "V_CAPITAL_TYPE", "fct_non_sec_exposures", "N_RWA",
                      source_type="CONSTANT", constant_value="CET1", derived_expression="'CET1'"),
        create_mapping(obj, "rpt_regulatory_capital", "N_AMOUNT", "fct_gl_summary", "N_BALANCE_RC",
                      usage_type="JOIN", usage_role="VALUE",
                      derived_expression="SUM(N_BALANCE_RC)",
                      join_alias="GL", join_filters="dim_gl_account.V_GL_CATEGORY = 'CAPITAL'"),
        create_mapping(obj, "rpt_regulatory_capital", "N_RATIO", "fct_non_sec_exposures", "N_RWA",
                      derived_expression="SUM(GL.N_BALANCE_RC) / SUM(N_RWA)"),
    ])

    # 5. REMOVED: Remove one of the exposure summary mappings (simulate deletion)
    mappings = [m for m in mappings if not (
        m["object_name"] == "DIH_RPT_EXPOSURE_SUMMARY" and
        m["destination_field"] == "N_AVG_LGD"
    )]

    # 6. MODIFIED: Change join filter in RWA report
    for m in mappings:
        if m["object_name"] == "DIH_RPT_RWA" and m["destination_field"] == "V_REGION":
            m["join_filters"] = "dim_counterparty.N_COUNTRY_SKEY = dim_geography.N_COUNTRY_SKEY AND dim_geography.V_DEVELOPED_FLAG = 'Y'"
            m["notes"] = "MODIFIED: Filter to developed markets only"

    df = pd.DataFrame(mappings)

    # Reorder columns
    column_order = [
        "object_name", "destination_table", "destination_field", "usage_type", "usage_role",
        "source_type", "source_table", "source_field", "constant_value", "derived_output",
        "derived_expression", "join_alias", "join_keys", "join_filters", "dm_match", "trace_path", "notes"
    ]
    df = df[column_order]

    output_path = OUTPUT_DIR / "mappings_v2.xlsx"
    df.to_excel(output_path, index=False)
    print(f"Created: {output_path} ({len(df)} mappings)")
    return df


def generate_mappings_v1_data():
    """Generate V1 mappings and return DataFrame (helper for V2)."""
    # This duplicates the logic from generate_mappings_v1 but returns the DataFrame
    # without saving to file
    mappings = []

    # Copy all the mapping generation from generate_mappings_v1
    # T2T_STG_LOAN_CONTRACTS
    obj = "T2T_STG_LOAN_CONTRACTS"
    mappings.extend([
        create_mapping(obj, "stg_loan_contracts", "N_CONTRACT_SKEY", "src_loan_contracts", "N_CONTRACT_SKEY"),
        create_mapping(obj, "stg_loan_contracts", "V_CONTRACT_ID", "src_loan_contracts", "V_CONTRACT_ID"),
        create_mapping(obj, "stg_loan_contracts", "D_ORIGINATION_DATE", "src_loan_contracts", "D_ORIGINATION_DATE"),
        create_mapping(obj, "stg_loan_contracts", "D_MATURITY_DATE", "src_loan_contracts", "D_MATURITY_DATE"),
        create_mapping(obj, "stg_loan_contracts", "N_ORIGINAL_AMOUNT", "src_loan_contracts", "N_ORIGINAL_AMOUNT"),
        create_mapping(obj, "stg_loan_contracts", "N_OUTSTANDING_AMOUNT", "src_loan_contracts", "N_OUTSTANDING_AMOUNT"),
        create_mapping(obj, "stg_loan_contracts", "V_CURRENCY_CODE", "src_loan_contracts", "V_CURRENCY_CODE"),
        create_mapping(obj, "stg_loan_contracts", "N_INTEREST_RATE", "src_loan_contracts", "N_INTEREST_RATE"),
        create_mapping(obj, "stg_loan_contracts", "V_RATE_TYPE", "src_loan_contracts", "V_RATE_TYPE"),
        create_mapping(obj, "stg_loan_contracts", "V_STATUS", "src_loan_contracts", "V_STATUS"),
        create_mapping(obj, "stg_loan_contracts", "N_DAYS_PAST_DUE", "src_loan_contracts", "N_DAYS_PAST_DUE"),
        create_mapping(obj, "stg_loan_contracts", "D_APPROVE_DATE", "src_loan_contracts", "D_APPROVE_DATE"),
        create_mapping(obj, "stg_loan_contracts", "V_PERFORMING_FLAG", "src_loan_contracts", "N_DAYS_PAST_DUE",
                      derived_expression="CASE WHEN N_DAYS_PAST_DUE <= 90 THEN 'Y' ELSE 'N' END"),
        create_mapping(obj, "stg_loan_contracts", "N_OUTSTANDING_AMOUNT_RC", "src_loan_contracts", "N_OUTSTANDING_AMOUNT",
                      derived_expression="N_OUTSTANDING_AMOUNT * FX_RATE"),
        create_mapping(obj, "stg_loan_contracts", "N_CUSTOMER_SKEY", "stg_counterparty", "N_PARTY_SKEY",
                      usage_type="JOIN", usage_role="JOIN_KEY",
                      join_alias="CUST", join_filters="src_loan_contracts.V_CUSTOMER_ID = stg_counterparty.V_PARTY_ID"),
        create_mapping(obj, "stg_loan_contracts", "N_LOAD_RUN_ID", "src_audit_control", "N_LOAD_RUN_ID",
                      usage_type="JOIN", usage_role="JOIN_FILTER",
                      join_alias="AUDIT",
                      join_filters="src_audit_control.V_TARGET_NAME = 'STG_LOAN_CONTRACTS' AND src_audit_control.V_DATA_LOAD_TYPE = 'INBOUND'"),
    ])

    # T2T_STG_COLLATERAL
    obj = "T2T_STG_COLLATERAL"
    mappings.extend([
        create_mapping(obj, "stg_collateral", "N_COLLATERAL_SKEY", "src_collateral", "N_COLLATERAL_SKEY"),
        create_mapping(obj, "stg_collateral", "V_COLLATERAL_ID", "src_collateral", "V_COLLATERAL_ID"),
        create_mapping(obj, "stg_collateral", "V_COLLATERAL_TYPE", "src_collateral", "V_COLLATERAL_TYPE"),
        create_mapping(obj, "stg_collateral", "N_MARKET_VALUE", "src_collateral", "N_MARKET_VALUE"),
        create_mapping(obj, "stg_collateral", "N_HAIRCUT_PCT", "src_collateral", "N_HAIRCUT_PCT"),
        create_mapping(obj, "stg_collateral", "N_ELIGIBLE_VALUE", "src_collateral", "N_ELIGIBLE_VALUE"),
        create_mapping(obj, "stg_collateral", "D_VALUATION_DATE", "src_collateral", "D_VALUATION_DATE"),
        create_mapping(obj, "stg_collateral", "N_MARKET_VALUE_RC", "src_collateral", "N_MARKET_VALUE",
                      derived_expression="N_MARKET_VALUE * FX_RATE"),
        create_mapping(obj, "stg_collateral", "N_ELIGIBLE_VALUE_RC", "src_collateral", "N_ELIGIBLE_VALUE",
                      derived_expression="N_ELIGIBLE_VALUE * FX_RATE"),
        create_mapping(obj, "stg_collateral", "N_CONTRACT_SKEY", "stg_loan_contracts", "N_CONTRACT_SKEY",
                      usage_type="JOIN", usage_role="JOIN_KEY",
                      join_alias="LOAN", join_filters="src_collateral.V_CONTRACT_ID = stg_loan_contracts.V_CONTRACT_ID"),
        create_mapping(obj, "stg_collateral", "N_LOAD_RUN_ID", "src_audit_control", "N_LOAD_RUN_ID",
                      usage_type="JOIN", usage_role="JOIN_FILTER",
                      join_alias="AUDIT",
                      join_filters="src_audit_control.V_TARGET_NAME = 'STG_COLLATERAL' AND src_audit_control.V_DATA_LOAD_TYPE = 'INBOUND'"),
    ])

    # T2T_STG_COUNTERPARTY
    obj = "T2T_STG_COUNTERPARTY"
    mappings.extend([
        create_mapping(obj, "stg_counterparty", "N_PARTY_SKEY", "src_counterparty", "N_PARTY_SKEY"),
        create_mapping(obj, "stg_counterparty", "V_PARTY_ID", "src_counterparty", "V_PARTY_ID"),
        create_mapping(obj, "stg_counterparty", "V_PARTY_NAME", "src_counterparty", "V_PARTY_NAME"),
        create_mapping(obj, "stg_counterparty", "V_PARTY_TYPE", "src_counterparty", "V_PARTY_TYPE"),
        create_mapping(obj, "stg_counterparty", "V_COUNTRY_CODE", "src_counterparty", "V_COUNTRY_CODE"),
        create_mapping(obj, "stg_counterparty", "V_INDUSTRY_CODE", "src_counterparty", "V_INDUSTRY_CODE"),
        create_mapping(obj, "stg_counterparty", "V_RISK_RATING", "src_counterparty", "V_RISK_RATING"),
        create_mapping(obj, "stg_counterparty", "N_PD_PERCENT", "src_counterparty", "V_PD_GRADE",
                      derived_expression="DECODE_PD_GRADE(V_PD_GRADE)"),
        create_mapping(obj, "stg_counterparty", "N_COUNTRY_SKEY", "dim_geography", "N_COUNTRY_SKEY",
                      usage_type="JOIN", usage_role="JOIN_KEY",
                      join_alias="GEO", join_filters="src_counterparty.V_COUNTRY_CODE = dim_geography.V_COUNTRY_CODE"),
        create_mapping(obj, "stg_counterparty", "N_LOAD_RUN_ID", "src_audit_control", "N_LOAD_RUN_ID",
                      usage_type="JOIN", usage_role="JOIN_FILTER",
                      join_alias="AUDIT",
                      join_filters="src_audit_control.V_TARGET_NAME = 'STG_COUNTERPARTY' AND src_audit_control.V_DATA_LOAD_TYPE = 'INBOUND'"),
    ])

    # T2T_STG_GL_BALANCES
    obj = "T2T_STG_GL_BALANCES"
    mappings.extend([
        create_mapping(obj, "stg_gl_balances", "N_GL_SKEY", "src_gl_balances", "N_GL_SKEY"),
        create_mapping(obj, "stg_gl_balances", "V_GL_ACCOUNT", "src_gl_balances", "V_GL_ACCOUNT"),
        create_mapping(obj, "stg_gl_balances", "V_COST_CENTER", "src_gl_balances", "V_COST_CENTER"),
        create_mapping(obj, "stg_gl_balances", "V_ENTITY_CODE", "src_gl_balances", "V_ENTITY_CODE"),
        create_mapping(obj, "stg_gl_balances", "D_ACCOUNTING_DATE", "src_gl_balances", "D_ACCOUNTING_DATE"),
        create_mapping(obj, "stg_gl_balances", "N_BALANCE", "src_gl_balances", "N_BALANCE",
                      derived_expression="N_DEBIT_AMOUNT - N_CREDIT_AMOUNT"),
        create_mapping(obj, "stg_gl_balances", "N_BALANCE_RC", "src_gl_balances", "N_BALANCE",
                      derived_expression="(N_DEBIT_AMOUNT - N_CREDIT_AMOUNT) * FX_RATE"),
        create_mapping(obj, "stg_gl_balances", "N_GL_ACCOUNT_SKEY", "dim_gl_account", "N_GL_ACCOUNT_SKEY",
                      usage_type="JOIN", usage_role="JOIN_KEY",
                      join_alias="GL", join_filters="src_gl_balances.V_GL_ACCOUNT = dim_gl_account.V_GL_ACCOUNT"),
        create_mapping(obj, "stg_gl_balances", "N_LOAD_RUN_ID", "src_audit_control", "N_LOAD_RUN_ID",
                      usage_type="JOIN", usage_role="JOIN_FILTER",
                      join_alias="AUDIT",
                      join_filters="src_audit_control.V_TARGET_NAME = 'STG_GL_BALANCES' AND src_audit_control.V_DATA_LOAD_TYPE = 'INBOUND'"),
    ])

    # SQL_DIM_COUNTERPARTY
    obj = "SQL_DIM_COUNTERPARTY"
    mappings.extend([
        create_mapping(obj, "dim_counterparty", "N_PARTY_SKEY", "stg_counterparty", "N_PARTY_SKEY"),
        create_mapping(obj, "dim_counterparty", "V_PARTY_ID", "stg_counterparty", "V_PARTY_ID"),
        create_mapping(obj, "dim_counterparty", "V_PARTY_NAME", "stg_counterparty", "V_PARTY_NAME"),
        create_mapping(obj, "dim_counterparty", "V_PARTY_TYPE", "stg_counterparty", "V_PARTY_TYPE"),
        create_mapping(obj, "dim_counterparty", "N_COUNTRY_SKEY", "stg_counterparty", "N_COUNTRY_SKEY"),
        create_mapping(obj, "dim_counterparty", "V_RISK_RATING", "stg_counterparty", "V_RISK_RATING"),
        create_mapping(obj, "dim_counterparty", "N_PD_PERCENT", "stg_counterparty", "N_PD_PERCENT"),
        create_mapping(obj, "dim_counterparty", "V_SEGMENT", "stg_counterparty", "V_PARTY_TYPE",
                      derived_expression="CASE WHEN V_PARTY_TYPE = 'CORPORATE' AND V_RISK_RATING IN ('AAA','AA','A') THEN 'PRIME' ELSE 'STANDARD' END"),
        create_mapping(obj, "dim_counterparty", "D_EFFECTIVE_FROM", "stg_counterparty", "N_LOAD_RUN_ID",
                      source_type="DERIVED", derived_expression="SYSDATE"),
        create_mapping(obj, "dim_counterparty", "V_CURRENT_FLAG", "stg_counterparty", "N_LOAD_RUN_ID",
                      source_type="CONSTANT", constant_value="Y", derived_expression="'Y'"),
    ])

    # T2T_DIM_PRODUCT
    obj = "T2T_DIM_PRODUCT"
    mappings.extend([
        create_mapping(obj, "dim_product", "N_PRODUCT_SKEY", "src_loan_contracts", "V_PRODUCT_CODE",
                      source_type="DERIVED", derived_expression="SEQUENCE_PRODUCT.NEXTVAL"),
        create_mapping(obj, "dim_product", "V_PRODUCT_CODE", "src_loan_contracts", "V_PRODUCT_CODE"),
        create_mapping(obj, "dim_product", "V_PRODUCT_NAME", "src_loan_contracts", "V_PRODUCT_CODE",
                      derived_expression="LOOKUP_PRODUCT_NAME(V_PRODUCT_CODE)"),
        create_mapping(obj, "dim_product", "V_PRODUCT_TYPE", "src_loan_contracts", "V_PRODUCT_CODE",
                      derived_expression="SUBSTR(V_PRODUCT_CODE, 1, 2)"),
        create_mapping(obj, "dim_product", "V_ASSET_CLASS", "src_loan_contracts", "V_PRODUCT_CODE",
                      derived_expression="GET_ASSET_CLASS(V_PRODUCT_CODE)"),
        create_mapping(obj, "dim_product", "N_RISK_WEIGHT_PCT", "src_loan_contracts", "V_PRODUCT_CODE",
                      derived_expression="GET_RISK_WEIGHT(V_PRODUCT_CODE)"),
    ])

    # T2T_FCT_LOAN_POSITIONS
    obj = "T2T_FCT_LOAN_POSITIONS"
    mappings.extend([
        create_mapping(obj, "fct_loan_positions", "N_POSITION_SKEY", "stg_loan_contracts", "N_CONTRACT_SKEY",
                      source_type="DERIVED", derived_expression="SEQUENCE_POSITION.NEXTVAL"),
        create_mapping(obj, "fct_loan_positions", "N_CONTRACT_SKEY", "stg_loan_contracts", "N_CONTRACT_SKEY"),
        create_mapping(obj, "fct_loan_positions", "N_CUSTOMER_SKEY", "stg_loan_contracts", "N_CUSTOMER_SKEY"),
        create_mapping(obj, "fct_loan_positions", "N_OUTSTANDING_AMOUNT", "stg_loan_contracts", "N_OUTSTANDING_AMOUNT"),
        create_mapping(obj, "fct_loan_positions", "N_OUTSTANDING_AMOUNT_RC", "stg_loan_contracts", "N_OUTSTANDING_AMOUNT_RC"),
        create_mapping(obj, "fct_loan_positions", "N_INTEREST_RATE", "stg_loan_contracts", "N_INTEREST_RATE"),
        create_mapping(obj, "fct_loan_positions", "N_DAYS_PAST_DUE", "stg_loan_contracts", "N_DAYS_PAST_DUE"),
        create_mapping(obj, "fct_loan_positions", "V_PERFORMING_FLAG", "stg_loan_contracts", "V_PERFORMING_FLAG"),
        create_mapping(obj, "fct_loan_positions", "N_ACCRUED_INTEREST", "stg_loan_contracts", "N_OUTSTANDING_AMOUNT",
                      derived_expression="N_OUTSTANDING_AMOUNT * N_INTEREST_RATE * DAYS_IN_PERIOD / 360"),
        create_mapping(obj, "fct_loan_positions", "N_PRODUCT_SKEY", "dim_product", "N_PRODUCT_SKEY",
                      usage_type="JOIN", usage_role="JOIN_KEY",
                      join_alias="PROD", join_filters="stg_loan_contracts.V_PRODUCT_CODE = dim_product.V_PRODUCT_CODE"),
        create_mapping(obj, "fct_loan_positions", "N_DATE_SKEY", "dim_time", "N_DATE_SKEY",
                      usage_type="JOIN", usage_role="JOIN_KEY",
                      join_alias="TIME", join_filters="TRUNC(SYSDATE) = dim_time.D_CALENDAR_DATE"),
        create_mapping(obj, "fct_loan_positions", "N_CURRENCY_SKEY", "dim_currency", "N_CURRENCY_SKEY",
                      usage_type="JOIN", usage_role="JOIN_KEY",
                      join_alias="CCY", join_filters="stg_loan_contracts.V_CURRENCY_CODE = dim_currency.V_CURRENCY_CODE"),
    ])

    # T2T_FCT_COLLATERAL_POSITIONS
    obj = "T2T_FCT_COLLATERAL_POSITIONS"
    mappings.extend([
        create_mapping(obj, "fct_collateral_positions", "N_COLL_POS_SKEY", "stg_collateral", "N_COLLATERAL_SKEY",
                      source_type="DERIVED", derived_expression="SEQUENCE_COLL_POS.NEXTVAL"),
        create_mapping(obj, "fct_collateral_positions", "N_COLLATERAL_SKEY", "stg_collateral", "N_COLLATERAL_SKEY"),
        create_mapping(obj, "fct_collateral_positions", "N_CONTRACT_SKEY", "stg_collateral", "N_CONTRACT_SKEY"),
        create_mapping(obj, "fct_collateral_positions", "N_MARKET_VALUE", "stg_collateral", "N_MARKET_VALUE"),
        create_mapping(obj, "fct_collateral_positions", "N_MARKET_VALUE_RC", "stg_collateral", "N_MARKET_VALUE_RC"),
        create_mapping(obj, "fct_collateral_positions", "N_ELIGIBLE_VALUE", "stg_collateral", "N_ELIGIBLE_VALUE"),
        create_mapping(obj, "fct_collateral_positions", "N_ELIGIBLE_VALUE_RC", "stg_collateral", "N_ELIGIBLE_VALUE_RC"),
        create_mapping(obj, "fct_collateral_positions", "N_DATE_SKEY", "dim_time", "N_DATE_SKEY",
                      usage_type="JOIN", usage_role="JOIN_KEY",
                      join_alias="TIME", join_filters="TRUNC(SYSDATE) = dim_time.D_CALENDAR_DATE"),
    ])

    # T2T_FCT_NON_SEC_EXPOSURES
    obj = "T2T_FCT_NON_SEC_EXPOSURES"
    mappings.extend([
        create_mapping(obj, "fct_non_sec_exposures", "N_EXPOSURE_SKEY", "fct_loan_positions", "N_POSITION_SKEY",
                      source_type="DERIVED", derived_expression="SEQUENCE_EXPOSURE.NEXTVAL"),
        create_mapping(obj, "fct_non_sec_exposures", "N_CONTRACT_SKEY", "fct_loan_positions", "N_CONTRACT_SKEY"),
        create_mapping(obj, "fct_non_sec_exposures", "N_CUSTOMER_SKEY", "fct_loan_positions", "N_CUSTOMER_SKEY"),
        create_mapping(obj, "fct_non_sec_exposures", "N_PRODUCT_SKEY", "fct_loan_positions", "N_PRODUCT_SKEY"),
        create_mapping(obj, "fct_non_sec_exposures", "N_DATE_SKEY", "fct_loan_positions", "N_DATE_SKEY"),
        create_mapping(obj, "fct_non_sec_exposures", "N_EAD", "fct_loan_positions", "N_OUTSTANDING_AMOUNT",
                      derived_expression="N_OUTSTANDING_AMOUNT + N_ACCRUED_INTEREST"),
        create_mapping(obj, "fct_non_sec_exposures", "N_EAD_RC", "fct_loan_positions", "N_OUTSTANDING_AMOUNT_RC",
                      derived_expression="N_OUTSTANDING_AMOUNT_RC + (N_ACCRUED_INTEREST * FX_RATE)"),
        create_mapping(obj, "fct_non_sec_exposures", "N_PD_PERCENT", "dim_counterparty", "N_PD_PERCENT",
                      usage_type="JOIN", usage_role="JOIN_KEY",
                      join_alias="CPTY", join_filters="fct_loan_positions.N_CUSTOMER_SKEY = dim_counterparty.N_PARTY_SKEY"),
        create_mapping(obj, "fct_non_sec_exposures", "N_LGD_PERCENT", "fct_collateral_positions", "N_ELIGIBLE_VALUE",
                      usage_type="JOIN", usage_role="VALUE",
                      derived_expression="CASE WHEN N_ELIGIBLE_VALUE >= N_EAD THEN 0.25 ELSE 0.45 END",
                      join_alias="COLL", join_filters="fct_loan_positions.N_CONTRACT_SKEY = fct_collateral_positions.N_CONTRACT_SKEY"),
        create_mapping(obj, "fct_non_sec_exposures", "N_RISK_WEIGHT", "dim_product", "N_RISK_WEIGHT_PCT",
                      usage_type="JOIN", usage_role="JOIN_KEY",
                      join_alias="PROD", join_filters="fct_loan_positions.N_PRODUCT_SKEY = dim_product.N_PRODUCT_SKEY"),
        create_mapping(obj, "fct_non_sec_exposures", "N_RWA", "fct_loan_positions", "N_OUTSTANDING_AMOUNT_RC",
                      derived_expression="N_EAD_RC * N_RISK_WEIGHT / 100"),
        create_mapping(obj, "fct_non_sec_exposures", "N_ECL", "fct_loan_positions", "N_OUTSTANDING_AMOUNT_RC",
                      derived_expression="N_EAD_RC * N_PD_PERCENT * N_LGD_PERCENT"),
        create_mapping(obj, "fct_non_sec_exposures", "V_STAGE", "fct_loan_positions", "N_DAYS_PAST_DUE",
                      derived_expression="CASE WHEN N_DAYS_PAST_DUE = 0 THEN 'STAGE1' WHEN N_DAYS_PAST_DUE <= 90 THEN 'STAGE2' ELSE 'STAGE3' END"),
        create_mapping(obj, "fct_non_sec_exposures", "D_DESJ_APPROVE_DATE", "stg_loan_contracts", "D_APPROVE_DATE",
                      usage_type="JOIN", usage_role="VALUE",
                      join_alias="STG", join_filters="fct_loan_positions.N_CONTRACT_SKEY = stg_loan_contracts.N_CONTRACT_SKEY"),
    ])

    # SQL_FCT_GL_SUMMARY
    obj = "SQL_FCT_GL_SUMMARY"
    mappings.extend([
        create_mapping(obj, "fct_gl_summary", "N_GL_SUMMARY_SKEY", "stg_gl_balances", "N_GL_SKEY",
                      source_type="DERIVED", derived_expression="SEQUENCE_GL_SUMMARY.NEXTVAL"),
        create_mapping(obj, "fct_gl_summary", "N_GL_ACCOUNT_SKEY", "stg_gl_balances", "N_GL_ACCOUNT_SKEY"),
        create_mapping(obj, "fct_gl_summary", "N_BALANCE", "stg_gl_balances", "N_BALANCE",
                      derived_expression="SUM(N_BALANCE)"),
        create_mapping(obj, "fct_gl_summary", "N_BALANCE_RC", "stg_gl_balances", "N_BALANCE_RC",
                      derived_expression="SUM(N_BALANCE_RC)"),
        create_mapping(obj, "fct_gl_summary", "N_DATE_SKEY", "dim_time", "N_DATE_SKEY",
                      usage_type="JOIN", usage_role="JOIN_KEY",
                      join_alias="TIME", join_filters="stg_gl_balances.D_ACCOUNTING_DATE = dim_time.D_CALENDAR_DATE"),
    ])

    # DIH_RPT_EXPOSURE_SUMMARY
    obj = "DIH_RPT_EXPOSURE_SUMMARY"
    mappings.extend([
        create_mapping(obj, "rpt_exposure_summary", "N_DATE_SKEY", "fct_non_sec_exposures", "N_DATE_SKEY"),
        create_mapping(obj, "rpt_exposure_summary", "V_SEGMENT", "dim_counterparty", "V_SEGMENT",
                      usage_type="JOIN", usage_role="VALUE",
                      join_alias="CPTY", join_filters="fct_non_sec_exposures.N_CUSTOMER_SKEY = dim_counterparty.N_PARTY_SKEY"),
        create_mapping(obj, "rpt_exposure_summary", "V_PRODUCT_GROUP", "dim_product", "V_PRODUCT_GROUP",
                      usage_type="JOIN", usage_role="VALUE",
                      join_alias="PROD", join_filters="fct_non_sec_exposures.N_PRODUCT_SKEY = dim_product.N_PRODUCT_SKEY"),
        create_mapping(obj, "rpt_exposure_summary", "N_TOTAL_EXPOSURE", "fct_non_sec_exposures", "N_EAD_RC",
                      derived_expression="SUM(N_EAD_RC)"),
        create_mapping(obj, "rpt_exposure_summary", "N_TOTAL_RWA", "fct_non_sec_exposures", "N_RWA",
                      derived_expression="SUM(N_RWA)"),
        create_mapping(obj, "rpt_exposure_summary", "N_TOTAL_ECL", "fct_non_sec_exposures", "N_ECL",
                      derived_expression="SUM(N_ECL)"),
        create_mapping(obj, "rpt_exposure_summary", "N_CONTRACT_COUNT", "fct_non_sec_exposures", "N_CONTRACT_SKEY",
                      derived_expression="COUNT(DISTINCT N_CONTRACT_SKEY)"),
        create_mapping(obj, "rpt_exposure_summary", "N_AVG_PD", "fct_non_sec_exposures", "N_PD_PERCENT",
                      derived_expression="AVG(N_PD_PERCENT)"),
        create_mapping(obj, "rpt_exposure_summary", "N_AVG_LGD", "fct_non_sec_exposures", "N_LGD_PERCENT",
                      derived_expression="AVG(N_LGD_PERCENT)"),
    ])

    # DIH_RPT_RWA
    obj = "DIH_RPT_RWA"
    mappings.extend([
        create_mapping(obj, "rpt_risk_weighted_assets", "N_DATE_SKEY", "fct_non_sec_exposures", "N_DATE_SKEY"),
        create_mapping(obj, "rpt_risk_weighted_assets", "V_ASSET_CLASS", "dim_product", "V_ASSET_CLASS",
                      usage_type="JOIN", usage_role="VALUE",
                      join_alias="PROD", join_filters="fct_non_sec_exposures.N_PRODUCT_SKEY = dim_product.N_PRODUCT_SKEY"),
        create_mapping(obj, "rpt_risk_weighted_assets", "V_REGION", "dim_geography", "V_REGION",
                      usage_type="JOIN", usage_role="VALUE",
                      join_alias="GEO",
                      join_filters="dim_counterparty.N_COUNTRY_SKEY = dim_geography.N_COUNTRY_SKEY"),
        create_mapping(obj, "rpt_risk_weighted_assets", "N_EXPOSURE", "fct_non_sec_exposures", "N_EAD_RC",
                      derived_expression="SUM(N_EAD_RC)"),
        create_mapping(obj, "rpt_risk_weighted_assets", "N_RWA", "fct_non_sec_exposures", "N_RWA",
                      derived_expression="SUM(N_RWA)"),
        create_mapping(obj, "rpt_risk_weighted_assets", "N_CAPITAL_REQUIREMENT", "fct_non_sec_exposures", "N_RWA",
                      derived_expression="SUM(N_RWA) * 0.08"),
    ])

    # DIH_RPT_CUSTOMER_360
    obj = "DIH_RPT_CUSTOMER_360"
    mappings.extend([
        create_mapping(obj, "rpt_customer_360", "N_CUSTOMER_SKEY", "dim_counterparty", "N_PARTY_SKEY"),
        create_mapping(obj, "rpt_customer_360", "V_PARTY_NAME", "dim_counterparty", "V_PARTY_NAME"),
        create_mapping(obj, "rpt_customer_360", "V_SEGMENT", "dim_counterparty", "V_SEGMENT"),
        create_mapping(obj, "rpt_customer_360", "V_RISK_RATING", "dim_counterparty", "V_RISK_RATING"),
        create_mapping(obj, "rpt_customer_360", "N_AVG_PD", "dim_counterparty", "N_PD_PERCENT"),
        create_mapping(obj, "rpt_customer_360", "N_TOTAL_EXPOSURE", "fct_non_sec_exposures", "N_EAD_RC",
                      usage_type="JOIN", usage_role="VALUE",
                      derived_expression="SUM(N_EAD_RC)",
                      join_alias="EXP", join_filters="dim_counterparty.N_PARTY_SKEY = fct_non_sec_exposures.N_CUSTOMER_SKEY"),
        create_mapping(obj, "rpt_customer_360", "N_CONTRACT_COUNT", "fct_non_sec_exposures", "N_CONTRACT_SKEY",
                      usage_type="JOIN", usage_role="VALUE",
                      derived_expression="COUNT(DISTINCT N_CONTRACT_SKEY)",
                      join_alias="EXP", join_filters="dim_counterparty.N_PARTY_SKEY = fct_non_sec_exposures.N_CUSTOMER_SKEY"),
        create_mapping(obj, "rpt_customer_360", "N_TOTAL_COLLATERAL", "fct_collateral_positions", "N_ELIGIBLE_VALUE_RC",
                      usage_type="JOIN", usage_role="VALUE",
                      derived_expression="SUM(N_ELIGIBLE_VALUE_RC)",
                      join_alias="COLL", join_filters="fct_non_sec_exposures.N_CONTRACT_SKEY = fct_collateral_positions.N_CONTRACT_SKEY"),
        create_mapping(obj, "rpt_customer_360", "N_NET_EXPOSURE", "fct_non_sec_exposures", "N_EAD_RC",
                      derived_expression="SUM(N_EAD_RC) - COALESCE(SUM(N_ELIGIBLE_VALUE_RC), 0)"),
    ])

    return pd.DataFrame(mappings)


# =============================================================================
# Large Scale Data Generation (30k+ mappings)
# =============================================================================

def generate_large_scale_mappings(target_count: int = 30000, mixed_case: bool = True) -> tuple:
    """
    Generate large scale mappings (~30k records) for performance testing.

    Creates a realistic data lineage structure:
    - Source tables (src_) - raw data ingestion
    - Staging tables (stg_) - cleansed data
    - Dimension tables (dim_) - master data
    - Fact tables (fact_) - transactional data
    - Report tables (rpt_) - aggregated data

    Args:
        target_count: Approximate number of mappings to generate
        mixed_case: If True, randomize case for testing normalization

    Returns:
        Tuple of (v1_df, v2_df, data_model_df)
    """
    random.seed(42)  # Reproducible results

    # Configuration for ~30k mappings
    # Each layer feeds into the next, creating realistic lineage
    # Total dest fields ~= 200*25 + 150*30 + 100*35 + 50*40 = 5000+4500+3500+2000 = 15000 fields
    # With avg 2 sources each = ~30k mappings
    layers = {
        'src': {'count': 100, 'fields_per_table': 20},   # Source layer (not dest, just sources)
        'stg': {'count': 200, 'fields_per_table': 25},   # Staging layer
        'dim': {'count': 150, 'fields_per_table': 30},   # Dimension layer
        'fact': {'count': 100, 'fields_per_table': 35},  # Fact layer
        'rpt': {'count': 50, 'fields_per_table': 40},    # Report layer
    }

    # Table name templates per layer
    table_templates = {
        'src': ['orders', 'customers', 'products', 'transactions', 'accounts',
                'payments', 'invoices', 'shipments', 'inventory', 'employees',
                'suppliers', 'contracts', 'loans', 'deposits', 'securities',
                'positions', 'trades', 'settlements', 'collateral', 'ratings',
                'limits', 'exposures', 'cashflows', 'valuations', 'prices',
                'rates', 'spreads', 'yields', 'durations', 'convexities',
                'greeks', 'scenarios', 'stresses', 'pnl', 'mtm',
                'accruals', 'provisions', 'impairments', 'writeoffs', 'recoveries',
                'guarantees', 'commitments', 'derivatives', 'swaps', 'options',
                'futures', 'forwards', 'caps', 'floors', 'swaptions'],
        'stg': ['orders', 'customers', 'products', 'transactions', 'accounts',
                'payments', 'invoices', 'shipments', 'inventory', 'employees',
                'suppliers', 'contracts', 'loans', 'deposits', 'securities',
                'positions', 'trades', 'settlements', 'collateral', 'ratings',
                'limits', 'exposures', 'cashflows', 'valuations', 'prices',
                'rates', 'spreads', 'yields', 'durations', 'convexities',
                'greeks', 'scenarios', 'stresses', 'pnl', 'mtm',
                'accruals', 'provisions', 'impairments', 'writeoffs', 'recoveries'],
        'dim': ['customer', 'product', 'geography', 'time', 'currency',
                'account', 'organization', 'channel', 'segment', 'rating',
                'industry', 'counterparty', 'instrument', 'portfolio', 'book',
                'desk', 'trader', 'strategy', 'benchmark', 'index',
                'issuer', 'guarantor', 'custodian', 'broker', 'exchange'],
        'fact': ['daily_positions', 'trade_events', 'cashflow_details', 'risk_measures',
                 'pnl_attribution', 'exposure_summary', 'collateral_values', 'limit_usage',
                 'settlement_activity', 'valuation_results', 'stress_results', 'scenario_outputs',
                 'regulatory_capital', 'liquidity_coverage', 'credit_losses'],
        'rpt': ['executive_summary', 'risk_dashboard', 'regulatory_report', 'management_report',
                'daily_pnl', 'var_report', 'exposure_report', 'limit_breach',
                'capital_adequacy', 'liquidity_report'],
    }

    # Field name templates
    field_templates = [
        'id', 'key', 'code', 'name', 'description', 'type', 'status', 'flag',
        'date', 'timestamp', 'start_date', 'end_date', 'effective_date', 'expiry_date',
        'amount', 'quantity', 'value', 'balance', 'rate', 'price', 'cost', 'margin',
        'currency', 'country', 'region', 'segment', 'category', 'class', 'grade',
        'created_by', 'created_date', 'modified_by', 'modified_date', 'load_run_id',
        'source_system', 'batch_id', 'record_status', 'version', 'checksum',
        'notional', 'principal', 'interest', 'accrued', 'premium', 'discount',
        'fair_value', 'book_value', 'market_value', 'face_value', 'nominal_value',
        'delta', 'gamma', 'vega', 'theta', 'rho', 'pv01', 'dv01', 'cs01',
        'var_95', 'var_99', 'es_95', 'es_99', 'stress_loss', 'base_loss',
        'pd', 'lgd', 'ead', 'rwa', 'ecl', 'el', 'ul', 'rc', 'cva', 'dva',
    ]

    # Expression templates for derived fields
    expression_templates = [
        "COALESCE({src}, 0)",
        "NVL({src}, 'UNKNOWN')",
        "UPPER(TRIM({src}))",
        "CASE WHEN {src} > 0 THEN 'POSITIVE' ELSE 'NEGATIVE' END",
        "{src} * exchange_rate",
        "{src} + adjustment_amount",
        "ROUND({src}, 2)",
        "TO_DATE({src}, 'YYYY-MM-DD')",
        "DECODE({src}, 'A', 1, 'B', 2, 0)",
        "SUM({src}) OVER (PARTITION BY entity_id)",
    ]

    # Generate tables for each layer
    all_tables = {}
    for layer, config in layers.items():
        prefix = f"{layer}_"
        templates = table_templates[layer]
        for i in range(config['count']):
            table_name = f"{prefix}{templates[i % len(templates)]}"
            if i >= len(templates):
                table_name += f"_{i // len(templates) + 1}"

            # Generate fields for this table
            fields = []
            for j in range(config['fields_per_table']):
                field_base = field_templates[j % len(field_templates)]
                if j >= len(field_templates):
                    field_name = f"{field_base}_{j // len(field_templates) + 1}"
                else:
                    field_name = field_base
                fields.append(field_name)

            all_tables[table_name] = fields

    # Build lineage: each layer maps from previous layer(s)
    layer_order = ['src', 'stg', 'dim', 'fact', 'rpt']
    mappings = []

    def maybe_randomize(name: str) -> str:
        """Apply case randomization if enabled."""
        if mixed_case:
            return randomize_case(name)
        return name

    # Generate mappings between layers
    for i, dest_layer in enumerate(layer_order[1:], 1):
        source_layers = layer_order[:i]  # All previous layers can be sources

        dest_tables = [t for t in all_tables.keys() if t.startswith(f"{dest_layer}_")]

        for dest_table in dest_tables:
            dest_fields = all_tables[dest_table]
            obj_name = f"T2T_{dest_table.upper()}"

            # Each dest field gets 1-4 source mappings (weighted to generate ~30k total)
            for dest_field in dest_fields:
                num_sources = random.choices([1, 2, 3, 4], weights=[40, 30, 20, 10])[0]

                for _ in range(num_sources):
                    # Pick a random source layer and table
                    src_layer = random.choice(source_layers)
                    src_tables = [t for t in all_tables.keys() if t.startswith(f"{src_layer}_")]
                    src_table = random.choice(src_tables)
                    src_fields = all_tables[src_table]
                    src_field = random.choice(src_fields)

                    # Determine mapping type
                    mapping_type = random.choice(MAPPING_TYPES)

                    # Build the mapping
                    mapping = {
                        "object_name": obj_name,
                        "destination_table": maybe_randomize(dest_table),
                        "destination_field": maybe_randomize(dest_field),
                        "usage_type": mapping_type if mapping_type in ['JOIN', 'LOOKUP'] else "MAPPING",
                        "usage_role": "JOIN_KEY" if mapping_type == 'JOIN' else ("JOIN_FILTER" if mapping_type == 'LOOKUP' else "VALUE"),
                        "source_type": "PHYSICAL",
                        "source_table": maybe_randomize(src_table),
                        "source_field": maybe_randomize(src_field),
                        "constant_value": "",
                        "derived_output": "Y" if mapping_type in ['TRANSFORM', 'CALC'] else "N",
                        "derived_expression": random.choice(expression_templates).format(src=src_field) if mapping_type in ['TRANSFORM', 'CALC'] else "",
                        "join_alias": f"J{random.randint(1,99):02d}" if mapping_type in ['JOIN', 'LOOKUP'] else "",
                        "join_keys": f"{src_table}.{src_field} = {dest_table}.{dest_field}" if mapping_type == 'JOIN' else "",
                        "join_filters": f"{src_table}.status = 'ACTIVE'" if mapping_type == 'LOOKUP' else "",
                        "dm_match": random.choice(["Y", "Y", "Y", "N"]),  # 75% match
                        "trace_path": f"{src_layer} -> {dest_layer}",
                        "notes": "",
                    }
                    mappings.append(mapping)

                    if len(mappings) >= target_count:
                        break
                if len(mappings) >= target_count:
                    break
            if len(mappings) >= target_count:
                break
        if len(mappings) >= target_count:
            break

    # Create V1 DataFrame
    v1_df = pd.DataFrame(mappings)

    # Create V2 with some changes (for delta testing)
    v2_mappings = mappings.copy()

    # Modify ~5% of mappings
    num_changes = len(v2_mappings) // 20
    change_indices = random.sample(range(len(v2_mappings)), min(num_changes, len(v2_mappings)))

    for idx in change_indices:
        change_type = random.choice(['modify_expression', 'modify_source', 'delete'])
        if change_type == 'modify_expression':
            v2_mappings[idx] = v2_mappings[idx].copy()
            v2_mappings[idx]['derived_expression'] = random.choice(expression_templates).format(src='modified_field')
            v2_mappings[idx]['notes'] = 'MODIFIED in V2'
        elif change_type == 'modify_source':
            v2_mappings[idx] = v2_mappings[idx].copy()
            v2_mappings[idx]['source_field'] = maybe_randomize('modified_field')
            v2_mappings[idx]['notes'] = 'SOURCE CHANGED in V2'

    # Remove ~2% of mappings
    num_remove = len(v2_mappings) // 50
    remove_indices = set(random.sample(range(len(v2_mappings)), min(num_remove, len(v2_mappings))))
    v2_mappings = [m for i, m in enumerate(v2_mappings) if i not in remove_indices]

    # Add ~3% new mappings
    num_add = len(mappings) // 33
    for _ in range(num_add):
        new_mapping = random.choice(mappings).copy()
        new_mapping['destination_field'] = maybe_randomize(f"new_field_{random.randint(1, 1000)}")
        new_mapping['notes'] = 'ADDED in V2'
        v2_mappings.append(new_mapping)

    v2_df = pd.DataFrame(v2_mappings)

    # Create data model DataFrame
    dm_rows = []
    for table_name, fields in all_tables.items():
        for field in fields:
            dm_rows.append({
                "Table Name": table_name,
                "Column Name": field,
                "Data Type": random.choice(["VARCHAR2(100)", "NUMBER(18,2)", "DATE", "TIMESTAMP"]),
                "Is PK": "TRUE" if field.endswith('_id') or field.endswith('_key') else "FALSE",
                "Is FK": "TRUE" if field.endswith('_skey') else "FALSE",
                "Description": f"Description for {field}",
            })
    dm_df = pd.DataFrame(dm_rows)

    return v1_df, v2_df, dm_df


def generate_30k_files():
    """Generate 30k mapping files for performance testing."""
    print("Generating 30k scale mock data...")

    v1_df, v2_df, dm_df = generate_large_scale_mappings(target_count=30000, mixed_case=True)

    # Reorder columns to match expected format
    column_order = [
        "object_name", "destination_table", "destination_field", "usage_type", "usage_role",
        "source_type", "source_table", "source_field", "constant_value", "derived_output",
        "derived_expression", "join_alias", "join_keys", "join_filters", "dm_match", "trace_path", "notes"
    ]

    v1_df = v1_df[column_order]
    v2_df = v2_df[column_order]

    # Save files
    v1_path = OUTPUT_DIR / "mappings_v1_30k.xlsx"
    v1_df.to_excel(v1_path, index=False)
    print(f"Created: {v1_path} ({len(v1_df)} mappings)")

    v2_path = OUTPUT_DIR / "mappings_v2_30k.xlsx"
    v2_df.to_excel(v2_path, index=False)
    print(f"Created: {v2_path} ({len(v2_df)} mappings)")

    dm_path = OUTPUT_DIR / "data_model_30k.xlsx"
    dm_df.to_excel(dm_path, index=False)
    print(f"Created: {dm_path} ({len(dm_df)} columns)")

    # Print statistics
    print(f"\n=== Statistics ===")
    print(f"V1 mappings: {len(v1_df)}")
    print(f"V2 mappings: {len(v2_df)}")
    print(f"Unique source tables: {v1_df['source_table'].str.upper().nunique()}")
    print(f"Unique dest tables: {v1_df['destination_table'].str.upper().nunique()}")
    print(f"Mapping types: {v1_df['usage_type'].value_counts().to_dict()}")

    # Check mixed case
    sample = v1_df['source_table'].head(20).tolist()
    has_mixed = any(not (s.isupper() or s.islower()) for s in sample if s)
    print(f"Has mixed case: {has_mixed}")

    return v1_df, v2_df, dm_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate mock data for Lineage Explorer")
    parser.add_argument('--size', choices=['small', '30k'], default='small',
                        help='Data size: small (~125 rows) or 30k (~30000 rows)')
    parser.add_argument('--output', type=str, default=None,
                        help='Output directory (default: sample_data)')

    args = parser.parse_args()

    if args.output:
        OUTPUT_DIR = Path(args.output)
        OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

    print("Generating OFSAA-style mock data for Lineage Explorer...\n")

    if args.size == '30k':
        generate_30k_files()
    else:
        # Original small data generation
        generate_data_model()
        generate_mappings_v1()
        generate_mappings_v2()

    print("\nDone! Files created in:", OUTPUT_DIR)
