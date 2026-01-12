"""
Validate lineage data against the contract.

This ensures any input (Excel, CSV, JSON) conforms to the expected format
before processing by the viewer or other consumers.
"""

from pathlib import Path
from typing import Union
import json

# Import pandas only when needed (optional dependency for validation)
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


class ValidationError(Exception):
    """Raised when input doesn't conform to the lineage contract."""
    pass


# Contract definition
REQUIRED_COLUMNS = [
    "object_name",
    "destination_table",
    "destination_field",
    "usage_type",
    "usage_role",
    "source_type",
]

OPTIONAL_COLUMNS = [
    "source_table",
    "source_field",
    "constant_value",
    "derived_output",
    "derived_expression",
    "join_alias",
    "join_keys",
    "join_filters",
    "dm_match",
    "trace_path",
    "notes",
]

ALL_COLUMNS = REQUIRED_COLUMNS + OPTIONAL_COLUMNS

VALID_USAGE_TYPES = {"MAPPING", "JOIN"}
VALID_USAGE_ROLES = {"VALUE", "JOIN_KEY", "JOIN_FILTER"}
VALID_SOURCE_TYPES = {"PHYSICAL", "CONSTANT", "UNRESOLVED", "DERIVED"}


def validate_input(path: Path) -> bool:
    """
    Validate input file conforms to lineage contract.

    Args:
        path: Path to input file (xlsx, csv, or json)

    Raises:
        ValidationError: If input is invalid

    Returns:
        True if valid
    """
    path = Path(path)

    if not path.exists():
        raise ValidationError(f"Input file not found: {path}")

    suffix = path.suffix.lower()

    if suffix in (".xlsx", ".xls"):
        return _validate_excel(path)
    elif suffix == ".csv":
        return _validate_csv(path)
    elif suffix == ".json":
        return _validate_json(path)
    else:
        raise ValidationError(f"Unsupported file format: {suffix}")


def _validate_excel(path: Path) -> bool:
    """Validate Excel file."""
    if not HAS_PANDAS:
        raise ValidationError("pandas required for Excel validation")

    try:
        # Try to find Mapping_exploded sheet
        xl = pd.ExcelFile(path)

        # Look for sheet containing mapping data
        sheet_name = None
        for name in xl.sheet_names:
            if "mapping" in name.lower() or "exploded" in name.lower():
                sheet_name = name
                break

        if sheet_name is None:
            # Try first sheet
            sheet_name = xl.sheet_names[0]

        df = pd.read_excel(path, sheet_name=sheet_name)

    except Exception as e:
        raise ValidationError(f"Cannot read Excel: {e}")

    return validate_dataframe(df)


def _validate_csv(path: Path) -> bool:
    """Validate CSV file."""
    if not HAS_PANDAS:
        raise ValidationError("pandas required for CSV validation")

    try:
        df = pd.read_csv(path)
    except Exception as e:
        raise ValidationError(f"Cannot read CSV: {e}")

    return validate_dataframe(df)


def validate_dataframe(df) -> bool:
    """
    Validate DataFrame has required columns and valid values.

    Args:
        df: pandas DataFrame with lineage data

    Raises:
        ValidationError: If DataFrame doesn't conform to contract

    Returns:
        True if valid
    """
    # Normalize column names (lowercase, strip whitespace)
    df.columns = [str(c).lower().strip().replace(" ", "_") for c in df.columns]

    # Map common variants to standard names
    column_mapping = {
        "dest_table": "destination_table",
        "dest_field": "destination_field",
        "target_table": "destination_table",
        "target_field": "destination_field",
        "target_column": "destination_field",
        "src_table": "source_table",
        "src_field": "source_field",
        "src_column": "source_field",
    }

    df.rename(columns=column_mapping, inplace=True)

    # Check required columns
    required_lower = [c.lower() for c in REQUIRED_COLUMNS]
    missing = set(required_lower) - set(df.columns)
    if missing:
        raise ValidationError(f"Missing required columns: {missing}")

    # Validate enum values (case-insensitive)
    df_usage_types = set(df["usage_type"].dropna().str.upper().unique())
    invalid_usage_types = df_usage_types - VALID_USAGE_TYPES
    if invalid_usage_types:
        raise ValidationError(f"Invalid usage_type values: {invalid_usage_types}")

    df_usage_roles = set(df["usage_role"].dropna().str.upper().unique())
    invalid_usage_roles = df_usage_roles - VALID_USAGE_ROLES
    if invalid_usage_roles:
        raise ValidationError(f"Invalid usage_role values: {invalid_usage_roles}")

    df_source_types = set(df["source_type"].dropna().str.upper().unique())
    invalid_source_types = df_source_types - VALID_SOURCE_TYPES
    if invalid_source_types:
        raise ValidationError(f"Invalid source_type values: {invalid_source_types}")

    # Validate PHYSICAL rows have source_table and source_field
    if "source_table" in df.columns and "source_field" in df.columns:
        physical_mask = df["source_type"].str.upper() == "PHYSICAL"
        physical_rows = df[physical_mask]

        missing_source = physical_rows[
            physical_rows["source_table"].isna() |
            (physical_rows["source_table"] == "") |
            physical_rows["source_field"].isna() |
            (physical_rows["source_field"] == "")
        ]

        if len(missing_source) > 0:
            raise ValidationError(
                f"PHYSICAL rows missing source_table/source_field: {len(missing_source)} rows"
            )

    return True


def _validate_json(path: Path) -> bool:
    """Validate JSON graph format."""
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        raise ValidationError(f"Cannot read JSON: {e}")

    # Check for edges or mappings
    if "edges" not in data and "mappings" not in data:
        raise ValidationError("JSON missing 'edges' or 'mappings' key")

    edges = data.get("edges", data.get("mappings", []))

    # Validate edge structure
    for i, edge in enumerate(edges):
        required = ["source_table", "source_column", "target_table", "target_column"]

        # Allow alternative naming
        alt_names = {
            "source_column": ["source_field", "src_column", "src_field"],
            "target_column": ["target_field", "dest_column", "dest_field",
                            "destination_column", "destination_field"],
            "target_table": ["dest_table", "destination_table"],
        }

        for req in required:
            found = req in edge
            if not found and req in alt_names:
                found = any(alt in edge for alt in alt_names[req])

            if not found and req in ["source_table", "source_column"]:
                # Source can be empty for CONSTANT type
                continue

            if not found:
                raise ValidationError(f"Edge {i} missing key: {req}")

    return True


def get_column_order() -> list:
    """Get the standard column order for OFSAA format output."""
    return [
        "object_name",
        "destination_table",
        "destination_field",
        "usage_type",
        "usage_role",
        "source_type",
        "source_table",
        "source_field",
        "constant_value",
        "derived_output",
        "derived_expression",
        "join_alias",
        "join_keys",
        "join_filters",
        "dm_match",
        "trace_path",
        "notes",
    ]
