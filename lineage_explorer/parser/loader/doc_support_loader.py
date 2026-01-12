"""
Load Doc-Support Excel files containing:
- T2T-F2T Mappings sheet (target table, target column, source expression)
- DM Atomic sheet (data model: table → columns)
- EDD files (source file definitions)
"""

from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set
import re

import pandas as pd


class DocSupportLoader:
    """Load and parse Doc-Support Excel files."""

    def __init__(self):
        self.schema: Dict[str, Dict[str, str]] = {}  # TABLE → {COL: TYPE}
        self.mappings: pd.DataFrame = pd.DataFrame()
        self.dm_columns: Dict[str, Set[str]] = {}  # TABLE → {COL1, COL2, ...}

    def load(
        self,
        doc_support_path: Path,
        dm_atomic_path: Optional[Path] = None,
        edd_paths: Optional[List[Path]] = None,
        mapping_sheet: str = None,
        dm_sheet: str = None,
    ) -> Tuple[pd.DataFrame, Dict[str, Dict[str, str]]]:
        """
        Load all inputs and build schema.

        Args:
            doc_support_path: Path to Doc-Support Excel
            dm_atomic_path: Optional separate DM Atomic file
            edd_paths: Optional list of EDD files
            mapping_sheet: Sheet name pattern for mappings
            dm_sheet: Sheet name pattern for data model

        Returns:
            Tuple of (mappings_df, schema_dict)
        """
        doc_support_path = Path(doc_support_path)

        # Load mappings
        self.mappings = self._load_mappings(doc_support_path, mapping_sheet)

        # Load DM from Doc-Support or separate file
        dm_path = dm_atomic_path or doc_support_path
        self._load_dm_atomic(dm_path, dm_sheet)

        # Load EDD files if provided
        if edd_paths:
            for edd_path in edd_paths:
                self._load_edd(edd_path)

        # Infer additional schema from mappings
        self._infer_schema_from_mappings()

        return self.mappings, self.schema

    def _load_mappings(self, path: Path, sheet_pattern: Optional[str] = None) -> pd.DataFrame:
        """Load T2T-F2T Mappings sheet."""
        xl = pd.ExcelFile(path)

        # Find mapping sheet
        sheet_name = self._find_sheet(xl.sheet_names, sheet_pattern, ["t2t", "f2t", "mapping"])

        if sheet_name is None:
            raise ValueError(f"No mapping sheet found in {path}")

        df = pd.read_excel(path, sheet_name=sheet_name)

        # Normalize column names
        df.columns = [str(c).strip() for c in df.columns]

        # Map common column name variants
        column_mapping = {
            # Target columns
            "Target Table": "target_table",
            "target table": "target_table",
            "Target_Table": "target_table",
            "Destination Table": "target_table",
            "destination_table": "target_table",

            "Target Column": "target_column",
            "target column": "target_column",
            "Target_Column": "target_column",
            "Destination Column": "target_column",
            "destination_field": "target_column",

            # Source columns
            "Source Column": "source_column",
            "source column": "source_column",
            "Source_Column": "source_column",
            "Source Expression": "source_column",

            # Expression
            "Expression": "expression",
            "Transformation": "expression",
            "Rule": "expression",

            # Metadata
            "Type": "type",
            "Name": "name",
            "Object Name": "name",
            "object_name": "name",
        }

        # Apply mapping
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df.rename(columns={old_name: new_name}, inplace=True)

        # Ensure required columns exist
        required = ["target_table", "target_column"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns in mappings: {missing}")

        return df

    def _load_dm_atomic(self, path: Path, sheet_pattern: Optional[str] = None):
        """Load DM Atomic sheet (data model)."""
        xl = pd.ExcelFile(path)

        # Find DM sheet
        sheet_name = self._find_sheet(xl.sheet_names, sheet_pattern, ["dm", "atomic", "data model", "model"])

        if sheet_name is None:
            print(f"Warning: No DM Atomic sheet found in {path}")
            return

        df = pd.read_excel(path, sheet_name=sheet_name)

        # Normalize column names
        df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

        # Find table and column name columns
        table_col = self._find_column(df.columns, ["table_physical_name", "table_name", "table", "physical_table"])
        column_col = self._find_column(df.columns, ["column_physical_name", "column_name", "column", "physical_column"])
        type_col = self._find_column(df.columns, ["data_type", "datatype", "type", "column_type"])

        if table_col is None or column_col is None:
            print(f"Warning: Cannot identify table/column columns in DM sheet")
            return

        # Build schema
        for _, row in df.iterrows():
            table = str(row[table_col]).strip().upper()
            column = str(row[column_col]).strip().upper()
            data_type = str(row[type_col]).strip().upper() if type_col else "VARCHAR2"

            if table and column and table != "NAN" and column != "NAN":
                if table not in self.schema:
                    self.schema[table] = {}
                    self.dm_columns[table] = set()

                self.schema[table][column] = data_type
                self.dm_columns[table].add(column)

        print(f"Loaded DM: {len(self.schema)} tables, "
              f"{sum(len(cols) for cols in self.dm_columns.values())} columns")

    def _load_edd(self, path: Path):
        """Load EDD (External Data Definition) file."""
        path = Path(path)

        if path.suffix.lower() in (".xlsx", ".xls"):
            df = pd.read_excel(path)
        elif path.suffix.lower() == ".csv":
            df = pd.read_csv(path)
        else:
            print(f"Warning: Unsupported EDD format: {path}")
            return

        # Normalize column names
        df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

        # Find relevant columns
        table_col = self._find_column(df.columns, ["table_name", "table", "file_name", "entity"])
        column_col = self._find_column(df.columns, ["column_name", "column", "field_name", "field"])
        type_col = self._find_column(df.columns, ["data_type", "datatype", "type"])

        if table_col is None or column_col is None:
            print(f"Warning: Cannot identify table/column columns in EDD: {path}")
            return

        # Add to schema (EDD tables are typically STG_ prefixed)
        for _, row in df.iterrows():
            table = str(row[table_col]).strip().upper()
            column = str(row[column_col]).strip().upper()
            data_type = str(row[type_col]).strip().upper() if type_col else "VARCHAR2"

            if table and column and table != "NAN" and column != "NAN":
                # Add STG_ prefix if not present
                if not any(table.startswith(p) for p in ["STG_", "SRC_", "EXT_"]):
                    table = f"STG_{table}"

                if table not in self.schema:
                    self.schema[table] = {}
                    self.dm_columns[table] = set()

                self.schema[table][column] = data_type
                self.dm_columns[table].add(column)

        print(f"Loaded EDD: {path.name}")

    def _infer_schema_from_mappings(self):
        """Infer additional schema from mapping expressions."""
        if self.mappings.empty:
            return

        # Pattern to find TABLE.COLUMN references
        pattern = r'\b([A-Z_][A-Z0-9_]*)\s*\.\s*([A-Z_][A-Z0-9_]*)\b'

        for _, row in self.mappings.iterrows():
            # Check expression column
            expr = str(row.get("expression", "") or row.get("source_column", ""))

            for match in re.finditer(pattern, expr.upper()):
                table, column = match.groups()

                # Skip aliases that look like single letters
                if len(table) == 1:
                    continue

                if table not in self.schema:
                    self.schema[table] = {}
                    self.dm_columns[table] = set()

                if column not in self.schema[table]:
                    self.schema[table][column] = "VARCHAR2"  # Default type
                    self.dm_columns[table].add(column)

    def _find_sheet(self, sheet_names: List[str], pattern: Optional[str],
                    keywords: List[str]) -> Optional[str]:
        """Find sheet by pattern or keywords."""
        if pattern:
            for name in sheet_names:
                if pattern.lower() in name.lower():
                    return name

        for keyword in keywords:
            for name in sheet_names:
                if keyword.lower() in name.lower():
                    return name

        return None

    def _find_column(self, columns, candidates: List[str]) -> Optional[str]:
        """Find column by candidate names."""
        for candidate in candidates:
            if candidate in columns:
                return candidate
            # Try case-insensitive
            for col in columns:
                if col.lower() == candidate.lower():
                    return col
        return None

    def dm_match(self, table: str, column: str) -> str:
        """Check if table.column exists in data model."""
        table_upper = table.upper()
        column_upper = column.upper()

        if table_upper in self.dm_columns:
            if column_upper in self.dm_columns[table_upper]:
                return "Y"
        return "N"

    def get_schema_for_sqlglot(self) -> Dict[str, Dict[str, str]]:
        """Get schema in SQLGlot-compatible format."""
        return self.schema

    def filter_mappings_by_object(self, object_name: str) -> pd.DataFrame:
        """Filter mappings for a specific T2T object."""
        if "type" in self.mappings.columns and "name" in self.mappings.columns:
            mask = (
                (self.mappings["type"].str.upper() == "T2T") &
                (self.mappings["name"].str.upper() == object_name.upper())
            )
            return self.mappings[mask].copy()

        # If no type/name columns, return all
        return self.mappings.copy()
