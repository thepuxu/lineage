"""
Excel writer for OFSAA format output.

Produces Excel workbooks with:
- Mapping_exploded (17-column lineage)
- Join_conditions_by_alias
- DM_validation_summary
- QA_compare_simple_vs_deep
"""

from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

import pandas as pd

from ..models.dataclasses import LineageData, LineageEdge
from ..models.enums import SourceType, UsageType, UsageRole
from ..contracts.validator import get_column_order


class ExcelWriter:
    """Write lineage data to OFSAA format Excel."""

    def __init__(self, output_dir: Path):
        """
        Initialize the writer.

        Args:
            output_dir: Directory for output files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write(self, lineage_data: LineageData,
              simple_lineage: Optional[LineageData] = None,
              object_name: Optional[str] = None) -> Path:
        """
        Write lineage to Excel workbook.

        Args:
            lineage_data: Deep lineage data
            simple_lineage: Optional simple lineage for QA comparison
            object_name: Object name for filename

        Returns:
            Path to written file
        """
        obj_name = object_name or lineage_data.metadata.get("object_name", "UNKNOWN")

        # Build filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{obj_name}_Physical_Mapping_WithJoins_DEEP_{timestamp}.xlsx"
        output_path = self.output_dir / filename

        # Build DataFrames
        mapping_df = self._build_mapping_df(lineage_data)
        join_df = self._build_join_df(lineage_data)
        summary_df = self._build_summary_df(lineage_data)
        qa_df = self._build_qa_df(lineage_data, simple_lineage) if simple_lineage else pd.DataFrame()

        # Write to Excel
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            mapping_df.to_excel(writer, sheet_name="Mapping_exploded", index=False)
            join_df.to_excel(writer, sheet_name="Join_conditions_by_alias", index=False)
            summary_df.to_excel(writer, sheet_name="DM_validation_summary", index=False)
            if not qa_df.empty:
                qa_df.to_excel(writer, sheet_name="QA_compare_simple_vs_deep", index=False)

        print(f"Written: {output_path}")
        return output_path

    def _build_mapping_df(self, lineage_data: LineageData) -> pd.DataFrame:
        """Build the Mapping_exploded DataFrame."""
        rows = []

        for edge in lineage_data.edges:
            rows.append(edge.to_ofsaa_dict())

        df = pd.DataFrame(rows)

        # Ensure column order
        column_order = get_column_order()
        for col in column_order:
            if col not in df.columns:
                df[col] = ""

        df = df[column_order]

        # Sort deterministically
        df = self._sort_mapping(df)

        return df

    def _sort_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sort mapping DataFrame deterministically."""
        if df.empty:
            return df

        # Define sort order
        usage_type_order = {"MAPPING": 0, "JOIN": 1}
        usage_role_order = {"VALUE": 0, "JOIN_KEY": 1, "JOIN_FILTER": 2}
        source_type_order = {"PHYSICAL": 0, "CONSTANT": 1, "UNRESOLVED": 2, "DERIVED": 3}

        # Create sort keys
        df = df.copy()
        df["_sort_usage_type"] = df["usage_type"].map(usage_type_order).fillna(99)
        df["_sort_usage_role"] = df["usage_role"].map(usage_role_order).fillna(99)
        df["_sort_source_type"] = df["source_type"].map(source_type_order).fillna(99)
        df["_sort_dest_table"] = df["destination_table"].fillna("ZZZZ")
        df["_sort_dest_field"] = df["destination_field"].fillna("")

        # Sort
        df = df.sort_values([
            "_sort_dest_table",
            "_sort_dest_field",
            "_sort_usage_type",
            "_sort_usage_role",
            "_sort_source_type",
            "source_table",
            "source_field",
            "join_alias",
            "constant_value",
            "derived_expression"
        ])

        # Drop sort columns
        df = df.drop(columns=[c for c in df.columns if c.startswith("_sort_")])

        return df.reset_index(drop=True)

    def _build_join_df(self, lineage_data: LineageData) -> pd.DataFrame:
        """Build Join_conditions_by_alias DataFrame."""
        # Group by alias
        join_edges = [e for e in lineage_data.edges if e.usage_type == UsageType.JOIN]

        if not join_edges:
            return pd.DataFrame(columns=[
                "object_name", "join_alias", "join_type",
                "join_keys", "join_filters", "tables_involved"
            ])

        # Group by alias
        by_alias: Dict[str, List[LineageEdge]] = {}
        for edge in join_edges:
            alias = edge.join_alias or edge.source_table or "UNKNOWN"
            if alias not in by_alias:
                by_alias[alias] = []
            by_alias[alias].append(edge)

        rows = []
        for alias, edges in sorted(by_alias.items()):
            # Collect unique keys and filters
            keys = set()
            filters = set()
            tables = set()
            join_type = ""

            for edge in edges:
                if edge.join_keys:
                    keys.add(edge.join_keys)
                if edge.join_filters:
                    filters.add(edge.join_filters)
                if edge.source_table:
                    tables.add(edge.source_table)
                if edge.notes and "JOIN" in edge.notes:
                    join_type = edge.notes.replace(" JOIN", "")

            rows.append({
                "object_name": edges[0].object_name,
                "join_alias": alias,
                "join_type": join_type or "INNER",
                "join_keys": " AND ".join(sorted(keys)),
                "join_filters": " AND ".join(sorted(filters)),
                "tables_involved": ", ".join(sorted(tables))
            })

        return pd.DataFrame(rows)

    def _build_summary_df(self, lineage_data: LineageData) -> pd.DataFrame:
        """Build DM_validation_summary DataFrame."""
        # Compute statistics
        total = len(lineage_data.edges)
        mapping_count = sum(1 for e in lineage_data.edges if e.usage_type == UsageType.MAPPING)
        join_count = sum(1 for e in lineage_data.edges if e.usage_type == UsageType.JOIN)

        physical_count = sum(1 for e in lineage_data.edges if e.source_type == SourceType.PHYSICAL)
        constant_count = sum(1 for e in lineage_data.edges if e.source_type == SourceType.CONSTANT)
        unresolved_count = sum(1 for e in lineage_data.edges if e.source_type == SourceType.UNRESOLVED)

        dm_match_y = sum(1 for e in lineage_data.edges if e.dm_match == "Y")
        dm_match_n = sum(1 for e in lineage_data.edges if e.dm_match == "N" and e.source_type == SourceType.PHYSICAL)

        # Collect unique tables
        source_tables = set()
        target_tables = set()
        for edge in lineage_data.edges:
            if edge.source_table:
                source_tables.add(edge.source_table)
            if edge.target_table:
                target_tables.add(edge.target_table)

        rows = [
            {"metric": "Total Edges", "value": total},
            {"metric": "Mapping Edges", "value": mapping_count},
            {"metric": "Join Edges", "value": join_count},
            {"metric": "", "value": ""},
            {"metric": "Physical Sources", "value": physical_count},
            {"metric": "Constants", "value": constant_count},
            {"metric": "Unresolved", "value": unresolved_count},
            {"metric": "", "value": ""},
            {"metric": "DM Match (Y)", "value": dm_match_y},
            {"metric": "DM Match (N)", "value": dm_match_n},
            {"metric": "", "value": ""},
            {"metric": "Unique Source Tables", "value": len(source_tables)},
            {"metric": "Unique Target Tables", "value": len(target_tables)},
            {"metric": "", "value": ""},
            {"metric": "Source Tables", "value": ", ".join(sorted(source_tables))},
            {"metric": "Target Tables", "value": ", ".join(sorted(target_tables))},
        ]

        return pd.DataFrame(rows)

    def _build_qa_df(self, deep_lineage: LineageData,
                     simple_lineage: LineageData) -> pd.DataFrame:
        """Build QA comparison DataFrame."""
        # Extract unique source references from each
        def get_sources(data: LineageData) -> set:
            sources = set()
            for edge in data.edges:
                if edge.source_table and edge.source_column:
                    sources.add((edge.source_table.upper(), edge.source_column.upper()))
            return sources

        deep_sources = get_sources(deep_lineage)
        simple_sources = get_sources(simple_lineage)

        # Find differences
        only_in_deep = deep_sources - simple_sources
        only_in_simple = simple_sources - deep_sources
        in_both = deep_sources & simple_sources

        rows = []

        for table, column in sorted(in_both):
            rows.append({
                "source_table": table,
                "source_column": column,
                "in_deep": "Y",
                "in_simple": "Y",
                "status": "MATCH"
            })

        for table, column in sorted(only_in_deep):
            rows.append({
                "source_table": table,
                "source_column": column,
                "in_deep": "Y",
                "in_simple": "N",
                "status": "DEEP_ONLY"
            })

        for table, column in sorted(only_in_simple):
            rows.append({
                "source_table": table,
                "source_column": column,
                "in_deep": "N",
                "in_simple": "Y",
                "status": "SIMPLE_ONLY"
            })

        df = pd.DataFrame(rows)

        # Sort by status then table/column
        if not df.empty:
            status_order = {"MATCH": 0, "DEEP_ONLY": 1, "SIMPLE_ONLY": 2}
            df["_sort"] = df["status"].map(status_order)
            df = df.sort_values(["_sort", "source_table", "source_column"])
            df = df.drop(columns=["_sort"])

        return df.reset_index(drop=True)

    def write_batch(self, results: List[Dict]) -> List[Path]:
        """
        Write multiple lineage results.

        Args:
            results: List of dicts with 'lineage_data', 'simple_lineage', 'object_name'

        Returns:
            List of output file paths
        """
        paths = []
        for result in results:
            path = self.write(
                lineage_data=result["lineage_data"],
                simple_lineage=result.get("simple_lineage"),
                object_name=result.get("object_name")
            )
            paths.append(path)
        return paths
