#!/usr/bin/env python
"""
SQL Lineage Parser - Extract column-level lineage from SQL files.

Usage:
    # Single file
    python sql_lineage_parser.py --doc-support Doc-Support.xlsx --sql T2T_LOAN.sql --output output/

    # Batch mode
    python sql_lineage_parser.py --doc-support Doc-Support.xlsx --sql-dir sql_files/ --output output/

    # With EDD files
    python sql_lineage_parser.py --doc-support Doc-Support.xlsx --sql-dir sql_files/ \\
        --edd source_edd.xlsx --output output/
"""

import argparse
import sys
from pathlib import Path
from typing import List, Optional

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from lineage_explorer.parser.loader import DocSupportLoader, SQLFileLoader
from lineage_explorer.parser.processor import LineageExtractor
from lineage_explorer.parser.output import ExcelWriter


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Extract column-level lineage from SQL files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # Input files
    parser.add_argument(
        "--doc-support",
        type=Path,
        required=True,
        help="Path to Doc-Support Excel file"
    )
    parser.add_argument(
        "--sql",
        type=Path,
        help="Path to single SQL file"
    )
    parser.add_argument(
        "--sql-dir",
        type=Path,
        help="Path to directory containing SQL files"
    )
    parser.add_argument(
        "--edd",
        type=Path,
        action="append",
        help="Path to EDD file (can be specified multiple times)"
    )

    # Sheet patterns
    parser.add_argument(
        "--mapping-sheet",
        type=str,
        help="Pattern to match mapping sheet name"
    )
    parser.add_argument(
        "--dm-sheet",
        type=str,
        help="Pattern to match DM Atomic sheet name"
    )

    # Output
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output"),
        help="Output directory (default: output/)"
    )

    # Options
    parser.add_argument(
        "--dialect",
        type=str,
        default="oracle",
        choices=["oracle", "tsql", "mysql", "postgres", "bigquery", "snowflake"],
        help="SQL dialect (default: oracle)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output"
    )
    parser.add_argument(
        "--qa",
        action="store_true",
        help="Include QA comparison (simple vs deep)"
    )

    args = parser.parse_args()

    # Validation
    if not args.sql and not args.sql_dir:
        parser.error("Must specify either --sql or --sql-dir")

    if args.sql and args.sql_dir:
        parser.error("Cannot specify both --sql and --sql-dir")

    return args


def main():
    """Main entry point."""
    args = parse_args()

    if args.verbose:
        print("=" * 60)
        print("SQL Lineage Parser")
        print("=" * 60)

    # Load Doc-Support
    print(f"\nLoading Doc-Support: {args.doc_support}")
    doc_loader = DocSupportLoader()
    try:
        mappings_df, schema = doc_loader.load(
            doc_support_path=args.doc_support,
            edd_paths=args.edd,
            mapping_sheet=args.mapping_sheet,
            dm_sheet=args.dm_sheet
        )
    except Exception as e:
        print(f"Error loading Doc-Support: {e}")
        sys.exit(1)

    if args.verbose:
        print(f"  Schema tables: {len(schema)}")
        print(f"  Mapping rows: {len(mappings_df)}")

    # Load SQL files
    sql_loader = SQLFileLoader()

    if args.sql:
        print(f"\nLoading SQL file: {args.sql}")
        try:
            sql_files = [sql_loader.load_file(args.sql)]
        except Exception as e:
            print(f"Error loading SQL: {e}")
            sys.exit(1)
    else:
        print(f"\nLoading SQL files from: {args.sql_dir}")
        try:
            sql_files = sql_loader.load_directory(args.sql_dir)
        except Exception as e:
            print(f"Error loading SQL directory: {e}")
            sys.exit(1)

    if not sql_files:
        print("No SQL files found")
        sys.exit(1)

    print(f"  Found {len(sql_files)} SQL file(s)")

    # Initialize extractor and writer
    extractor = LineageExtractor(schema=schema, dialect=args.dialect)
    writer = ExcelWriter(args.output)

    # Process each file
    print(f"\nProcessing...")
    results = []

    for object_name, sql_content in sql_files:
        if args.verbose:
            print(f"  Processing: {object_name}")

        # Get target table from mappings
        target_table = _get_target_table(mappings_df, object_name)

        # Normalize SQL
        sql_normalized = SQLFileLoader.extract_select(sql_content)
        if not sql_normalized:
            print(f"    Warning: Could not extract SELECT from {object_name}")
            continue

        # Extract deep lineage
        lineage_data = extractor.extract(
            sql=sql_normalized,
            object_name=object_name,
            target_table=target_table
        )

        # Extract simple lineage for QA
        simple_lineage = None
        if args.qa:
            simple_lineage = extractor.extract_simple_lineage(
                sql=sql_normalized,
                object_name=object_name,
                target_table=target_table
            )

        results.append({
            "object_name": object_name,
            "lineage_data": lineage_data,
            "simple_lineage": simple_lineage
        })

        if args.verbose:
            print(f"    Edges: {len(lineage_data.edges)}")

    # Write output
    print(f"\nWriting output to: {args.output}")
    output_paths = writer.write_batch(results)

    print(f"\nComplete!")
    print(f"  Processed: {len(results)} file(s)")
    print(f"  Output files: {len(output_paths)}")

    for path in output_paths:
        print(f"    - {path.name}")


def _get_target_table(mappings_df, object_name: str) -> str:
    """Get target table for object from mappings."""
    if mappings_df.empty:
        return object_name

    # Filter for this object
    if "name" in mappings_df.columns:
        mask = mappings_df["name"].str.upper() == object_name.upper()
        filtered = mappings_df[mask]
    else:
        filtered = mappings_df

    # Get target table
    if "target_table" in filtered.columns and len(filtered) > 0:
        target = filtered["target_table"].iloc[0]
        if target and str(target).strip():
            return str(target).strip().upper()

    return object_name


if __name__ == "__main__":
    main()
