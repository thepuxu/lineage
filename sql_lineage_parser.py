#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
SQL Lineage Parser - Single File Implementation
================================================================================
Extracts column-level physical data lineage from Oracle T2T SQL files.

Version: 1.0.0
Created: 2025-01-05

Usage:
    python sql_lineage_parser.py --doc-support DOC-SUPPORT.xlsx --sql file.sql
    python sql_lineage_parser.py --doc-support DOC-SUPPORT.xlsx --sql-dir sql_files/

Dependencies:
    pip install sqlglot pandas openpyxl
================================================================================
"""

# =============================================================================
# SECTION 0: IMPORTS
# =============================================================================

from __future__ import annotations

import argparse
import json
import logging
import re
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

# Check dependencies
try:
    import sqlglot
    from sqlglot import exp
except ImportError:
    print("ERROR: sqlglot not installed. Run: pip install sqlglot")
    sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas not installed. Run: pip install pandas openpyxl")
    sys.exit(1)

try:
    import openpyxl
except ImportError:
    print("ERROR: openpyxl not installed. Run: pip install openpyxl")
    sys.exit(1)

# =============================================================================
# SECTION 1: CONSTANTS & ENUMS
# =============================================================================

VERSION = "1.0.0"
TRACE_LEVEL = 5  # Custom log level below DEBUG

# Register TRACE level
logging.addLevelName(TRACE_LEVEL, 'TRACE')


class SourceType(Enum):
    """Type of source for lineage."""
    PHYSICAL = "PHYSICAL"
    CONSTANT = "CONSTANT"
    UNRESOLVED = "UNRESOLVED"


class UnresolvedReason(Enum):
    """Reason codes for unresolved lineage."""
    DEPTH_GUARD = "DEPTH_GUARD"
    ALIAS_NOT_FOUND = "ALIAS_NOT_FOUND"
    MISSING_PROJECTION = "MISSING_PROJECTION"
    COLUMN_NOT_FOUND = "COLUMN_NOT_FOUND"
    AMBIGUOUS = "AMBIGUOUS"
    CYCLE_DETECTED = "CYCLE_DETECTED"
    PARSER_LIMITATION = "PARSER_LIMITATION"
    PARTIAL_FAILURE = "PARTIAL_FAILURE"
    COMPLETE_FAILURE = "COMPLETE_FAILURE"
    STAR_EXPANSION_FAILED = "STAR_EXPANSION_FAILED"
    DYNAMIC_SQL = "DYNAMIC_SQL"
    REMOTE_DBLINK = "REMOTE_DBLINK"


# Oracle constant patterns
ORACLE_CONSTANTS = [
    r"^NULL$",
    r"^SYSDATE$",
    r"^SYSTIMESTAMP$",
    r"^CURRENT_DATE$",
    r"^CURRENT_TIMESTAMP$",
    r"^ROWNUM$",
    r"^ROWID$",
    r"^LEVEL$",
    r"^USER$",
    r"^SYS_GUID\(\s*\)$",
    r"^USERENV\s*\(.+\)$",
    r"^SYS_CONTEXT\s*\(.+\)$",
    r"^'(?:[^']|'')*'$",            # String literal (handles Oracle doubled quotes)
    r"^N'(?:[^']|'')*'$",           # National string literal (handles Oracle doubled quotes)
    r"^-?[0-9]+(\.[0-9]+)?$",      # Numeric
    r"^DATE\s*'(?:[^']|'')*'$",     # Date literal (handles Oracle doubled quotes)
    r"^TIMESTAMP\s*'(?:[^']|'')*'$", # Timestamp literal (handles Oracle doubled quotes)
    r"^INTERVAL\s*'(?:[^']|'')*'",  # Interval literal (handles Oracle doubled quotes)
    r"^\$[A-Z_][A-Z0-9_]*$",       # Parameter
    r"^\[[A-Z_][A-Z0-9_]*\]$",     # Macro
    r"^:[A-Z_][A-Z0-9_]*$",        # Bind variable
    r"^UNPIVOT_VALUE\(.+\)$",      # UNPIVOT value column marker
    r"^UNPIVOT_FOR\(.+\)$",        # UNPIVOT FOR column marker
]
CONSTANT_PATTERNS = [re.compile(p, re.IGNORECASE) for p in ORACLE_CONSTANTS]

# SQL aggregate functions (output is derived, not direct column)
AGGREGATE_FUNCTIONS = {
    'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'STDDEV', 'VARIANCE',
    'LISTAGG', 'WM_CONCAT', 'XMLAGG', 'COLLECT',
}

# Required columns for validation
REQUIRED_MAPPING_COLUMNS = ['Type', 'Name', 'Target Table', 'Target Column']
OPTIONAL_MAPPING_COLUMNS = ['Source Column', 'Expression']
REQUIRED_DM_COLUMNS = ['Table Physical Name', 'Column Physical Name']

# Output column definitions
MAPPING_COLUMNS = [
    'sql_file', 'object_name', 'row_type', 'dest_table', 'dest_field', 'source_type',
    'source_table', 'source_field', 'constant_value', 'expression',
    'dm_match', 'trace_path', 'notes',
    # FIX 24: Expression context columns
    'source_alias', 'original_ref', 'full_expression',
    # Join-specific columns (empty for MAPPING rows)
    'table_alias', 'join_seq', 'join_type', 'join_side', 'field_role',
    'join_condition', 'context_path'
]

JOIN_COLUMNS = [
    'sql_file', 'object_name', 'join_seq', 'join_type', 'left_table', 'left_field',
    'right_table', 'right_field', 'join_condition', 'join_filters', 'context_path'
]

JOIN_KEYS_EXPLODED_COLUMNS = [
    'sql_file', 'object_name', 'join_seq', 'join_type', 'join_side',
    'field_role', 'original_alias', 'original_field', 'source_type',
    'physical_table', 'physical_field', 'constant_value', 'dm_match',
    'trace_path', 'unresolved_reason', 'join_condition', 'context_path',
    # FIX 24: Full expression context for join keys
    'full_expression'
]

# MAPPING_JOIN_RELATION_COLUMNS removed - joins now merged into MAPPING_COLUMNS

SUMMARY_COLUMNS = [
    'sql_file', 'metric', 'value', 'notes'
]

QA_COLUMNS = [
    'sql_file', 'object_name', 'check_type', 'dest_field', 'deep_sources',
    'simple_sources', 'match', 'notes'
]

MANIFEST_FILE = "processing_manifest.json"

# =============================================================================
# SECTION 2: DATA CLASSES
# =============================================================================

@dataclass
class ProjectionDef:
    """Definition of a SELECT list item."""
    output_name: str
    expression: str
    source_refs: List[str] = field(default_factory=list)
    origin_alias: Optional[str] = None  # Which alias.* expansion this projection came from


@dataclass
class Scope:
    """Represents a query scope (main query or subquery)."""
    name: str
    parent: Optional['Scope'] = None
    relations: Dict[str, Union[str, 'Scope']] = field(default_factory=dict)
    projections: Dict[str, ProjectionDef] = field(default_factory=dict)
    ctes: Dict[str, 'Scope'] = field(default_factory=dict)
    union_branches: List['Scope'] = field(default_factory=list)
    joins: List['JoinKey'] = field(default_factory=list)  # Joins belonging to THIS scope


@dataclass
class ResolvedColumn:
    """A fully resolved physical column."""
    source_type: SourceType
    table: str = ""
    column: str = ""
    constant_value: str = ""
    trace_path: str = ""
    reason: str = ""
    dm_match: str = "N"
    # FIX 24: Expression context fields
    source_alias: str = ""      # Original alias (e.g., "t" from "some_table t")
    original_ref: str = ""      # Original reference (e.g., "t.column_a")
    full_expression: str = ""   # Full expression context (e.g., "COALESCE(t.col, 0)")


@dataclass
class JoinKey:
    """A single join key relationship."""
    sql_file: str = ""  # Source SQL filename (for combined output)
    join_seq: int = 0
    join_type: str = ""
    left_table: str = ""
    left_field: str = ""
    right_table: str = ""
    right_field: str = ""
    join_condition: str = ""
    join_filters: str = ""
    context_path: str = ""


@dataclass
class JoinKeyExploded:
    """A join key field resolved to physical source."""
    sql_file: str = ""
    object_name: str = ""
    join_seq: int = 0
    join_type: str = ""
    join_side: str = ""          # LEFT, RIGHT, or FILTER
    field_role: str = ""         # KEY or FILTER
    original_alias: str = ""
    original_field: str = ""
    source_type: str = ""        # PHYSICAL, CONSTANT, UNRESOLVED
    physical_table: str = ""
    physical_field: str = ""
    constant_value: str = ""
    dm_match: str = ""
    trace_path: str = ""
    unresolved_reason: str = ""
    join_condition: str = ""
    context_path: str = ""
    # FIX 24: Full expression context
    full_expression: str = ""    # The complete expression (for complex filters)


@dataclass
class LineageEdge:
    """A single lineage relationship (MAPPING or JOIN)."""
    sql_file: str = ""  # Source SQL filename (for combined output)
    object_name: str = ""
    row_type: str = "MAPPING"  # "MAPPING" or "JOIN"
    dest_table: str = ""
    dest_field: str = ""
    source_type: str = ""
    source_table: str = ""
    source_field: str = ""
    constant_value: str = ""
    expression: str = ""  # Full expression context (e.g., "COALESCE(t.col, 0)")
    dm_match: str = ""
    trace_path: str = ""
    notes: str = ""
    # FIX 24: Expression context fields
    source_alias: str = ""      # Original alias used (e.g., "t" from "some_table t")
    original_ref: str = ""      # Original reference as written (e.g., "t.column_a")
    full_expression: str = ""   # Full SQL expression containing the column (e.g., "COALESCE(t.col, 0)")
    # Join-specific fields (empty for MAPPING rows)
    table_alias: str = ""
    join_seq: str = ""  # String to allow empty for mappings
    join_type: str = ""
    join_side: str = ""
    field_role: str = ""
    join_condition: str = ""
    context_path: str = ""


@dataclass
class LineageResult:
    """Complete result for one SQL file."""
    object_name: str
    sql_content: str = ""  # Original SQL content (for QA sheet in combined output)
    edges: List[LineageEdge] = field(default_factory=list)  # Now includes both MAPPING and JOIN rows
    joins: List[JoinKey] = field(default_factory=list)
    joins_exploded: List[JoinKeyExploded] = field(default_factory=list)  # Kept for backward compatibility
    warnings: List[str] = field(default_factory=list)
    stats: Dict[str, int] = field(default_factory=dict)


# Global logger
logger = logging.getLogger('sql_lineage_parser')


# =============================================================================
# SECTION 3: UTILITY FUNCTIONS
# =============================================================================

def safe_str(value: Any, default: str = '', strip: bool = True, normalize_ws: bool = True) -> str:
    """Safely convert any value to string, handling None/NaN/whitespace."""
    if value is None:
        return default
    if isinstance(value, float) and pd.isna(value):
        return default
    result = str(value)

    # Normalize whitespace
    if normalize_ws:
        result = re.sub(r'\s+', ' ', result)

    if strip:
        result = result.strip()

    # Check for empty or 'nan' string
    if result == '' or result.lower() == 'nan':
        return default

    return result


def normalize_whitespace(value: str) -> str:
    """Normalize all whitespace in a string."""
    if not value:
        return ''
    result = re.sub(r'\s+', ' ', value)
    return result.strip()


def normalize_identifier(value: str) -> str:
    """Normalize identifier: trim, uppercase, handle quotes."""
    if not value:
        return ''
    value = normalize_whitespace(value)
    # Remove surrounding quotes if present
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    return value.upper()


def normalize_expression(expr: str) -> str:
    """Normalize expression: whitespace, case (preserve literals)."""
    if not expr:
        return ''

    # Step 1: Protect string literals
    literals: List[str] = []
    def save_literal(match: re.Match) -> str:
        literals.append(match.group(0))
        return f"__LITERAL_{len(literals)-1}__"

    # Handle Oracle doubled quotes: 'O''Reilly' -> matches as single literal
    protected = re.sub(r"'(?:[^']|'')*'", save_literal, expr)

    # Step 2: Normalize whitespace
    protected = re.sub(r'\s+', ' ', protected).strip()

    # Step 3: Uppercase (identifiers only)
    protected = protected.upper()

    # Step 4: Restore literals
    for i, lit in enumerate(literals):
        protected = protected.replace(f"__LITERAL_{i}__", lit)

    return protected


def is_constant(value: str) -> bool:
    """Check if value matches constant pattern."""
    value = value.strip()
    for pattern in CONSTANT_PATTERNS:
        if pattern.match(value):
            return True
    return False


def parse_ref(ref: str) -> Tuple[Optional[str], str]:
    """Parse ALIAS.COLUMN or COLUMN reference."""
    ref = normalize_whitespace(ref).upper()
    if '.' in ref:
        parts = ref.split('.', 1)
        return parts[0].strip(), parts[1].strip()
    return None, ref.strip()


def find_closest_match(target: str, candidates: List[str]) -> Optional[str]:
    """Find closest string match using simple edit distance."""
    if not candidates:
        return None

    target = target.upper()
    best_match = None
    best_score = 0

    for candidate in candidates:
        candidate_upper = candidate.upper()
        # Simple matching: count common characters
        common = sum(1 for c in target if c in candidate_upper)
        score = common / max(len(target), len(candidate_upper))
        if score > best_score:
            best_score = score
            best_match = candidate

    return best_match if best_score > 0.5 else candidates[0] if candidates else None


def read_file_with_retry(path: Path, max_attempts: int = 3, delay: float = 0.5) -> str:
    """Read file with retry for transient failures."""
    last_error: Optional[Exception] = None

    for attempt in range(max_attempts):
        try:
            return path.read_text(encoding='utf-8')
        except (IOError, PermissionError) as e:
            last_error = e
            if attempt < max_attempts - 1:
                logger.warning(f"Retry {attempt + 1}/{max_attempts} for {path}: {e}")
                time.sleep(delay)

    raise IOError(f"Failed to read {path} after {max_attempts} attempts: {last_error}")


# =============================================================================
# SECTION 4: EXCEL LOADERS
# =============================================================================

def validate_excel_schema(df: pd.DataFrame, required: List[str], sheet_name: str) -> Tuple[bool, List[str]]:
    """Validate Excel sheet has required columns."""
    actual = [c.strip() for c in df.columns]
    missing = [c for c in required if c not in actual]

    if missing:
        return False, [f"Sheet '{sheet_name}' missing columns: {missing}"]
    return True, []


def load_doc_support(path: Path) -> Tuple[pd.DataFrame, Dict[str, Set[str]]]:
    """Load DOC-SUPPORT Excel workbook."""
    if not path.exists():
        logger.error(f"Doc-Support file not found: {path}")
        sys.exit(2)

    logger.info(f"Loading Doc-Support: {path}")

    temp_path = None
    file_to_open = path

    try:
        xlsx = pd.ExcelFile(path, engine='openpyxl')
    except PermissionError:
        # File is locked (probably open in Excel) - copy to temp
        logger.warning(f"File is locked, copying to temp location...")
        try:
            temp_dir = tempfile.mkdtemp()
            temp_path = Path(temp_dir) / path.name
            shutil.copy2(path, temp_path)
            file_to_open = temp_path
            xlsx = pd.ExcelFile(temp_path, engine='openpyxl')
            logger.info(f"Successfully opened from temp copy")
        except Exception as e2:
            logger.error(f"Failed to open Excel file (even from temp): {e2}")
            sys.exit(2)
    except Exception as e:
        logger.error(f"Failed to open Excel file: {e}")
        sys.exit(2)

    # Find T2T-F2T Mappings sheet (exact name)
    mapping_sheet = None
    for sheet in xlsx.sheet_names:
        if sheet.strip() == "T2T-F2T Mappings":
            mapping_sheet = sheet
            break

    if not mapping_sheet:
        logger.error("Sheet 'T2T-F2T Mappings' not found in Excel")
        sys.exit(2)

    # Load mappings
    mappings_df = pd.read_excel(xlsx, sheet_name=mapping_sheet)
    # Normalize column names: strip whitespace
    mappings_df.columns = mappings_df.columns.str.strip()
    valid, errors = validate_excel_schema(mappings_df, REQUIRED_MAPPING_COLUMNS, mapping_sheet)
    if not valid:
        logger.error("\n".join(errors))
        sys.exit(2)

    # Normalize mapping columns
    for col in ['Type', 'Name']:
        if col in mappings_df.columns:
            mappings_df[col] = mappings_df[col].apply(lambda x: safe_str(x).upper())

    for col in ['Target Table', 'Target Column']:
        if col in mappings_df.columns:
            mappings_df[col] = mappings_df[col].apply(lambda x: normalize_identifier(safe_str(x)))

    if 'Expression' in mappings_df.columns:
        mappings_df['Expression'] = mappings_df['Expression'].apply(lambda x: normalize_expression(safe_str(x)))

    if 'Source Column' in mappings_df.columns:
        mappings_df['Source Column'] = mappings_df['Source Column'].apply(lambda x: normalize_expression(safe_str(x)))

    logger.info(f"Loaded {len(mappings_df)} mapping rows")

    # Find DM ATOMIC sheet (pattern match)
    dm_sheet = None
    for sheet in xlsx.sheet_names:
        if sheet.strip().startswith("DM ATOMIC"):
            dm_sheet = sheet
            break

    dm_dict: Dict[str, Set[str]] = {}
    if dm_sheet:
        dm_df = pd.read_excel(xlsx, sheet_name=dm_sheet)
        # Normalize column names: strip whitespace
        dm_df.columns = dm_df.columns.str.strip()
        valid, errors = validate_excel_schema(dm_df, REQUIRED_DM_COLUMNS, dm_sheet)
        if valid:
            dm_dict = build_dm_dictionary(dm_df)
            logger.info(f"Loaded DM ATOMIC: {len(dm_dict)} tables")
        else:
            logger.warning(f"DM sheet validation failed: {errors}")
    else:
        logger.warning("No 'DM ATOMIC*' sheet found - dm_match will be 'N' for all")

    # Clean up temp file if we created one
    if temp_path and temp_path.exists():
        try:
            temp_path.unlink()
            temp_path.parent.rmdir()
        except Exception:
            pass  # Best effort cleanup

    return mappings_df, dm_dict


def build_dm_dictionary(dm_df: pd.DataFrame) -> Dict[str, Set[str]]:
    """Build DM dictionary with sets for fast lookup."""
    dm_dict: Dict[str, Set[str]] = {}

    for _, row in dm_df.iterrows():
        table = normalize_identifier(safe_str(row.get('Table Physical Name')))
        column = normalize_identifier(safe_str(row.get('Column Physical Name')))
        if table and column:
            if table not in dm_dict:
                dm_dict[table] = set()
            dm_dict[table].add(column)

    return dm_dict


def find_column_case_insensitive(df: pd.DataFrame, col_name: str) -> Optional[str]:
    """Find column name with case-insensitive matching and whitespace trimming."""
    col_lower = col_name.lower().strip()
    for col in df.columns:
        if col.lower().strip() == col_lower:
            return col
    return None


def filter_mappings(df: pd.DataFrame, object_name: str) -> pd.DataFrame:
    """Filter mappings for specific object.

    Filters by Name column (required) and optionally by Type='T2T' if Type column exists.
    Column names are matched case-insensitively.
    Handles NaN values, extra whitespace, and non-string types safely.
    """
    object_name = object_name.upper().strip()

    # Helper to safely normalize values (handles NaN, whitespace, non-strings)
    def normalize_value(val) -> str:
        if pd.isna(val):
            return ''
        return str(val).upper().strip()

    # Find Name column (case-insensitive)
    name_col = find_column_case_insensitive(df, 'Name')
    if not name_col:
        raise KeyError(f"Required column 'Name' not found. Available columns: {list(df.columns)}")

    # Always filter by Name (using apply for safe handling)
    name_mask = df[name_col].apply(normalize_value) == object_name

    # Optionally filter by Type='T2T' if column exists (case-insensitive)
    type_col = find_column_case_insensitive(df, 'Type')
    if type_col:
        type_mask = df[type_col].apply(normalize_value) == 'T2T'
        mask = name_mask & type_mask
    else:
        # No Type column - just filter by Name
        mask = name_mask

    return df[mask].copy()


def column_exists_in_dm(table: str, column: str, dm_dict: Optional[Dict[str, Set[str]]]) -> bool:
    """O(1) lookup for column existence."""
    if dm_dict is None:
        return False
    return column.upper() in dm_dict.get(table.upper(), set())


def get_dm_columns(table: str, dm_dict: Dict[str, Set[str]]) -> Set[str]:
    """Get all columns for a table from DM."""
    return dm_dict.get(table.upper(), set())


# =============================================================================
# SECTION 5: SQL FILE LOADER
# =============================================================================

def load_sql_file(path: Path) -> Tuple[str, str]:
    """Load SQL file and return (object_name, content)."""
    object_name = path.stem.upper()
    content = read_file_with_retry(path)
    return object_name, content


def load_sql_directory(dir_path: Path) -> List[Path]:
    """Find all SQL files in directory (recursive)."""
    if not dir_path.exists():
        logger.error(f"SQL directory not found: {dir_path}")
        sys.exit(3)

    # Search recursively for SQL files in subdirectories
    sql_files = list(dir_path.glob("**/*.sql"))
    if not sql_files:
        logger.error(f"No SQL files found in: {dir_path}")
        sys.exit(3)

    return sorted(sql_files)


def validate_sql_content(sql: str, filepath: Path) -> Tuple[bool, str, List[str]]:
    """Validate SQL content before parsing."""
    warnings: List[str] = []

    if not sql or not sql.strip():
        return False, "SQL file is empty or whitespace only", []

    if len(sql.strip()) < 10:
        return False, f"SQL too short ({len(sql)} chars), likely invalid", []

    if not re.search(r'\bSELECT\b', sql, re.IGNORECASE):
        return False, "No SELECT keyword found", []

    # Detect potential issues
    if re.search(r'\bEXECUTE\s+IMMEDIATE\b', sql, re.IGNORECASE):
        warnings.append("Contains EXECUTE IMMEDIATE - dynamic SQL cannot be fully traced")

    if re.search(r'@\w+', sql):
        warnings.append("Contains DB links (@) - remote tables may not resolve")

    if sql.count('(') != sql.count(')'):
        warnings.append(f"Unbalanced parentheses: {sql.count('(')} open, {sql.count(')')} close")

    return True, sql.strip(), warnings


def strip_comments(sql: str) -> str:
    """Remove SQL comments while preserving string literals.

    The simple regex approach breaks when -- or /* appear inside strings:
    e.g., '---- NOT FOUND' would have '-- NOT FOUND' stripped.

    This parser tracks string state and only strips comments outside strings.
    """
    result = []
    i = 0
    in_string = False

    while i < len(sql):
        # Handle string literals (single quotes, with '' escape)
        if sql[i] == "'" and not in_string:
            in_string = True
            result.append(sql[i])
            i += 1
        elif sql[i] == "'" and in_string:
            result.append(sql[i])
            # Check for escaped quote ''
            if i + 1 < len(sql) and sql[i + 1] == "'":
                result.append(sql[i + 1])
                i += 2
            else:
                in_string = False
                i += 1
        # Skip -- comments (only outside strings)
        elif not in_string and sql[i:i+2] == '--':
            # Skip until end of line
            while i < len(sql) and sql[i] != '\n':
                i += 1
        # Skip /* */ comments (only outside strings)
        elif not in_string and sql[i:i+2] == '/*':
            # Skip until */
            i += 2
            while i < len(sql) - 1 and sql[i:i+2] != '*/':
                i += 1
            i += 2  # Skip the */
        else:
            result.append(sql[i])
            i += 1

    return ''.join(result)


def normalize_sql(sql: str, debug: bool = False) -> str:
    """Clean and normalize SQL content."""
    def _quote_count(s: str, step: str = "") -> int:
        count = s.count("'")
        if debug:
            logger.info(f"  Quote count after {step}: {count}")
        return count

    if debug:
        _quote_count(sql, "original")

    # Strip ANSI escape codes in various formats
    # Standard escape character \x1b (ESC)
    sql = re.sub(r'\x1b\[[0-9;]*m', '', sql)
    # Octal escape \033
    sql = re.sub(r'\033\[[0-9;]*m', '', sql)
    # Literal "esc" text (in case stored as text)
    sql = re.sub(r'esc\[[0-9;]*m', '', sql, flags=re.IGNORECASE)
    if debug:
        _quote_count(sql, "ANSI strip")

    # Replace Unicode whitespace characters (em space, en space, etc.)
    sql = re.sub(r'[\u2003\u2002\u2001\u2000\u00A0]+', ' ', sql)

    # Replace common placeholders with string literals
    # These are parameters that appear in T2T SQL files
    sql = re.sub(r'\[VARTECHSRC\]', "'PLACEHOLDER_VARTECHSRC'", sql, flags=re.IGNORECASE)
    sql = re.sub(r'\[PERIMETER\]', "'PLACEHOLDER_PERIMETER'", sql, flags=re.IGNORECASE)
    if debug:
        _quote_count(sql, "[PLACEHOLDER] replace")

    # Replace $VARIABLE patterns with placeholder values
    # First handle already-quoted: '$VAR' → 'PLACEHOLDER_VAR' (avoid double quotes)
    sql = re.sub(r"'\$([A-Z_][A-Z0-9_]*)'", r"'PLACEHOLDER_\1'", sql, flags=re.IGNORECASE)
    if debug:
        _quote_count(sql, "quoted $VAR")

    # Handle date-related variables: $MISDATE, $DATE_START, etc. → DATE literal
    # This prevents tokenizing errors when used in BETWEEN or date comparisons
    sql = re.sub(r'\$([A-Z_]*DATE[A-Z0-9_]*)\b', "DATE '2025-09-30'", sql, flags=re.IGNORECASE)
    if debug:
        _quote_count(sql, "$DATE vars")

    # Then handle remaining unquoted: $VAR → 'PLACEHOLDER_VAR'
    sql = re.sub(r'\$([A-Z_][A-Z0-9_]*)\b', r"'PLACEHOLDER_\1'", sql, flags=re.IGNORECASE)
    if debug:
        _quote_count(sql, "unquoted $VAR")

    # Fix unquoted ~ between concatenation operators
    # Pattern: || ~ || should become || '~' ||
    sql = re.sub(r'\|\|\s*~\s*\|\|', "|| '~' ||", sql)
    if debug:
        _quote_count(sql, "tilde fix")

    # Fix double alias: "ALIAS AS ALIAS" → "ALIAS" (when both are identical)
    # Keep first alias, remove redundant "AS ALIAS"
    sql = re.sub(r'\b([A-Z_][A-Z0-9_]*)\s+AS\s+\1\b', r'\1', sql, flags=re.IGNORECASE)

    # Normalize backslash escapes to Oracle style (doubled quotes)
    # Convert \' to '' for Oracle compatibility
    sql = re.sub(r"\\'", "''", sql)
    if debug:
        _quote_count(sql, "backslash escape")

    sql = strip_comments(sql)
    if debug:
        _quote_count(sql, "strip_comments")

    sql = normalize_whitespace(sql)
    return sql


def diagnose_sql_issues(sql: str) -> List[str]:
    """Analyze SQL for common issues and return diagnostic messages.

    Helps users understand why SQL parsing might have failed.
    """
    issues = []
    sql_upper = sql.upper()

    # Count CASE vs END (simple heuristic)
    # END not followed by LOOP/IF/FUNCTION/PROCEDURE/PACKAGE (those are PL/SQL)
    case_count = len(re.findall(r'\bCASE\b', sql_upper))
    end_count = len(re.findall(r'\bEND\b(?!\s+(LOOP|IF|FUNCTION|PROCEDURE|PACKAGE))', sql_upper))

    if case_count != end_count:
        issues.append(f"CASE/END mismatch: {case_count} CASE vs {end_count} END")

    # Check for unmatched parentheses
    open_parens = sql.count('(')
    close_parens = sql.count(')')
    if open_parens != close_parens:
        issues.append(f"Parentheses mismatch: {open_parens} '(' vs {close_parens} ')'")

    # Check for unmatched quotes (simple check)
    single_quotes = sql.count("'") - sql.count("''") * 2  # Adjust for escaped quotes
    if single_quotes % 2 != 0:
        issues.append("Possible unclosed string literal (odd number of single quotes)")

    return issues


# =============================================================================
# SECTION 6: LINEAGE EXTRACTOR (CORE)
# =============================================================================

def extract_column_refs(expr: str) -> List[str]:
    """Extract column references from expression."""
    refs: List[str] = []

    # Protect string literals
    literals: List[str] = []
    def save_literal(match: re.Match) -> str:
        literals.append(match.group(0))
        return f"__LIT{len(literals)-1}__"

    # Handle Oracle doubled quotes: 'O''Reilly' -> matches as single literal
    protected = re.sub(r"'(?:[^']|'')*'", save_literal, expr)

    # Normalize whitespace around dots (e.g., "C . COL" -> "C.COL")
    protected = re.sub(r'\s*\.\s*', '.', protected)
    # Collapse multiple whitespace to single space
    protected = re.sub(r'\s+', ' ', protected).strip()

    # Pattern for ALIAS.COLUMN or standalone COLUMN
    # Must start with letter or underscore, can contain alphanumeric and underscore
    pattern = r'\b([A-Z_][A-Z0-9_]*(?:\.[A-Z_][A-Z0-9_]*)?)\b'

    for match in re.finditer(pattern, protected, re.IGNORECASE):
        token = match.group(1).upper()

        # Skip SQL keywords
        if token in {'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'IS',
                     'NULL', 'LIKE', 'BETWEEN', 'CASE', 'WHEN', 'THEN', 'ELSE',
                     'END', 'AS', 'ON', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER',
                     'FULL', 'CROSS', 'UNION', 'ALL', 'ORDER', 'BY', 'GROUP',
                     'HAVING', 'DISTINCT', 'EXISTS', 'ANY', 'SOME', 'TRUE', 'FALSE',
                     'WITH', 'OVER', 'PARTITION', 'ROWS', 'RANGE', 'UNBOUNDED',
                     'PRECEDING', 'FOLLOWING', 'CURRENT', 'ROW', 'ASC', 'DESC',
                     'NULLS', 'FIRST', 'LAST', 'FETCH', 'NEXT', 'ONLY', 'OFFSET',
                     'LIMIT', 'FOR', 'UPDATE', 'NOWAIT', 'SKIP', 'LOCKED'}:
            continue

        # Skip known functions without dot
        if '.' not in token and token in {
            # String functions
            'NVL', 'NVL2', 'COALESCE', 'DECODE', 'TRIM', 'LTRIM', 'RTRIM',
            'UPPER', 'LOWER', 'INITCAP', 'LENGTH', 'LENGTHB', 'SUBSTR', 'SUBSTRB',
            'INSTR', 'INSTRB', 'REPLACE', 'TRANSLATE', 'LPAD', 'RPAD',
            'CONCAT', 'ASCII', 'CHR', 'SOUNDEX', 'REGEXP_REPLACE', 'REGEXP_SUBSTR',
            'REGEXP_INSTR', 'REGEXP_LIKE', 'REGEXP_COUNT',
            # Conversion functions
            'TO_CHAR', 'TO_DATE', 'TO_NUMBER', 'TO_TIMESTAMP', 'TO_CLOB', 'TO_LOB',
            'CAST', 'CONVERT', 'HEXTORAW', 'RAWTOHEX', 'CHARTOROWID', 'ROWIDTOCHAR',
            # Numeric functions
            'ROUND', 'TRUNC', 'ABS', 'CEIL', 'FLOOR', 'MOD', 'POWER', 'SQRT',
            'SIGN', 'GREATEST', 'LEAST', 'NULLIF', 'EXP', 'LN', 'LOG', 'SIN',
            'COS', 'TAN', 'ASIN', 'ACOS', 'ATAN', 'SINH', 'COSH', 'TANH',
            'BITAND', 'WIDTH_BUCKET',
            # Date/Time functions (IMPORTANT: includes ADD_MONTHS)
            'ADD_MONTHS', 'MONTHS_BETWEEN', 'NEXT_DAY', 'LAST_DAY',
            'EXTRACT', 'SYSDATE', 'SYSTIMESTAMP', 'CURRENT_DATE', 'CURRENT_TIMESTAMP',
            'LOCALTIMESTAMP', 'DBTIMEZONE', 'SESSIONTIMEZONE', 'NEW_TIME',
            'NUMTODSINTERVAL', 'NUMTOYMINTERVAL', 'TO_DSINTERVAL', 'TO_YMINTERVAL',
            'TZ_OFFSET', 'FROM_TZ', 'SYS_EXTRACT_UTC',
            # Aggregate functions
            'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'STDDEV', 'VARIANCE', 'LISTAGG',
            'MEDIAN', 'STATS_MODE', 'PERCENTILE_CONT', 'PERCENTILE_DISC',
            'CORR', 'COVAR_POP', 'COVAR_SAMP', 'REGR_SLOPE', 'REGR_INTERCEPT',
            'CUME_DIST', 'PERCENT_RANK', 'NTILE',
            # Analytic/Window functions
            'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'LEAD', 'LAG',
            'FIRST_VALUE', 'LAST_VALUE', 'NTH_VALUE', 'RATIO_TO_REPORT',
            # Other Oracle functions
            'ROWNUM', 'ROWID', 'LEVEL', 'USER', 'UID', 'SYS_GUID',
            'USERENV', 'SYS_CONTEXT', 'DUMP', 'VSIZE', 'ORA_HASH',
            'EMPTY_BLOB', 'EMPTY_CLOB', 'BFILENAME', 'XMLELEMENT', 'XMLFOREST',
            'XMLAGG', 'XMLROOT', 'XMLPARSE', 'XMLSERIALIZE',
        }:
            continue

        # Skip literals placeholder
        if token.startswith('__LIT'):
            continue

        # Skip if it's a constant
        if is_constant(token):
            continue

        refs.append(token)

    return refs


def build_scope_tree(ast: exp.Expression, name: str = "ROOT", parent: Optional[Scope] = None,
                     dm_dict: Optional[Dict[str, Set[str]]] = None) -> Scope:
    """Build complete scope tree from AST."""
    scope = Scope(name=name, parent=parent)

    # Handle CTEs (WITH clause) - Two-pass approach for CTE chaining
    if isinstance(ast, exp.Select):
        cte_nodes = list(ast.find_all(exp.CTE))

        # PASS 1: Register all CTE names first (enables forward/cross references)
        for cte in cte_nodes:
            cte_name = cte.alias.upper() if cte.alias else ""
            if cte_name:
                scope.ctes[cte_name] = None  # Placeholder - will be filled in pass 2

        # PASS 2: Build CTE scopes (now all CTE names are visible in parent)
        for cte in cte_nodes:
            cte_name = cte.alias.upper() if cte.alias else ""
            if cte_name and cte.this:
                cte_scope = build_scope_tree(cte.this, f"{name}/CTE_{cte_name}", scope, dm_dict)
                scope.ctes[cte_name] = cte_scope

    # FIX 21: Helper to qualify column refs with source table
    def _qualify_column_ref(col_ref: str, branch: Scope) -> str:
        """
        Qualify an unqualified column ref with its source table from the branch.
        If col_ref already has a dot (table.col), return as-is.
        Otherwise, find the source table from branch.relations.
        """
        col_ref = col_ref.strip()

        # FIX 23: Don't qualify non-column expressions (functions, subqueries)
        # If ref contains spaces, parentheses, or newlines, it's not a simple column ref
        if any(c in col_ref for c in ' ()\n\r\t'):
            return col_ref  # Return as-is, let resolve_expression handle it

        # Already qualified with table
        if '.' in col_ref:
            return col_ref

        # Try to find the source table from branch's relations
        for alias, rel in branch.relations.items():
            if isinstance(rel, str):
                # rel is a physical table name - qualify with it
                return f"{rel}.{col_ref}"
            elif isinstance(rel, Scope):
                # rel is a subquery - check if column exists in its projections
                if col_ref.upper() in rel.projections:
                    return f"{alias}.{col_ref}"

        # Fallback: return unqualified (resolution will handle later)
        return col_ref

    # FIX 20: Merge UNION branch projections by position
    def _merge_union_projections(union_scope: Scope) -> None:
        """
        Merge lineage across UNION branches by column position.
        - Column names follow the first branch (SQL behavior).
        - Lineage (source_refs) is the union of all branches for each position.
        - FIX 21: Each source is qualified with its branch's source table.
        """
        if not union_scope.union_branches:
            return

        first = union_scope.union_branches[0]
        merged: Dict[str, ProjectionDef] = {}

        # Build index -> name map from first branch
        first_items = list(first.projections.items())

        for idx, (col_name, base_proj) in enumerate(first_items):
            combined_sources: List[str] = []

            # FIX 21: Qualify first branch's source refs with their source table
            first_sources = base_proj.source_refs or [base_proj.expression]
            for src in first_sources:
                qualified = _qualify_column_ref(src, first)
                if qualified not in combined_sources:
                    combined_sources.append(qualified)

            # Walk other branches and merge lineage at same position
            for branch in union_scope.union_branches[1:]:
                branch_items = list(branch.projections.items())
                if idx < len(branch_items):
                    _, branch_proj = branch_items[idx]
                    # FIX 21: Qualify this branch's source refs with their source table
                    branch_sources = branch_proj.source_refs or [branch_proj.expression]
                    for src in branch_sources:
                        qualified = _qualify_column_ref(src, branch)
                        if qualified not in combined_sources:
                            combined_sources.append(qualified)

            # Create merged projection (name from first branch, all sources accumulated)
            merged[col_name.upper()] = ProjectionDef(
                output_name=col_name,
                expression=base_proj.expression,
                source_refs=combined_sources,
                origin_alias=base_proj.origin_alias
            )

        union_scope.projections = merged

    # Handle UNION
    if isinstance(ast, exp.Union):
        if ast.left:
            scope.union_branches.append(build_scope_tree(ast.left, f"{name}__UNION1", scope, dm_dict))
        if ast.right:
            scope.union_branches.append(build_scope_tree(ast.right, f"{name}__UNION2", scope, dm_dict))
        # FIX 20: Merge projections from ALL branches (not just first)
        if scope.union_branches:
            _merge_union_projections(scope)

            # FIX 17A: Make branch aliases visible at union scope level
            # This allows find_relation_in_scope_chain to find aliases like STG_CARDS
            # that are defined inside union branches
            for branch in scope.union_branches:
                for alias, rel in branch.relations.items():
                    if alias not in scope.relations:  # First-branch precedence
                        scope.relations[alias] = rel
        return scope

    # Find SELECT clause
    select_expr = ast if isinstance(ast, exp.Select) else ast.find(exp.Select)
    if not select_expr:
        return scope

    # PASS 1: Register FROM sources
    from_clause = select_expr.find(exp.From)
    if from_clause:
        _register_from_sources(from_clause, scope, name, dm_dict)

    # PASS 2: Register JOIN sources (only direct JOINs, not from nested subqueries)
    for join in select_expr.find_all(exp.Join):
        # Skip JOINs that are inside a nested subquery
        # Check if there's a Subquery between this JOIN and our SELECT
        join_parent = join.parent
        is_nested = False
        while join_parent and join_parent != select_expr:
            if isinstance(join_parent, exp.Subquery):
                is_nested = True
                break
            join_parent = join_parent.parent
        if not is_nested:
            _register_join_source(join, scope, name, dm_dict)

    # PASS 2b: Register UNPIVOT-generated columns
    # UNPIVOT creates virtual columns that need to be tracked for lineage
    _register_unpivot_columns(select_expr, scope)

    # PASS 2c: Extract joins for THIS scope (not nested subqueries)
    # sql_file will be updated later when collecting joins in process_sql_file()
    scope.joins = _extract_scope_joins(select_expr, scope, object_name="", sql_file="")

    # FIX 22: Register tables from EXISTS/IN subqueries in WHERE/HAVING clauses
    # These need to be registered so that references like setup_master.column resolve
    _register_exists_subquery_tables(select_expr, scope, name, dm_dict)

    # PASS 3: Register SELECT projections
    for item in select_expr.expressions:
        # Handle SELECT * - expand from all visible relations
        if isinstance(item, exp.Star):
            _expand_star_projection(scope, dm_dict)
            continue

        # Handle SELECT alias.* - expand from specific relation
        if isinstance(item, exp.Column) and isinstance(item.this, exp.Star):
            alias = item.table.upper() if item.table else None
            if alias:
                # Try direct relations first, then search nested scopes
                if alias in scope.relations or _find_scope_by_alias(scope, alias):
                    _expand_qualified_star(scope, alias, dm_dict)
            continue

        output_name = _get_projection_alias(item)
        if output_name:
            # Extract the actual expression (without AS alias part)
            if isinstance(item, exp.Alias):
                expr_node = item.this
            else:
                expr_node = item
            expr_sql = expr_node.sql(dialect="oracle")

            # Extract origin_alias from qualified column expression
            # This helps identity case prioritize the right source when descending through scopes
            refs = extract_column_refs(expr_sql)
            origin_alias = None
            if len(refs) == 1:
                ref_alias, ref_col = parse_ref(refs[0])
                if ref_alias:
                    origin_alias = ref_alias  # e.g., "SLC" for SLC.FIC_MIS_DATE

            scope.projections[output_name.upper()] = ProjectionDef(
                output_name=output_name,
                expression=expr_sql,
                source_refs=refs,
                origin_alias=origin_alias
            )
            logger.debug(f"[PROJ_REG] {scope.name}: {output_name.upper()} = {expr_sql[:60]}...")

    logger.debug(f"[SCOPE_BUILT] {scope.name} with {len(scope.projections)} projections, {len(scope.relations)} relations")
    return scope


def _expand_star_projection(scope: Scope, dm_dict: Optional[Dict[str, Set[str]]] = None) -> None:
    """
    Expand SELECT * by collecting projections from all visible relations.
    For CTEs/subqueries: copy their projection definitions
    For physical tables: expand ALL columns from DM if available
    """
    # Check relations (FROM/JOIN sources)
    for alias, relation in scope.relations.items():
        if isinstance(relation, Scope):
            # CTE or subquery - copy inner projection's expression directly
            # This avoids creating ALIAS.COLUMN refs that cause cycle detection issues
            for proj_name, proj_def in relation.projections.items():
                scope.projections[proj_name.upper()] = ProjectionDef(
                    output_name=proj_name,
                    expression=proj_def.expression,
                    source_refs=proj_def.source_refs.copy() if proj_def.source_refs else [proj_def.expression],
                    origin_alias=alias  # Track which alias.* this came from
                )
        elif isinstance(relation, str):
            # Physical table - expand from DM if available
            table_name = relation.upper()
            if dm_dict and table_name in dm_dict:
                # Expand to ALL columns from DM
                for col_name in dm_dict[table_name]:
                    qualified_expr = f"{alias}.{col_name}"
                    scope.projections[col_name.upper()] = ProjectionDef(
                        output_name=col_name,
                        expression=qualified_expr,
                        source_refs=[qualified_expr]
                    )
            else:
                # Table not in DM - create placeholder
                scope.projections[f"*_{alias}"] = ProjectionDef(
                    output_name="*",
                    expression=f"{alias}.*",
                    source_refs=[f"{alias}.*"]
                )


def _find_scope_by_alias(scope: Scope, alias: str) -> Optional[Scope]:
    """
    Find a scope by alias, searching in direct relations first, then recursively in nested scopes.
    This handles cases where SQL references a nested scope (e.g., interia1.* when interia1 is 2 levels deep).
    """
    # First check direct relations
    if alias in scope.relations:
        rel = scope.relations[alias]
        if isinstance(rel, Scope):
            return rel
        return None  # Physical table, not a scope

    # Search in nested scopes (depth-first)
    for rel_alias, relation in scope.relations.items():
        if isinstance(relation, Scope):
            found = _find_scope_by_alias(relation, alias)
            if found:
                return found

    return None


def _expand_qualified_star(scope: Scope, alias: str, dm_dict: Optional[Dict[str, Set[str]]] = None) -> None:
    """
    Expand SELECT alias.* by collecting projections from a specific relation.
    For CTEs/subqueries: copy their projection definitions
    For physical tables: expand ALL columns from DM if available
    Also searches nested scopes if alias not found directly.
    """
    logger.debug(f"[STAR_EXPAND] Called for alias={alias} in scope={scope.name}")

    # First try direct relations, then search nested scopes
    if alias in scope.relations:
        relation = scope.relations[alias]
        logger.debug(f"[STAR_EXPAND] Found {alias} in direct relations")
    else:
        # Search in nested scopes (handles interia1.* when interia1 is deeply nested)
        relation = _find_scope_by_alias(scope, alias)
        if relation is None:
            logger.debug(f"[STAR_EXPAND] Alias {alias} NOT FOUND anywhere!")
            return  # Alias not found anywhere
        logger.debug(f"[STAR_EXPAND] Found {alias} in nested scopes")

    if isinstance(relation, Scope):
        # CTE or subquery - copy inner projection's expression directly
        # This avoids creating ALIAS.COLUMN refs that cause cycle detection issues
        logger.debug(f"[STAR_EXPAND] {alias} is Scope with {len(relation.projections)} projections")
        for pname in list(relation.projections.keys())[:10]:
            expr = relation.projections[pname].expression[:60] if relation.projections[pname].expression else "None"
            logger.debug(f"[STAR_EXPAND]   - {pname}: {expr}")
        if len(relation.projections) > 10:
            logger.debug(f"[STAR_EXPAND]   ... and {len(relation.projections) - 10} more")
        for proj_name, proj_def in relation.projections.items():
            scope.projections[proj_name.upper()] = ProjectionDef(
                output_name=proj_name,
                expression=proj_def.expression,
                source_refs=proj_def.source_refs.copy() if proj_def.source_refs else [proj_def.expression],
                origin_alias=alias  # Track which alias.* this came from
            )
    elif isinstance(relation, str):
        # Physical table - expand from DM if available
        table_name = relation.upper()
        logger.debug(f"[STAR_EXPAND] {alias} is physical table: {table_name}")
        if dm_dict and table_name in dm_dict:
            # Expand to ALL columns from DM
            for col_name in dm_dict[table_name]:
                qualified_expr = f"{alias}.{col_name}"
                scope.projections[col_name.upper()] = ProjectionDef(
                    output_name=col_name,
                    expression=qualified_expr,
                    source_refs=[qualified_expr]
                )
        else:
            # Table not in DM - create placeholder indicating star expansion
            scope.projections[f"*_{alias}"] = ProjectionDef(
                output_name="*",
                expression=f"{alias}.*",
                source_refs=[f"{alias}.*"]
            )


def _find_cte_in_scope_chain(cte_name: str, scope: Scope) -> Optional[Scope]:
    """
    Find a CTE by name, walking up the scope chain.
    CTEs are registered in the scope where WITH clause appears (usually ROOT).
    Child scopes need to walk up to find sibling CTEs.
    """
    current = scope
    while current:
        if cte_name in current.ctes and current.ctes[cte_name] is not None:
            return current.ctes[cte_name]
        current = current.parent
    return None


def _register_from_sources(from_clause: exp.From, scope: Scope, name: str,
                          dm_dict: Optional[Dict[str, Set[str]]] = None) -> None:
    """Register sources from FROM clause."""
    # Direct table in FROM
    if from_clause.this:
        source = from_clause.this
        if isinstance(source, exp.Table):
            table_name = source.name.upper() if source.name else ""
            alias = (source.alias or source.name)
            if alias:
                alias_upper = alias.upper()
                # Check if this table name refers to a known CTE (walk up scope chain)
                cte_scope = _find_cte_in_scope_chain(table_name, scope)
                if cte_scope is not None:
                    scope.relations[alias_upper] = cte_scope
                else:
                    scope.relations[alias_upper] = table_name
        elif isinstance(source, exp.Subquery):
            alias = source.alias
            if alias:
                child = build_scope_tree(source.this, f"{name}/{alias.upper()}", scope, dm_dict)
                scope.relations[alias.upper()] = child
            else:
                # Anonymous subquery (no alias) - still need to build and link it
                # Use synthetic name for the scope, register as __ANON__ so resolution can find it
                child = build_scope_tree(source.this, f"{name}/__ANON__", scope, dm_dict)
                scope.relations["__ANON__"] = child
                logger.debug(f"Registered anonymous subquery in FROM clause for scope {name}")


def _register_join_source(join: exp.Join, scope: Scope, name: str,
                         dm_dict: Optional[Dict[str, Set[str]]] = None) -> None:
    """Register source from JOIN clause."""
    source = join.this
    if isinstance(source, exp.Table):
        table_name = source.name.upper() if source.name else ""
        alias = source.alias or source.name
        if alias:
            alias_upper = alias.upper()
            # Check if this table name refers to a known CTE (walk up scope chain)
            cte_scope = _find_cte_in_scope_chain(table_name, scope)
            if cte_scope is not None:
                scope.relations[alias_upper] = cte_scope
            else:
                scope.relations[alias_upper] = table_name
    elif isinstance(source, exp.Subquery):
        alias = source.alias
        if alias:
            child = build_scope_tree(source.this, f"{name}/{alias.upper()}", scope, dm_dict)
            scope.relations[alias.upper()] = child
        else:
            # Parenthesized join without alias: (table1 JOIN table2 ON ...)
            # Extract all tables from within and register them directly
            _register_parenthesized_join_tables(source, scope, name, dm_dict)


def _register_parenthesized_join_tables(source: exp.Subquery, scope: Scope, name: str,
                                        dm_dict: Optional[Dict[str, Set[str]]] = None) -> None:
    """
    Register tables from a parenthesized join expression.

    Handles patterns like:
    LEFT OUTER JOIN (stg_abc INNER JOIN audit_control ON ...) ON ...

    The tables inside the parentheses need to be registered in the current scope.
    """
    # Find all tables within the parenthesized expression
    for table in source.find_all(exp.Table):
        table_name = table.name.upper() if table.name else ""
        alias = table.alias or table.name
        if alias and table_name:
            alias_upper = alias.upper()
            # Check if this table name refers to a known CTE
            cte_scope = _find_cte_in_scope_chain(table_name, scope)
            if cte_scope is not None:
                scope.relations[alias_upper] = cte_scope
            else:
                scope.relations[alias_upper] = table_name
            logger.debug(f"Registered table from parenthesized join: {alias_upper} -> {table_name}")


def _register_exists_subquery_tables(select_expr: exp.Select, scope: Scope, name: str,
                                      dm_dict: Optional[Dict[str, Set[str]]] = None) -> None:
    """
    FIX 22: Register tables from EXISTS/IN subqueries in WHERE/HAVING clauses.

    When SQL has patterns like:
        WHERE EXISTS (SELECT 1 FROM setup_master WHERE setup_master.column = ...)

    The table 'setup_master' needs to be registered as a relation so that
    setup_master.column references can resolve.

    For unaliased tables (FROM table_name), register table_name as both the
    physical table reference AND as an alias (self-aliased).
    """
    # Find all EXISTS nodes in WHERE/HAVING clauses
    for exists_node in select_expr.find_all(exp.Exists):
        # Check if this EXISTS is nested inside another subquery (skip if so)
        parent = exists_node.parent
        is_nested_in_subquery = False
        while parent and parent != select_expr:
            if isinstance(parent, exp.Subquery):
                is_nested_in_subquery = True
                break
            parent = parent.parent

        if is_nested_in_subquery:
            continue  # This EXISTS belongs to a child scope

        # Get the subquery inside EXISTS
        subquery = exists_node.this
        if not subquery:
            continue

        # Find the SELECT inside the EXISTS
        inner_select = subquery if isinstance(subquery, exp.Select) else subquery.find(exp.Select)
        if not inner_select:
            continue

        # Find tables in the EXISTS subquery's FROM clause
        from_clause = inner_select.find(exp.From)
        if from_clause and from_clause.this:
            source = from_clause.this
            if isinstance(source, exp.Table):
                table_name = source.name.upper() if source.name else ""
                # For unaliased tables, use table name as alias (self-aliased)
                alias = (source.alias or source.name)
                if alias and table_name:
                    alias_upper = alias.upper()
                    if alias_upper not in scope.relations:
                        # Check if this refers to a CTE
                        cte_scope = _find_cte_in_scope_chain(table_name, scope)
                        if cte_scope is not None:
                            scope.relations[alias_upper] = cte_scope
                        else:
                            scope.relations[alias_upper] = table_name
                        logger.debug(f"FIX 22: Registered table from EXISTS subquery: {alias_upper} -> {table_name}")

        # Also handle JOINs inside the EXISTS subquery
        for join in inner_select.find_all(exp.Join):
            join_source = join.this
            if isinstance(join_source, exp.Table):
                table_name = join_source.name.upper() if join_source.name else ""
                alias = (join_source.alias or join_source.name)
                if alias and table_name:
                    alias_upper = alias.upper()
                    if alias_upper not in scope.relations:
                        cte_scope = _find_cte_in_scope_chain(table_name, scope)
                        if cte_scope is not None:
                            scope.relations[alias_upper] = cte_scope
                        else:
                            scope.relations[alias_upper] = table_name
                        logger.debug(f"FIX 22: Registered table from EXISTS JOIN: {alias_upper} -> {table_name}")


def _register_unpivot_columns(select_expr: exp.Select, scope: Scope) -> None:
    """
    Register columns generated by UNPIVOT operations.

    UNPIVOT (value_col FOR for_col IN (col_a, col_b, col_c)) creates:
    - value_col: contains values from col_a, col_b, col_c
    - for_col: contains 'COL_A', 'COL_B', 'COL_C' as string literals

    These are virtual columns that need to be registered for lineage tracking.
    """
    # Find all Pivot nodes with unpivot=True
    for pivot in select_expr.find_all(exp.Pivot):
        if not getattr(pivot, 'unpivot', False):
            continue  # Skip regular PIVOT, only process UNPIVOT

        # Extract value column(s) from expressions
        # UNPIVOT (amount FOR type IN (...)) -> amount is the value column
        for expr in pivot.expressions:
            if isinstance(expr, exp.Column):
                col_name = expr.name.upper() if expr.name else ""
                if col_name:
                    # Register as CONSTANT since it's derived from multiple source columns
                    scope.projections[col_name] = ProjectionDef(
                        output_name=col_name,
                        expression=f"UNPIVOT_VALUE({col_name})",
                        source_refs=[]
                    )
                    logger.debug(f"Registered UNPIVOT value column: {col_name}")

        # Extract FOR column from fields
        # UNPIVOT (amount FOR type_solde IN (...)) -> type_solde is the FOR column
        for field in getattr(pivot, 'fields', []) or []:
            if isinstance(field, exp.In):
                for_col = field.this
                if isinstance(for_col, exp.Column):
                    for_name = for_col.name.upper() if for_col.name else ""
                    if for_name:
                        # The FOR column contains literal values (column names as strings)
                        # Register as CONSTANT since values are derived
                        scope.projections[for_name] = ProjectionDef(
                            output_name=for_name,
                            expression=f"UNPIVOT_FOR({for_name})",
                            source_refs=[]
                        )
                        logger.debug(f"Registered UNPIVOT FOR column: {for_name}")


def _get_projection_alias(item: exp.Expression) -> str:
    """Get output alias for a SELECT item."""
    if isinstance(item, exp.Alias):
        return item.alias
    elif hasattr(item, 'alias') and item.alias:
        return item.alias
    elif isinstance(item, exp.Column):
        return item.name
    else:
        # Generate from expression
        return item.sql(dialect="oracle")[:30]


def find_relation_in_scope_chain(alias: str, scope: Scope) -> Optional[Union[str, Scope]]:
    """Find relation by alias, walking up scope chain and searching nested scopes."""
    alias = alias.upper()

    # Check current scope
    if alias in scope.relations:
        return scope.relations[alias]

    # Check CTEs
    if alias in scope.ctes:
        return scope.ctes[alias]

    # Search nested scopes for SCOPE alias (handles cases like interia1.* when interia1 is deeply nested)
    nested = _find_scope_by_alias(scope, alias)
    if nested:
        return nested

    # Search nested scopes for PHYSICAL TABLE alias
    # This handles cases where star expansion copied expressions with inner-scope table aliases
    # e.g., outer scope has projection "SLC.FIC_MIS_DATE" but SLC is only defined in inner scope
    def find_physical_in_nested(s: Scope, tbl_alias: str) -> Optional[str]:
        """Recursively search for physical table alias in nested scopes."""
        for rel_alias, rel in s.relations.items():
            if rel_alias == tbl_alias and isinstance(rel, str):
                return rel  # Found physical table
            if isinstance(rel, Scope):
                found = find_physical_in_nested(rel, tbl_alias)
                if found:
                    return found
        return None

    physical = find_physical_in_nested(scope, alias)
    if physical:
        return physical

    # FIX 17B: Search union branches for alias
    # When resolving STG_CARDS.X in a UNION scope, the alias STG_CARDS
    # lives inside union_branches, not in scope.relations directly
    for branch in scope.union_branches:
        if alias in branch.relations:
            return branch.relations[alias]
        if alias in branch.ctes:
            return branch.ctes[alias]
        # Also search nested within each branch
        nested = _find_scope_by_alias(branch, alias)
        if nested:
            return nested
        physical = find_physical_in_nested(branch, alias)
        if physical:
            return physical

    # Walk up to parent
    if scope.parent:
        return find_relation_in_scope_chain(alias, scope.parent)

    return None


def _generate_debug_info(
    column: str,
    scope: Scope,
    reason: str,
    searched_alias: str = ""
) -> str:
    """
    Generate detailed debug info for UNRESOLVED columns.
    This helps diagnose why resolution failed.
    """
    lines = []
    lines.append(f"DEBUG: Failed to resolve '{column}'")
    lines.append(f"Reason: {reason}")
    lines.append(f"Current scope: {scope.name}")

    if searched_alias:
        lines.append(f"Searched for alias: {searched_alias}")

    # Show available relations in current scope
    if scope.relations:
        rel_info = []
        for alias, rel in scope.relations.items():
            if isinstance(rel, Scope):
                rel_info.append(f"{alias}(scope:{len(rel.projections)}cols)")
            else:
                rel_info.append(f"{alias}={rel}")
        lines.append(f"Available relations: {', '.join(rel_info[:10])}")
    else:
        lines.append("Available relations: NONE")

    # Show available projections (first 10)
    if scope.projections:
        proj_names = list(scope.projections.keys())[:10]
        lines.append(f"Available projections ({len(scope.projections)} total): {', '.join(proj_names)}")
        if column.upper() not in scope.projections:
            # Show similar names that might help
            similar = [p for p in scope.projections.keys() if column.upper()[:4] in p][:5]
            if similar:
                lines.append(f"Similar projections: {', '.join(similar)}")
    else:
        lines.append("Available projections: NONE")

    # If there's a searched alias, show what's in that scope
    if searched_alias and searched_alias in scope.relations:
        rel = scope.relations[searched_alias]
        if isinstance(rel, Scope):
            lines.append(f"--- In {searched_alias} scope ---")
            lines.append(f"Projections ({len(rel.projections)}): {', '.join(list(rel.projections.keys())[:10])}")
            if column.upper() not in rel.projections:
                similar = [p for p in rel.projections.keys() if column.upper()[:4] in p][:5]
                if similar:
                    lines.append(f"Similar in {searched_alias}: {', '.join(similar)}")

    return " | ".join(lines)


def resolve_to_physical(
    ref: str,
    scope: Scope,
    dm_dict: Dict[str, Set[str]],
    visited: Optional[Set[str]] = None,
    trace: Optional[List[str]] = None,
    max_depth: int = 50
) -> List[ResolvedColumn]:
    """
    Core resolution algorithm: resolve column reference to physical source(s).
    """
    if visited is None:
        visited = set()
    if trace is None:
        trace = [scope.name]

    ref = normalize_whitespace(ref).upper()

    # GUARD 1: Cycle detection using (scope_id, column) to avoid false positives
    # Use scope object id instead of name to distinguish same-named scopes
    cycle_key = f"{id(scope)}:{ref}"
    if cycle_key in visited:
        debug_info = f"DEBUG: Cycle detected for '{ref}' in scope '{scope.name}'"
        return [ResolvedColumn(
            source_type=SourceType.UNRESOLVED,
            reason=f"{UnresolvedReason.CYCLE_DETECTED.value} | {debug_info}",
            trace_path="->".join(trace)
        )]
    visited.add(cycle_key)

    # GUARD 2: Depth limit
    if len(trace) > max_depth:
        return [ResolvedColumn(
            source_type=SourceType.UNRESOLVED,
            reason=UnresolvedReason.DEPTH_GUARD.value,
            trace_path="->".join(trace)
        )]

    # STEP 1: Constant check
    if is_constant(ref):
        return [ResolvedColumn(
            source_type=SourceType.CONSTANT,
            constant_value=ref,
            trace_path="->".join(trace) + f":CONSTANT({ref})"
        )]

    # STEP 2: Parse reference
    alias, column = parse_ref(ref)

    # STEP 3: Qualified reference (ALIAS.COLUMN)
    if alias:
        relation = find_relation_in_scope_chain(alias, scope)

        if relation is None:
            debug_info = _generate_debug_info(ref, scope, "ALIAS_NOT_FOUND", alias)
            return [ResolvedColumn(
                source_type=SourceType.UNRESOLVED,
                reason=f"{UnresolvedReason.ALIAS_NOT_FOUND.value} | {debug_info}",
                trace_path="->".join(trace),
                column=ref
            )]

        # Physical table
        if isinstance(relation, str):
            dm_match = "Y" if column_exists_in_dm(relation, column, dm_dict) else "N"
            return [ResolvedColumn(
                source_type=SourceType.PHYSICAL,
                table=relation,
                column=column,
                dm_match=dm_match,
                trace_path="->".join(trace) + f":{relation}.{column}",
                # FIX 24: Capture original alias and reference
                source_alias=alias,
                original_ref=ref
            )]

        # Subquery scope
        if isinstance(relation, Scope):
            # SELF-REFERENCE CHECK: Only trigger when relation IS the same scope object
            # (e.g., resolving ALIAS.COL where ALIAS points to the current scope we're in)
            # Don't trigger for normal parent→child alias mapping (that's valid subquery descent)
            is_self_reference = (relation is scope)

            if is_self_reference:
                # Look for physical table with same name inside current scope or its nested scopes
                def find_physical_table_in_tree(s: Scope, tbl_alias: str) -> Optional[str]:
                    """Recursively search for physical table by alias in scope and nested scopes."""
                    if tbl_alias in s.relations:
                        rel = s.relations[tbl_alias]
                        if isinstance(rel, str):
                            return rel
                    # Search nested scopes
                    for _, rel in s.relations.items():
                        if isinstance(rel, Scope):
                            found = find_physical_table_in_tree(rel, tbl_alias)
                            if found:
                                return found
                    return None

                physical_table = find_physical_table_in_tree(scope, alias.upper())
                if physical_table:
                    # Found physical table - resolve to it
                    dm_match = "Y" if column_exists_in_dm(physical_table, column, dm_dict) else "N"
                    return [ResolvedColumn(
                        source_type=SourceType.PHYSICAL,
                        table=physical_table,
                        column=column,
                        dm_match=dm_match,
                        trace_path="->".join(trace) + f":{physical_table}.{column}"
                    )]
                # FIX: For self-referential subquery (no physical table), resolve the unqualified
                # column directly in the current scope instead of falling through to relation.projections
                # which would cause false CYCLE_DETECTED (since relation IS scope)
                logger.debug(f"Self-reference detected for alias {alias}, resolving {column} directly in scope")
                if column.upper() in scope.projections:
                    proj = scope.projections[column.upper()]
                    return resolve_expression(
                        proj.expression,
                        scope,
                        dm_dict,
                        visited.copy(),  # Fresh visited set to avoid false cycle
                        trace,
                        max_depth
                    )
                # Column not found in projections - return MISSING_PROJECTION
                debug_info = _generate_debug_info(column, scope, "MISSING_PROJECTION (self-ref)", alias)
                return [ResolvedColumn(
                    source_type=SourceType.UNRESOLVED,
                    reason=f"{UnresolvedReason.MISSING_PROJECTION.value} | {debug_info}",
                    trace_path="->".join(trace),
                    column=f"{alias}.{column}"
                )]

            # All subquery scopes get fresh visited set to avoid false cycle detection
            # (not just CTEs - any child scope should reset to avoid ping-pong cycles)
            visited = set()

            # Handle UNION scopes - resolve by name, then by position (FIX 17D)
            # SQL UNION aligns columns by POSITION, not name. First branch defines output names.
            if relation.union_branches:
                all_results: List[ResolvedColumn] = []

                # Get first branch (defines output column names at UNION level)
                first = relation.union_branches[0]
                first_names = list(first.projections.keys())  # Insertion order preserved (Python 3.7+)

                # Determine the target index from the first branch
                target_idx: Optional[int] = None
                try:
                    target_idx = first_names.index(column.upper())
                except ValueError:
                    pass  # Name not found in first branch

                for branch in relation.union_branches:
                    if column.upper() in branch.projections:
                        # Name match - use directly
                        proj = branch.projections[column.upper()]
                        branch_results = resolve_expression(
                            proj.expression,
                            branch,
                            dm_dict,
                            visited.copy(),
                            trace + [branch.name],
                            max_depth
                        )
                        all_results.extend(branch_results)
                    elif target_idx is not None:
                        # FIX 17D: Position fallback - get N-th projection from this branch
                        branch_names = list(branch.projections.keys())
                        if target_idx < len(branch_names):
                            positional_col = branch_names[target_idx]
                            proj = branch.projections[positional_col]
                            branch_results = resolve_expression(
                                proj.expression,
                                branch,
                                dm_dict,
                                visited.copy(),
                                trace + [branch.name],
                                max_depth
                            )
                            all_results.extend(branch_results)

                if all_results:
                    return all_results
                # Fall through to MISSING_PROJECTION if no results

            # Regular subquery (non-UNION)
            elif column.upper() in relation.projections:
                proj = relation.projections[column.upper()]
                # Recurse into subquery's expression
                return resolve_expression(
                    proj.expression,
                    relation,
                    dm_dict,
                    visited.copy(),
                    trace + [relation.name],
                    max_depth
                )

            # Column not found in scope projections - try searching nested scopes
            # This handles cases where star expansion didn't propagate all columns
            # (Removed misuse of _find_scope_by_alias with column name - it expects alias)
            def find_column_in_nested(s: Scope, col: str) -> Optional[Scope]:
                if col.upper() in s.projections:
                    return s
                for alias, rel in s.relations.items():
                    if isinstance(rel, Scope):
                        found = find_column_in_nested(rel, col)
                        if found:
                            return found
                # FIX 17C: Also search union branches
                for branch in s.union_branches:
                    found = find_column_in_nested(branch, col)
                    if found:
                        return found
                return None

            nested_with_col = find_column_in_nested(relation, column)
            if nested_with_col:
                proj = nested_with_col.projections[column.upper()]
                return resolve_expression(
                    proj.expression,
                    nested_with_col,
                    dm_dict,
                    visited.copy(),
                    trace + [nested_with_col.name],
                    max_depth
                )

            # Column truly not found
            debug_info = _generate_debug_info(column, relation if isinstance(relation, Scope) else scope, "MISSING_PROJECTION", alias)
            return [ResolvedColumn(
                source_type=SourceType.UNRESOLVED,
                reason=f"{UnresolvedReason.MISSING_PROJECTION.value} | {debug_info}",
                trace_path="->".join(trace),
                column=f"{alias}.{column}"
            )]

    # STEP 4: Unqualified column
    else:
        # Handle UNION scopes for unqualified columns (top-level UNION)
        # FIX 17D: Use position-based matching when names differ across branches
        if scope.union_branches:
            all_results: List[ResolvedColumn] = []

            # Get first branch (defines output column names at UNION level)
            first = scope.union_branches[0]
            first_names = list(first.projections.keys())  # Insertion order preserved

            # Determine the target index from the first branch
            target_idx: Optional[int] = None
            try:
                target_idx = first_names.index(column.upper())
            except ValueError:
                pass  # Name not found in first branch

            for branch in scope.union_branches:
                # First try projections by name
                if column.upper() in branch.projections:
                    proj = branch.projections[column.upper()]
                    branch_results = resolve_expression(
                        proj.expression,
                        branch,
                        dm_dict,
                        visited.copy(),
                        trace + [branch.name],
                        max_depth
                    )
                    all_results.extend(branch_results)
                elif target_idx is not None:
                    # FIX 17D: Position fallback - get N-th projection from this branch
                    branch_names = list(branch.projections.keys())
                    if target_idx < len(branch_names):
                        positional_col = branch_names[target_idx]
                        proj = branch.projections[positional_col]
                        branch_results = resolve_expression(
                            proj.expression,
                            branch,
                            dm_dict,
                            visited.copy(),
                            trace + [branch.name],
                            max_depth
                        )
                        all_results.extend(branch_results)
                else:
                    # Column not in projections - try resolving directly in branch
                    # This handles source columns (e.g., CREDIT_LIMIT) vs projected names (e.g., AMOUNT)
                    branch_results = resolve_to_physical(
                        column,
                        branch,
                        dm_dict,
                        visited.copy(),
                        trace + [branch.name],
                        max_depth
                    )
                    # Only add if resolved (not UNRESOLVED)
                    for r in branch_results:
                        if r.source_type != SourceType.UNRESOLVED:
                            all_results.append(r)
            if all_results:
                return all_results

        # First check projections in current scope
        if column.upper() in scope.projections:
            proj = scope.projections[column.upper()]
            # Prevent identity loop - check both unqualified AND qualified identity
            # e.g., D_END_OF_PERIOD → D_END_OF_PERIOD (unqualified)
            # e.g., D_END_OF_PERIOD → ALIAS.D_END_OF_PERIOD (qualified identity)
            expr_refs = extract_column_refs(proj.expression)
            is_identity = False
            if normalize_identifier(proj.expression) == column.upper():
                is_identity = True  # Unqualified identity
            elif len(expr_refs) == 1:
                # Check if it's a qualified reference to the same column
                ref_alias, ref_col = parse_ref(expr_refs[0])
                if ref_col.upper() == column.upper():
                    # FIX 17E: Only treat as identity if the alias is NOT a relation in current scope
                    # If alias IS a relation (child scope), resolve normally to get all UNION branches
                    if ref_alias and ref_alias.upper() in scope.relations:
                        is_identity = False  # Not identity - it's a reference to a child scope
                    else:
                        is_identity = True  # Qualified identity (e.g., ALIAS.SAME_COL from parent)

            if not is_identity:
                return resolve_expression(
                    proj.expression,
                    scope,
                    dm_dict,
                    visited.copy(),
                    trace,
                    max_depth
                )
            else:
                # IDENTITY CASE: Prefer child scope (origin_alias) FIRST, then parent physical tables
                # This prevents wrong mapping when parent has same-named column from different table
                phys_candidates: List[ResolvedColumn] = []

                # Get the origin alias if this projection came from alias.* expansion
                origin = proj.origin_alias.upper() if proj.origin_alias else None

                # Build ordered list of child scopes to check (origin first)
                child_scopes_to_check: List[tuple] = []
                for alias_name, relation in scope.relations.items():
                    if isinstance(relation, Scope):
                        if origin and alias_name.upper() == origin:
                            child_scopes_to_check.insert(0, (alias_name, relation))  # Origin at front
                        else:
                            child_scopes_to_check.append((alias_name, relation))

                # STEP 1: Check child scopes (origin first) for the true physical source
                # Use recursive helper to traverse multiple nested layers
                def trace_to_physical(current_scope: Scope, col: str, path: List[str], depth: int = 0) -> List[ResolvedColumn]:
                    """Recursively trace through nested scopes to find physical source."""
                    if depth > 10:  # Prevent infinite loops
                        return []
                    results: List[ResolvedColumn] = []

                    # If column not in projections, search nested scopes
                    if col.upper() not in current_scope.projections:
                        # Search child scopes for the column
                        for child_alias, child_rel in current_scope.relations.items():
                            if isinstance(child_rel, Scope) and col.upper() in child_rel.projections:
                                deeper = trace_to_physical(child_rel, col, path + [child_rel.name], depth + 1)
                                if deeper:
                                    return deeper
                        return []  # Not found in nested scopes either

                    proj = current_scope.projections[col.upper()]
                    # FIX 21: Use source_refs when populated (from UNION merge) instead of expression
                    refs = proj.source_refs if proj.source_refs else extract_column_refs(proj.expression)

                    # FIX 16: Check if expression is a constant (no column refs)
                    if not refs:
                        expr_stripped = proj.expression.strip()
                        if is_constant(expr_stripped):
                            return [ResolvedColumn(
                                source_type=SourceType.CONSTANT,
                                constant_value=expr_stripped,
                                trace_path="->".join(path) + f":CONSTANT({expr_stripped})"
                            )]
                        # If not a constant and no refs, fall through (will return empty results)

                    for r in refs:
                        ref_alias, ref_col = parse_ref(r)
                        if ref_alias:
                            rel = find_relation_in_scope_chain(ref_alias, current_scope)
                            if isinstance(rel, str):  # Physical table found!
                                dm = "Y" if column_exists_in_dm(rel, ref_col, dm_dict) else "N"
                                results.append(ResolvedColumn(
                                    source_type=SourceType.PHYSICAL,
                                    table=rel,
                                    column=ref_col,
                                    dm_match=dm,
                                    trace_path="->".join(path) + f":{rel}.{ref_col}",
                                    # FIX 24: Capture original alias and reference
                                    source_alias=ref_alias,
                                    original_ref=r
                                ))
                            elif isinstance(rel, Scope):  # Another nested scope - drill deeper
                                deeper = trace_to_physical(rel, ref_col, path + [rel.name], depth + 1)
                                results.extend(deeper)
                        else:
                            # Unqualified ref - PRIORITIZE child scopes over physical tables
                            scope_results: List[ResolvedColumn] = []
                            physical_results: List[ResolvedColumn] = []
                            for child_alias, child_rel in current_scope.relations.items():
                                if isinstance(child_rel, str):
                                    dm = "Y" if column_exists_in_dm(child_rel, ref_col, dm_dict) else "N"
                                    physical_results.append(ResolvedColumn(
                                        source_type=SourceType.PHYSICAL,
                                        table=child_rel,
                                        column=ref_col,
                                        dm_match=dm,
                                        trace_path="->".join(path) + f":{child_rel}.{ref_col}",
                                        # FIX 24: Capture alias (inferred) and reference
                                        source_alias=child_alias,
                                        original_ref=ref_col  # Unqualified - just column name
                                    ))
                                elif isinstance(child_rel, Scope) and ref_col.upper() in child_rel.projections:
                                    deeper = trace_to_physical(child_rel, ref_col, path + [child_rel.name], depth + 1)
                                    scope_results.extend(deeper)
                            # Prefer scope-derived results over physical tables
                            if scope_results:
                                results.extend(scope_results)
                            elif physical_results:
                                results.extend(physical_results)

                    return results

                for alias_name, relation in child_scopes_to_check:
                    if column.upper() in relation.projections:
                        phys_candidates = trace_to_physical(relation, column, trace + [relation.name])
                        if phys_candidates:
                            return phys_candidates  # Found physical source - return immediately

                # FIX 18: If no child scopes but there ARE physical tables in current scope,
                # the identity column must reference one of those physical tables.
                # This handles innermost scopes like: SELECT D_CLOSE_DATE FROM STG_PREPAID_CARDS
                # where D_CLOSE_DATE = D_CLOSE_DATE (identity to physical table column)
                if not child_scopes_to_check:
                    physical_results: List[ResolvedColumn] = []
                    for alias_name, relation in scope.relations.items():
                        if isinstance(relation, str):  # Physical table
                            dm = "Y" if column_exists_in_dm(relation, column, dm_dict) else "N"
                            physical_results.append(ResolvedColumn(
                                source_type=SourceType.PHYSICAL,
                                table=relation,
                                column=column,
                                dm_match=dm,
                                trace_path="->".join(trace) + f":{relation}.{column}",
                                # FIX 24: Capture alias and reference (identity case)
                                source_alias=alias_name,
                                original_ref=column  # Unqualified identity reference
                            ))
                    if physical_results:
                        return physical_results

                # No child scope or physical table source found for identity column
                debug_info = _generate_debug_info(column, scope, "COLUMN_NOT_FOUND (identity passthrough)")
                return [ResolvedColumn(
                    source_type=SourceType.UNRESOLVED,
                    reason=f"{UnresolvedReason.COLUMN_NOT_FOUND.value} | {debug_info}",
                    trace_path="->".join(trace),
                    column=column
                )]

        # Find in visible relations - PRIORITIZE scopes over physical tables
        # For unqualified columns, scope-derived results (from subqueries) should take precedence
        # over physical JOINed tables to avoid picking wrong tables like DESJ_CTR_MAPPING
        scope_candidates: List[ResolvedColumn] = []
        physical_candidates: List[ResolvedColumn] = []

        for alias_name, relation in scope.relations.items():
            if isinstance(relation, str):
                # Physical table - check DM (lower priority)
                if column_exists_in_dm(relation, column, dm_dict):
                    physical_candidates.append(ResolvedColumn(
                        source_type=SourceType.PHYSICAL,
                        table=relation,
                        column=column,
                        dm_match="Y",
                        trace_path="->".join(trace) + f":{relation}.{column}"
                    ))
            elif isinstance(relation, Scope):
                # CTE or subquery scope - check if column exists in its projections (higher priority)
                if column.upper() in relation.projections:
                    # Resolve through this scope
                    proj = relation.projections[column.upper()]
                    sub_results = resolve_expression(
                        proj.expression,
                        relation,
                        dm_dict,
                        visited.copy(),
                        trace + [relation.name],
                        max_depth
                    )
                    scope_candidates.extend(sub_results)

        # Prefer scope-derived results over physical table matches
        if scope_candidates:
            return scope_candidates
        if physical_candidates:
            return physical_candidates

        # No candidates found - walk up to parent scope
        if scope.parent:
            return resolve_to_physical(ref, scope.parent, dm_dict, visited, trace, max_depth)
        debug_info = _generate_debug_info(column, scope, "COLUMN_NOT_FOUND (no parent scope)")
        return [ResolvedColumn(
            source_type=SourceType.UNRESOLVED,
            reason=f"{UnresolvedReason.COLUMN_NOT_FOUND.value} | {debug_info}",
            trace_path="->".join(trace),
            column=column
        )]

    debug_info = _generate_debug_info(ref, scope, "COLUMN_NOT_FOUND (fallback)")
    return [ResolvedColumn(
        source_type=SourceType.UNRESOLVED,
        reason=f"{UnresolvedReason.COLUMN_NOT_FOUND.value} | {debug_info}",
        trace_path="->".join(trace),
        column=ref
    )]


def resolve_expression(
    expr: str,
    scope: Scope,
    dm_dict: Dict[str, Set[str]],
    visited: Optional[Set[str]] = None,
    trace: Optional[List[str]] = None,
    max_depth: int = 50
) -> List[ResolvedColumn]:
    """Resolve all column references in an expression."""
    if visited is None:
        visited = set()
    if trace is None:
        trace = [scope.name]

    results: List[ResolvedColumn] = []

    # Check if entire expression is a constant
    if is_constant(expr.strip()):
        return [ResolvedColumn(
            source_type=SourceType.CONSTANT,
            constant_value=expr.strip(),
            trace_path="->".join(trace) + f":CONSTANT({expr.strip()})"
        )]

    # Handle scalar subqueries in expression (e.g., "(SELECT MAX(col) FROM tab)")
    subquery_pattern = r'\(\s*SELECT\b'
    if re.search(subquery_pattern, expr, re.IGNORECASE):
        try:
            # Parse expression to extract subqueries
            parsed = sqlglot.parse_one(f"SELECT {expr} AS _x", dialect="oracle")
            for subq in parsed.find_all(exp.Subquery):
                # Build scope for subquery
                sub_scope = build_scope_tree(subq.this, f"{scope.name}/SCALAR", scope, dm_dict)

                # Resolve from subquery's SELECT list
                for proj_name, proj in sub_scope.projections.items():
                    sub_results = resolve_expression(
                        proj.expression, sub_scope, dm_dict,
                        visited.copy(), trace + [sub_scope.name], max_depth
                    )
                    results.extend(sub_results)

                # Handle correlated references (refs to outer scope)
                subq_sql = subq.sql(dialect="oracle")
                for ref in extract_column_refs(subq_sql):
                    if '.' in ref:
                        alias, _ = ref.split('.', 1)
                        # Check if alias is from outer scope (not in subquery)
                        if alias.upper() not in sub_scope.relations and alias.upper() not in sub_scope.ctes:
                            outer_results = resolve_to_physical(
                                ref, scope, dm_dict,
                                visited.copy(), trace, max_depth
                            )
                            results.extend(outer_results)

            # If we found subquery results, return them (don't double-process)
            if results:
                return results
        except Exception as e:
            logger.debug(f"Scalar subquery parse failed for '{expr[:50]}...': {e}")

    # Extract all references
    refs = extract_column_refs(expr)

    if not refs:
        # No column refs found - might be pure constant expression
        return [ResolvedColumn(
            source_type=SourceType.CONSTANT,
            constant_value=expr,
            trace_path="->".join(trace) + f":CONSTANT({expr})"
        )]

    for ref in refs:
        resolved = resolve_to_physical(ref, scope, dm_dict, visited.copy(), trace.copy(), max_depth)
        results.extend(resolved)

    return results


def resolve_with_fallback(
    expr: str,
    scope: Scope,
    dm_dict: Dict[str, Set[str]],
    max_depth: int = 50
) -> List[ResolvedColumn]:
    """Never fail - always return something."""

    # Level 1: Try normal resolution
    try:
        results = resolve_expression(expr, scope, dm_dict, None, None, max_depth)
        if results:
            return results
    except Exception as e:
        logger.warning(f"L1 resolution failed for '{expr}': {e}")

    # Level 2: Try partial (extract individual refs)
    try:
        refs = extract_column_refs(expr)
        partial: List[ResolvedColumn] = []
        for ref in refs:
            try:
                resolved = resolve_to_physical(ref, scope, dm_dict, None, None, max_depth)
                partial.extend(resolved)
            except Exception:
                partial.append(ResolvedColumn(
                    source_type=SourceType.UNRESOLVED,
                    reason=UnresolvedReason.PARTIAL_FAILURE.value,
                    column=ref
                ))
        if partial:
            return partial
    except Exception:
        pass

    # Level 3: Last resort
    return [ResolvedColumn(
        source_type=SourceType.UNRESOLVED,
        reason=UnresolvedReason.COMPLETE_FAILURE.value,
        constant_value=expr
    )]


def _extract_scope_joins(
    select_expr: exp.Select,
    scope: Scope,
    object_name: str,
    sql_file: str = ""
) -> List[JoinKey]:
    """Extract joins that belong to THIS scope only (not from nested subqueries).

    This function extracts only DIRECT joins - those that are immediate children
    of the current SELECT, not joins nested inside subqueries. Each scope
    extracts its own joins, which are then resolved using that scope's context.
    """
    joins: List[JoinKey] = []
    join_seq = 0

    # Iterate through all Join nodes, but skip those nested in subqueries
    for join in select_expr.find_all(exp.Join):
        # Check if this join is nested inside another SELECT (subquery)
        # by walking up the AST to the current select_expr
        is_nested = False
        parent = join.parent
        while parent is not None and parent != select_expr:
            if isinstance(parent, exp.Select):
                # This join belongs to a nested SELECT, not our scope
                is_nested = True
                break
            parent = parent.parent

        if is_nested:
            continue  # Skip - this join belongs to a child scope

        # This join belongs to current scope - extract it
        join_seq += 1
        join_type = _get_join_type(join)

        # Get the joined table
        right_source = join.this
        right_table = ""
        right_alias = ""

        if isinstance(right_source, exp.Table):
            right_table = right_source.name.upper()
            right_alias = (right_source.alias or right_source.name).upper()
        elif isinstance(right_source, exp.Subquery):
            right_alias = right_source.alias.upper() if right_source.alias else f"SUBQ_{join_seq}"
            right_table = f"(SUBQUERY:{right_alias})"

        # Parse ON condition
        on_clause = join.args.get('on')
        if on_clause:
            join_keys, join_filters = _parse_join_condition(on_clause, scope, right_alias)

            for left_tbl, left_col, right_tbl, right_col, condition in join_keys:
                joins.append(JoinKey(
                    sql_file=sql_file,
                    join_seq=join_seq,
                    join_type=join_type,
                    left_table=left_tbl,
                    left_field=left_col,
                    right_table=right_tbl,
                    right_field=right_col,
                    join_condition=condition,
                    join_filters="; ".join(join_filters) if join_filters else "",
                    context_path=scope.name
                ))

    return joins


def collect_joins_with_scopes(scope: Scope) -> List[Tuple[JoinKey, Scope]]:
    """Collect all joins from the scope tree, each paired with its owning scope.

    This recursively traverses the entire scope tree (relations, CTEs, union branches)
    and collects (join, scope) pairs. Each join is paired with the scope where it
    was defined, allowing resolution to use the correct context.

    Returns:
        List of (JoinKey, Scope) tuples where each join is paired with its owning scope.
    """
    result: List[Tuple[JoinKey, Scope]] = []

    # Add this scope's joins
    for join in scope.joins:
        result.append((join, scope))

    # Recurse into child scopes (subqueries registered as relations)
    for child in scope.relations.values():
        if isinstance(child, Scope):
            result.extend(collect_joins_with_scopes(child))

    # Recurse into CTE scopes
    for cte_scope in scope.ctes.values():
        if cte_scope is not None:
            result.extend(collect_joins_with_scopes(cte_scope))

    # Recurse into UNION branches
    for branch in scope.union_branches:
        result.extend(collect_joins_with_scopes(branch))

    return result


def extract_joins(ast: exp.Expression, scope: Scope, object_name: str, sql_file: str = "") -> List[JoinKey]:
    """Extract JOIN conditions from AST."""
    joins: List[JoinKey] = []
    join_seq = 0

    select_expr = ast if isinstance(ast, exp.Select) else ast.find(exp.Select)
    if not select_expr:
        return joins

    for join in select_expr.find_all(exp.Join):
        join_seq += 1
        join_type = _get_join_type(join)

        # Get the joined table
        right_source = join.this
        right_table = ""
        right_alias = ""

        if isinstance(right_source, exp.Table):
            right_table = right_source.name.upper()
            right_alias = (right_source.alias or right_source.name).upper()
        elif isinstance(right_source, exp.Subquery):
            right_alias = right_source.alias.upper() if right_source.alias else f"SUBQ_{join_seq}"
            right_table = f"(SUBQUERY:{right_alias})"

        # Parse ON condition
        on_clause = join.args.get('on')
        if on_clause:
            join_keys, join_filters = _parse_join_condition(on_clause, scope, right_alias)

            for left_tbl, left_col, right_tbl, right_col, condition in join_keys:
                joins.append(JoinKey(
                    sql_file=sql_file,
                    join_seq=join_seq,
                    join_type=join_type,
                    left_table=left_tbl,
                    left_field=left_col,
                    right_table=right_tbl,
                    right_field=right_col,
                    join_condition=condition,
                    join_filters="; ".join(join_filters) if join_filters else "",
                    context_path=scope.name
                ))

    return joins


def _get_join_type(join: exp.Join) -> str:
    """Determine JOIN type."""
    if join.side:
        return f"{join.side.upper()} JOIN"
    if join.kind:
        return f"{join.kind.upper()} JOIN"
    return "INNER JOIN"


def _parse_join_condition(
    condition: exp.Expression,
    scope: Scope,
    right_alias: str
) -> Tuple[List[Tuple[str, str, str, str, str]], List[str]]:
    """Parse JOIN ON condition into keys and filters.

    NOTE: This function stores ORIGINAL aliases (not resolved table names) in the
    join_keys tuples. Physical resolution happens later in explode_join_keys()
    using each join's owning scope. This is important for nested joins where
    aliases are only visible in their local scope.

    Handles:
    - EQ (=): If both sides are columns, it's a join key; otherwise a filter
    - BETWEEN: Added to filters (e.g., date BETWEEN start_date AND end_date)
    - IN: Added to filters (e.g., col IN ('A', 'B'))
    - IS/IS NOT: Added to filters (e.g., col IS NULL)
    - Comparisons (>, <, >=, <=, !=): Added to filters
    - LIKE: Added to filters
    - OR conditions: Added to filters
    """
    join_keys: List[Tuple[str, str, str, str, str]] = []
    join_filters: List[str] = []

    # Track which expressions we've already processed as join keys
    processed_eq_ids: Set[int] = set()

    # Find all EQ expressions that are join keys (column = column)
    for eq in condition.find_all(exp.EQ):
        left_expr = eq.left
        right_expr = eq.right

        # Check if both sides are columns
        left_col = _extract_column_from_expr(left_expr)
        right_col = _extract_column_from_expr(right_expr)

        if left_col and right_col:
            # This is a join key - store ORIGINAL aliases, not resolved names
            left_alias, left_field = parse_ref(left_col)
            right_alias_parsed, right_field = parse_ref(right_col)

            # Store the original alias (or empty if unqualified)
            left_table = left_alias.upper() if left_alias else ""
            right_table = right_alias_parsed.upper() if right_alias_parsed else ""

            condition_str = f"{left_col} = {right_col}"  # Original condition text
            join_keys.append((left_table, left_field, right_table, right_field, condition_str))
            processed_eq_ids.add(id(eq))
        else:
            # EQ with literal - add as filter
            join_filters.append(eq.sql(dialect="oracle"))
            processed_eq_ids.add(id(eq))

    # Capture non-EQ expressions as filters
    # These expression types contain column references that need to be resolved
    filter_expression_types = (
        exp.Between,    # col BETWEEN val1 AND val2
        exp.In,         # col IN (...)
        exp.Is,         # col IS NULL / IS NOT NULL
        exp.GT,         # col > val
        exp.GTE,        # col >= val
        exp.LT,         # col < val
        exp.LTE,        # col <= val
        exp.NEQ,        # col != val / col <> val
        exp.Like,       # col LIKE pattern
        exp.ILike,      # col ILIKE pattern (case-insensitive)
        exp.Or,         # Entire OR clause as filter
    )

    for expr in condition.find_all(filter_expression_types):
        # Skip if this expression is nested inside an already-added expression
        # (e.g., don't add a nested BETWEEN if its parent OR was already added)
        parent = expr.parent
        is_nested_in_or = False
        while parent and parent != condition:
            if isinstance(parent, exp.Or):
                is_nested_in_or = True
                break
            parent = parent.parent

        if not is_nested_in_or:
            filter_sql = expr.sql(dialect="oracle")
            if filter_sql not in join_filters:  # Avoid duplicates
                join_filters.append(filter_sql)

    return join_keys, join_filters


def _extract_column_from_expr(expr: exp.Expression) -> Optional[str]:
    """Extract column reference from expression."""
    if isinstance(expr, exp.Column):
        if expr.table:
            return f"{expr.table}.{expr.name}"
        return expr.name
    return None


# =============================================================================
# SECTION 6B: JOIN KEY EXPLOSION
# =============================================================================

def explode_join_keys(
    joins_with_scopes: List[Tuple[JoinKey, Scope]],
    dm_dict: Dict[str, Set[str]],
    object_name: str,
    sql_file: str = "",
    max_depth: int = 50
) -> List[JoinKeyExploded]:
    """
    Resolve join key fields to physical sources using each join's owning scope.

    For each (JoinKey, Scope) pair, resolves both left and right fields through
    the JOIN'S OWN SCOPE (not ROOT) to find physical table.column sources.
    This is critical for nested joins where aliases are only visible in their
    local scope, not at ROOT level.

    Returns one JoinKeyExploded per resolved field.
    """
    exploded: List[JoinKeyExploded] = []

    for join, join_scope in joins_with_scopes:
        # Process LEFT side of join key using JOIN's scope
        if join.left_field:
            left_ref = f"{join.left_table}.{join.left_field}" if join.left_table else join.left_field
            left_resolved = _resolve_join_field(
                ref=left_ref,
                scope=join_scope,  # Use JOIN's scope, not ROOT
                dm_dict=dm_dict,
                join=join,
                join_side="LEFT",
                field_role="KEY",
                object_name=object_name,
                sql_file=sql_file,
                max_depth=max_depth
            )
            exploded.extend(left_resolved)

        # Process RIGHT side of join key using JOIN's scope
        if join.right_field:
            right_ref = f"{join.right_table}.{join.right_field}" if join.right_table else join.right_field
            right_resolved = _resolve_join_field(
                ref=right_ref,
                scope=join_scope,  # Use JOIN's scope, not ROOT
                dm_dict=dm_dict,
                join=join,
                join_side="RIGHT",
                field_role="KEY",
                object_name=object_name,
                sql_file=sql_file,
                max_depth=max_depth
            )
            exploded.extend(right_resolved)

        # Process FILTER conditions using JOIN's scope
        if join.join_filters:
            filter_exploded = _resolve_join_filters(
                filters=join.join_filters,
                scope=join_scope,  # Use JOIN's scope, not ROOT
                dm_dict=dm_dict,
                join=join,
                object_name=object_name,
                sql_file=sql_file,
                max_depth=max_depth
            )
            exploded.extend(filter_exploded)

    return exploded


def _resolve_join_field(
    ref: str,
    scope: Scope,
    dm_dict: Dict[str, Set[str]],
    join: JoinKey,
    join_side: str,
    field_role: str,
    object_name: str,
    sql_file: str,
    max_depth: int
) -> List[JoinKeyExploded]:
    """Resolve a single join field reference to physical source(s)."""
    results: List[JoinKeyExploded] = []

    # Parse the original reference
    original_alias, original_field = parse_ref(ref)

    # Resolve to physical using existing resolution logic
    resolved_columns = resolve_to_physical(ref, scope, dm_dict, None, None, max_depth)

    for resolved in resolved_columns:
        exploded = JoinKeyExploded(
            sql_file=sql_file,
            object_name=object_name,
            join_seq=join.join_seq,
            join_type=join.join_type,
            join_side=join_side,
            field_role=field_role,
            original_alias=original_alias or "",
            original_field=original_field,
            source_type=resolved.source_type.value,
            physical_table=resolved.table,
            physical_field=resolved.column,
            constant_value=resolved.constant_value,
            dm_match=resolved.dm_match,
            trace_path=resolved.trace_path,
            unresolved_reason=resolved.reason if resolved.source_type == SourceType.UNRESOLVED else "",
            join_condition=join.join_condition,
            context_path=join.context_path,
            # FIX 24: Full expression is the join condition
            full_expression=join.join_condition
        )
        results.append(exploded)

    return results


def _filter_non_column_refs(
    refs: List[str],
    dm_dict: Dict[str, Set[str]],
    scope: Scope
) -> List[str]:
    """
    FIX 19: Filter out tokens that are not actual column references.

    Removes:
    - schema.table patterns (e.g., ATOMIC.STG_DES3_DEFAULT_CASH_FLOW_SCD)
    - Bare table names (e.g., STG_CARDS)
    - Bare aliases (e.g., SC) that are subquery aliases, not columns
    """
    all_tables = {t.upper() for t in dm_dict.keys()} if dm_dict else set()

    # Common schema names - these are NOT aliases
    known_schemas = {'ATOMIC', 'DBO', 'PUBLIC', 'SYS', 'SCHEMA', 'ADMIN', 'APP'}

    # Collect all aliases in current scope (subquery aliases, table aliases)
    scope_aliases = {a.upper() for a in scope.relations.keys()} if scope else set()

    filtered = []
    for ref in refs:
        ref_upper = ref.upper()

        if '.' in ref:
            left, right = ref.split('.', 1)
            left_upper, right_upper = left.upper(), right.upper()

            # Skip if left is a known schema and right is a table name
            if left_upper in known_schemas and right_upper in all_tables:
                logger.debug(f"[FIX19] Skipping schema.table: {ref}")
                continue

            # Skip if right side is a known table name (likely schema.table pattern)
            if right_upper in all_tables and left_upper not in scope_aliases:
                logger.debug(f"[FIX19] Skipping schema.table (right is table): {ref}")
                continue
        else:
            # Bare token - skip if it's a known table name
            if ref_upper in all_tables:
                logger.debug(f"[FIX19] Skipping bare table name: {ref}")
                continue

            # Skip if it's a known alias in current scope (not a column)
            if ref_upper in scope_aliases:
                logger.debug(f"[FIX19] Skipping bare alias: {ref}")
                continue

        filtered.append(ref)

    return filtered


def _resolve_join_filters(
    filters: str,
    scope: Scope,
    dm_dict: Dict[str, Set[str]],
    join: JoinKey,
    object_name: str,
    sql_file: str,
    max_depth: int
) -> List[JoinKeyExploded]:
    """Resolve column references in join filter conditions."""
    results: List[JoinKeyExploded] = []

    # Extract column refs from filter string (may contain multiple conditions)
    refs = extract_column_refs(filters)

    # FIX 19: Filter out non-column tokens (schema.table, bare aliases, table names)
    refs = _filter_non_column_refs(refs, dm_dict, scope)

    for ref in refs:
        original_alias, original_field = parse_ref(ref)

        # Resolve the reference
        resolved_columns = resolve_to_physical(ref, scope, dm_dict, None, None, max_depth)

        for resolved in resolved_columns:
            exploded = JoinKeyExploded(
                sql_file=sql_file,
                object_name=object_name,
                join_seq=join.join_seq,
                join_type=join.join_type,
                join_side="FILTER",
                field_role="FILTER",
                original_alias=original_alias or "",
                original_field=original_field,
                source_type=resolved.source_type.value,
                physical_table=resolved.table,
                physical_field=resolved.column,
                constant_value=resolved.constant_value,
                dm_match=resolved.dm_match,
                trace_path=resolved.trace_path,
                unresolved_reason=resolved.reason if resolved.source_type == SourceType.UNRESOLVED else "",
                join_condition=filters,
                context_path=join.context_path,
                # FIX 24: Full expression is the filter expression
                full_expression=filters
            )
            results.append(exploded)

    return results


# =============================================================================
# SECTION 6C: MAPPING-JOIN RELATION
# =============================================================================

def generate_mapping_key(edge: LineageEdge, sql_file: str) -> str:
    """Generate composite key for a mapping."""
    return f"{sql_file}|{edge.dest_field}|{edge.source_table}|{edge.source_field}"


def generate_join_key(join: JoinKeyExploded) -> str:
    """Generate composite key for a join."""
    return f"{join.sql_file}|{join.context_path}|{join.join_seq}|{join.join_type}"


# build_mapping_join_relation() removed - joins now merged into Mapping_Exploded sheet


# =============================================================================
# SECTION 7: OUTPUT BUILDERS
# =============================================================================

def build_mapping_df(result: LineageResult) -> pd.DataFrame:
    """Build Mapping_exploded DataFrame (includes both MAPPING and JOIN rows)."""
    rows = []
    for edge in result.edges:
        rows.append({
            'sql_file': edge.sql_file,
            'object_name': edge.object_name,
            'row_type': edge.row_type,
            'dest_table': edge.dest_table,
            'dest_field': edge.dest_field,
            'source_type': edge.source_type,
            'source_table': edge.source_table,
            'source_field': edge.source_field,
            'constant_value': edge.constant_value,
            'expression': edge.expression,
            'dm_match': edge.dm_match,
            'trace_path': edge.trace_path,
            'notes': edge.notes,
            # FIX 24: Expression context columns
            'source_alias': edge.source_alias,
            'original_ref': edge.original_ref,
            'full_expression': edge.full_expression,
            # Join-specific columns
            'table_alias': edge.table_alias,
            'join_seq': edge.join_seq,
            'join_type': edge.join_type,
            'join_side': edge.join_side,
            'field_role': edge.field_role,
            'join_condition': edge.join_condition,
            'context_path': edge.context_path
        })

    df = pd.DataFrame(rows, columns=MAPPING_COLUMNS)

    # Sort: MAPPINGs first (by dest), then JOINs (by join_seq)
    df = df.sort_values(
        by=['sql_file', 'row_type', 'dest_table', 'dest_field', 'join_seq', 'join_side'],
        na_position='last'
    ).reset_index(drop=True)

    return df


def build_join_keys_df(result: LineageResult) -> pd.DataFrame:
    """Build Join_Keys DataFrame."""
    rows = []
    for join in result.joins:
        rows.append({
            'sql_file': join.sql_file,
            'object_name': result.object_name,
            'join_seq': join.join_seq,
            'join_type': join.join_type,
            'left_table': join.left_table,
            'left_field': join.left_field,
            'right_table': join.right_table,
            'right_field': join.right_field,
            'join_condition': join.join_condition,
            'join_filters': join.join_filters,
            'context_path': join.context_path
        })

    df = pd.DataFrame(rows, columns=JOIN_COLUMNS)
    return df


def build_join_keys_exploded_df(result: LineageResult) -> pd.DataFrame:
    """Build Join_Keys_Exploded DataFrame."""
    rows = []
    for jke in result.joins_exploded:
        rows.append({
            'sql_file': jke.sql_file,
            'object_name': jke.object_name,
            'join_seq': jke.join_seq,
            'join_type': jke.join_type,
            'join_side': jke.join_side,
            'field_role': jke.field_role,
            'original_alias': jke.original_alias,
            'original_field': jke.original_field,
            'source_type': jke.source_type,
            'physical_table': jke.physical_table,
            'physical_field': jke.physical_field,
            'constant_value': jke.constant_value,
            'dm_match': jke.dm_match,
            'trace_path': jke.trace_path,
            'unresolved_reason': jke.unresolved_reason,
            'join_condition': jke.join_condition,
            'context_path': jke.context_path,
            # FIX 24: Full expression context
            'full_expression': jke.full_expression
        })

    df = pd.DataFrame(rows, columns=JOIN_KEYS_EXPLODED_COLUMNS)

    # Sort by sql_file, join_seq, join_side
    if not df.empty:
        df = df.sort_values(
            by=['sql_file', 'join_seq', 'join_side', 'field_role'],
            na_position='last'
        ).reset_index(drop=True)

    return df


def build_summary_df(result: LineageResult, sql_file: str = "") -> pd.DataFrame:
    """Build DM_validation_summary DataFrame."""
    stats = result.stats

    # Get sql_file from result's edges if not provided
    if not sql_file and result.edges:
        sql_file = result.edges[0].sql_file

    rows = [
        {'sql_file': sql_file, 'metric': 'Total Mappings', 'value': stats.get('total_mappings', 0), 'notes': ''},
        {'sql_file': sql_file, 'metric': 'Physical Sources', 'value': stats.get('physical_count', 0), 'notes': ''},
        {'sql_file': sql_file, 'metric': 'Constant Sources', 'value': stats.get('constant_count', 0), 'notes': ''},
        {'sql_file': sql_file, 'metric': 'Unresolved Sources', 'value': stats.get('unresolved_count', 0), 'notes': ''},
        {'sql_file': sql_file, 'metric': 'DM Match (Y)', 'value': stats.get('dm_match_y', 0), 'notes': ''},
        {'sql_file': sql_file, 'metric': 'DM Match (N)', 'value': stats.get('dm_match_n', 0), 'notes': ''},
        {'sql_file': sql_file, 'metric': 'Join Count', 'value': stats.get('join_count', 0), 'notes': ''},
        {'sql_file': sql_file, 'metric': 'Warnings', 'value': len(result.warnings), 'notes': '; '.join(result.warnings[:5])},
    ]

    df = pd.DataFrame(rows, columns=SUMMARY_COLUMNS)
    return df


def build_qa_df(result: LineageResult, simple_refs: Set[Tuple[str, str]], sql_file: str = "") -> pd.DataFrame:
    """Build QA comparison DataFrame."""
    rows = []

    # Get sql_file from result's edges if not provided
    if not sql_file and result.edges:
        sql_file = result.edges[0].sql_file

    # Group edges by dest_field
    by_dest: Dict[str, List[LineageEdge]] = {}
    for edge in result.edges:
        key = edge.dest_field
        if key not in by_dest:
            by_dest[key] = []
        by_dest[key].append(edge)

    for dest_field, edges in by_dest.items():
        deep_sources = set()
        for e in edges:
            if e.source_type == SourceType.PHYSICAL.value:
                deep_sources.add((e.source_table, e.source_field))

        # Compare with simple extraction
        match = "Y" if deep_sources == simple_refs else "N"

        rows.append({
            'sql_file': sql_file,
            'object_name': result.object_name,
            'check_type': 'SOURCE_COMPARE',
            'dest_field': dest_field,
            'deep_sources': str(deep_sources),
            'simple_sources': str(simple_refs),
            'match': match,
            'notes': ''
        })

    df = pd.DataFrame(rows, columns=QA_COLUMNS)
    return df


def extract_simple_refs(sql: str) -> Set[Tuple[str, str]]:
    """Simple regex extraction for QA comparison."""
    refs: Set[Tuple[str, str]] = set()

    pattern = r'\b([A-Z_][A-Z0-9_]*)\.([A-Z_][A-Z0-9_]*)\b'
    for match in re.finditer(pattern, sql, re.IGNORECASE):
        table = match.group(1).upper()
        column = match.group(2).upper()
        refs.add((table, column))

    return refs


# =============================================================================
# SECTION 8: EXCEL WRITER
# =============================================================================

def get_next_output_path(output_dir: Path, object_name: str) -> Path:
    """Get next incremental output file path."""
    output_dir.mkdir(parents=True, exist_ok=True)

    i = 1
    while True:
        path = output_dir / f"{object_name}_Normalized_{i}.xlsx"
        if not path.exists():
            return path
        i += 1


def write_output_workbook(result: LineageResult, output_dir: Path, sql_content: str) -> Path:
    """Write output Excel workbook."""
    output_path = get_next_output_path(output_dir, result.object_name)

    # Build DataFrames
    mapping_df = build_mapping_df(result)
    join_df = build_join_keys_df(result)
    join_exploded_df = build_join_keys_exploded_df(result)
    summary_df = build_summary_df(result)
    simple_refs = extract_simple_refs(sql_content)
    qa_df = build_qa_df(result, simple_refs)

    # Write to Excel
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        mapping_df.to_excel(writer, sheet_name='Mapping_exploded', index=False)
        join_df.to_excel(writer, sheet_name='Join_Keys', index=False)
        join_exploded_df.to_excel(writer, sheet_name='Join_Keys_Exploded', index=False)
        summary_df.to_excel(writer, sheet_name='DM_validation_summary', index=False)
        qa_df.to_excel(writer, sheet_name='QA_compare_simple_vs_deep', index=False)

    logger.info(f"Written: {output_path}")
    return output_path


def write_combined_excel(
    results: List[LineageResult],
    output_dir: Path,
    dm_dict: Dict[str, Set[str]]
) -> Optional[Path]:
    """Write combined output from multiple SQL files to a single Excel workbook.

    Args:
        results: List of LineageResult objects from all processed SQL files
        output_dir: Output directory
        dm_dict: Data model dictionary (for validation)

    Returns:
        Path to combined Excel file, or None if no results
    """
    if not results:
        return None

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "Combined_Lineage.xlsx"

    # Combine all edges
    all_mapping_rows = []
    for result in results:
        df = build_mapping_df(result)
        all_mapping_rows.append(df)

    mapping_df = pd.concat(all_mapping_rows, ignore_index=True) if all_mapping_rows else pd.DataFrame(columns=MAPPING_COLUMNS)

    # Sort combined mapping by sql_file first
    if not mapping_df.empty:
        mapping_df = mapping_df.sort_values(
            by=['sql_file', 'dest_table', 'dest_field', 'source_type', 'source_table', 'source_field'],
            na_position='last'
        ).reset_index(drop=True)

    # Combine all joins
    all_join_rows = []
    for result in results:
        df = build_join_keys_df(result)
        all_join_rows.append(df)

    join_df = pd.concat(all_join_rows, ignore_index=True) if all_join_rows else pd.DataFrame(columns=JOIN_COLUMNS)

    # Combine all exploded joins
    all_join_exploded_rows = []
    for result in results:
        df = build_join_keys_exploded_df(result)
        all_join_exploded_rows.append(df)

    join_exploded_df = pd.concat(all_join_exploded_rows, ignore_index=True) if all_join_exploded_rows else pd.DataFrame(columns=JOIN_KEYS_EXPLODED_COLUMNS)

    # Combine all summaries
    all_summary_rows = []
    for result in results:
        sql_file = result.edges[0].sql_file if result.edges else result.object_name
        df = build_summary_df(result, sql_file=sql_file)
        all_summary_rows.append(df)

    summary_df = pd.concat(all_summary_rows, ignore_index=True) if all_summary_rows else pd.DataFrame(columns=SUMMARY_COLUMNS)

    # Combine all QA data
    all_qa_rows = []
    for result in results:
        sql_file = result.edges[0].sql_file if result.edges else result.object_name
        simple_refs = extract_simple_refs(result.sql_content)
        df = build_qa_df(result, simple_refs, sql_file=sql_file)
        all_qa_rows.append(df)

    qa_df = pd.concat(all_qa_rows, ignore_index=True) if all_qa_rows else pd.DataFrame(columns=QA_COLUMNS)

    # Write to Excel
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        mapping_df.to_excel(writer, sheet_name='Mapping_exploded', index=False)
        join_df.to_excel(writer, sheet_name='Join_Keys', index=False)
        join_exploded_df.to_excel(writer, sheet_name='Join_Keys_Exploded', index=False)
        summary_df.to_excel(writer, sheet_name='DM_validation_summary', index=False)
        qa_df.to_excel(writer, sheet_name='QA_compare_simple_vs_deep', index=False)

    logger.info(f"Written combined output: {output_path}")
    return output_path


# =============================================================================
# SECTION 9: MAIN PIPELINE
# =============================================================================

def is_direct_mapping(row: pd.Series) -> bool:
    """
    Check if row is a direct mapping (no expression, has physical source).
    Direct mappings can be output without SQL resolution when SQL doesn't contain the source.

    Returns True if:
    - Expression column is empty/null
    - Source Table has a value (not empty, not 'EXPRESSION' keyword)
    - Source Column has a value
    """
    expr = safe_str(row.get('Expression'))
    source_table = safe_str(row.get('Source Table'))
    source_column = safe_str(row.get('Source Column'))

    # Direct mapping: no expression, has source table and column
    if expr:
        return False  # Has expression → must resolve via SQL

    if not source_table or not source_column:
        return False  # Missing source info → can't do direct

    # Check if source_table is the "EXPRESSION" keyword
    if source_table.upper() == 'EXPRESSION':
        return False  # Expression mode (but expression is empty - edge case)

    return True


def create_direct_mapping_edge(
    row: pd.Series,
    object_name: str,
    dm_dict: Dict[str, Set[str]],
    sql_file: str = ""
) -> LineageEdge:
    """
    Create edge directly from Source Table/Column without SQL resolution.
    Used when Expression is empty and source is explicitly defined.
    """
    source_table = normalize_identifier(safe_str(row.get('Source Table')))
    source_column = normalize_identifier(safe_str(row.get('Source Column')))
    dest_table = safe_str(row.get('Target Table'))
    dest_field = safe_str(row.get('Target Column'))

    # Check DM match
    dm_match = 'Y' if column_exists_in_dm(source_table, source_column, dm_dict) else 'N'

    # FIX 24: Build original reference (no alias for direct mappings)
    orig_ref = f'{source_table}.{source_column}'
    return LineageEdge(
        sql_file=sql_file,
        object_name=object_name,
        dest_table=dest_table,
        dest_field=dest_field,
        source_type=SourceType.PHYSICAL.value,
        source_table=source_table,
        source_field=source_column,
        constant_value='',
        expression=orig_ref,
        dm_match=dm_match,
        trace_path='DIRECT_MAPPING',
        notes='Direct mapping from Source Table/Column (no Expression)',
        # FIX 24: Expression context (no alias for direct mappings)
        source_alias='',
        original_ref=orig_ref,
        full_expression=orig_ref
    )


def process_sql_file(
    sql_path: Path,
    mappings_df: pd.DataFrame,
    dm_dict: Dict[str, Set[str]],
    output_dir: Optional[Path] = None,
    max_depth: int = 50,
    sql_file_name: str = ""
) -> Optional[Union[Path, LineageResult]]:
    """Process a single SQL file.

    Args:
        sql_path: Path to SQL file
        mappings_df: DataFrame with T2T-F2T mappings
        dm_dict: Data model dictionary
        output_dir: Output directory. If None, returns LineageResult instead of writing file.
        max_depth: Maximum resolution depth
        sql_file_name: SQL filename for combined output. Defaults to sql_path.stem.

    Returns:
        Path to output file if output_dir provided, LineageResult if output_dir is None.
    """
    object_name, sql_content = load_sql_file(sql_path)

    # Use provided sql_file_name or default to path stem
    if not sql_file_name:
        sql_file_name = sql_path.stem

    # Filter mappings
    filtered = filter_mappings(mappings_df, object_name)
    if filtered.empty:
        logger.warning(f"No mappings found for {object_name}")
        return None

    # Validate SQL
    valid, sql_clean, warnings = validate_sql_content(sql_content, sql_path)
    if not valid:
        logger.error(f"Invalid SQL in {sql_path}: {sql_clean}")
        return None

    sql_normalized = normalize_sql(sql_clean)

    # Parse SQL
    try:
        ast = sqlglot.parse_one(sql_normalized, dialect="oracle")
    except Exception as e:
        logger.error(f"SQL parse error in {sql_path}: {e}")
        # Provide diagnostic hints for common issues
        diagnostics = diagnose_sql_issues(sql_normalized)
        if diagnostics:
            for diag in diagnostics:
                logger.error(f"  Hint: {diag}")
        # Re-run normalization with debug to show quote counts at each step
        logger.info("Debug: Re-running normalization to trace quote counts...")
        normalize_sql(sql_clean, debug=True)
        return None

    # Build scope tree (pass dm_dict for SELECT * expansion)
    scope = build_scope_tree(ast, dm_dict=dm_dict)

    # Initialize result
    result = LineageResult(object_name=object_name, sql_content=sql_content, warnings=warnings)

    # Process each mapping row
    for idx, row in filtered.iterrows():
        dest_table = safe_str(row.get('Target Table'))
        dest_field = safe_str(row.get('Target Column'))

        # Check if this is a direct mapping (no expression, has source table/column)
        if is_direct_mapping(row):
            # Direct mapping mode: try SQL resolution first, fall back to direct output
            source_table = safe_str(row.get('Source Table'))
            source_column = safe_str(row.get('Source Column'))
            expr = f"{source_table}.{source_column}"

            # Try SQL resolution first (Flexible mode)
            resolved_columns = resolve_with_fallback(expr, scope, dm_dict, max_depth)

            # If SQL resolution failed or returned UNRESOLVED, use direct mapping
            all_unresolved = all(r.source_type == SourceType.UNRESOLVED for r in resolved_columns)
            if not resolved_columns or all_unresolved:
                # Fall back to direct mapping
                edge = create_direct_mapping_edge(row, object_name, dm_dict, sql_file=sql_file_name)
                result.edges.append(edge)
            else:
                # SQL resolution succeeded
                for resolved in resolved_columns:
                    edge = LineageEdge(
                        sql_file=sql_file_name,
                        object_name=object_name,
                        dest_table=dest_table,
                        dest_field=dest_field,
                        source_type=resolved.source_type.value,
                        source_table=resolved.table,
                        source_field=resolved.column,
                        constant_value=resolved.constant_value,
                        expression=expr,
                        dm_match=resolved.dm_match,
                        trace_path=resolved.trace_path,
                        notes=resolved.reason,
                        # FIX 24: Expression context
                        source_alias=resolved.source_alias,
                        original_ref=resolved.original_ref,
                        full_expression=resolved.full_expression or expr  # Use resolved if available, else doc-support expr
                    )
                    result.edges.append(edge)
        else:
            # Expression mode: parse Expression via SQL
            expr = safe_str(row.get('Expression'))
            if not expr:
                # Edge case: no expression AND source_table is "EXPRESSION" keyword
                # Try Source Column as expression
                expr = safe_str(row.get('Source Column'))
            if not expr:
                expr = dest_field

            # Resolve expression through SQL
            resolved_columns = resolve_with_fallback(expr, scope, dm_dict, max_depth)

            # Create edges (multiple rows if multiple sources)
            for resolved in resolved_columns:
                edge = LineageEdge(
                    sql_file=sql_file_name,
                    object_name=object_name,
                    dest_table=dest_table,
                    dest_field=dest_field,
                    source_type=resolved.source_type.value,
                    source_table=resolved.table,
                    source_field=resolved.column,
                    constant_value=resolved.constant_value,
                    expression=expr,
                    dm_match=resolved.dm_match,
                    trace_path=resolved.trace_path,
                    notes=resolved.reason,
                    # FIX 24: Expression context
                    source_alias=resolved.source_alias,
                    original_ref=resolved.original_ref,
                    full_expression=resolved.full_expression or expr  # Use resolved if available, else doc-support expr
                )
                result.edges.append(edge)

    # Collect joins from all scopes (extracted during build_scope_tree)
    # Each join is paired with its owning scope for correct resolution
    joins_with_scopes = collect_joins_with_scopes(scope)

    # Update sql_file on each join (was empty during scope building)
    for join, _ in joins_with_scopes:
        join.sql_file = sql_file_name

    # Extract just the joins for result.joins (backward compatibility)
    result.joins = [join for join, _ in joins_with_scopes]

    # Explode join keys to physical sources using each join's owning scope
    result.joins_exploded = explode_join_keys(
        joins_with_scopes=joins_with_scopes,
        dm_dict=dm_dict,
        object_name=object_name,
        sql_file=sql_file_name,
        max_depth=max_depth
    )

    # Convert JoinKeyExploded to LineageEdge and append to edges (merged output)
    for jke in result.joins_exploded:
        # FIX 24: Build original_ref from alias and field
        orig_ref = f"{jke.original_alias}.{jke.original_field}" if jke.original_alias else jke.original_field
        join_edge = LineageEdge(
            sql_file=jke.sql_file,
            object_name=jke.object_name,
            row_type="JOIN",
            dest_table="",  # Joins don't have a destination
            dest_field="",
            source_type=jke.source_type,
            source_table=jke.physical_table,
            source_field=jke.physical_field,
            constant_value=jke.constant_value,
            expression=orig_ref,
            dm_match=jke.dm_match,
            trace_path=jke.trace_path,
            notes=jke.unresolved_reason,
            # FIX 24: Expression context
            source_alias=jke.original_alias,
            original_ref=orig_ref,
            full_expression=jke.full_expression,
            # Join-specific fields
            table_alias=jke.original_alias,
            join_seq=str(jke.join_seq) if jke.join_seq else "",
            join_type=jke.join_type,
            join_side=jke.join_side,
            field_role=jke.field_role,
            join_condition=jke.join_condition,
            context_path=jke.context_path
        )
        result.edges.append(join_edge)

    # Note: Mapping_Join_Relation removed - joins are now merged into edges

    # Calculate stats (edges now includes both MAPPING and JOIN rows)
    mapping_edges = [e for e in result.edges if e.row_type == "MAPPING"]
    join_edges = [e for e in result.edges if e.row_type == "JOIN"]

    result.stats = {
        'total_rows': len(result.edges),
        'mapping_count': len(mapping_edges),
        'join_field_count': len(join_edges),
        'physical_count': sum(1 for e in result.edges if e.source_type == SourceType.PHYSICAL.value),
        'constant_count': sum(1 for e in result.edges if e.source_type == SourceType.CONSTANT.value),
        'unresolved_count': sum(1 for e in result.edges if e.source_type == SourceType.UNRESOLVED.value),
        'dm_match_y': sum(1 for e in result.edges if e.dm_match == 'Y'),
        'dm_match_n': sum(1 for e in result.edges if e.dm_match == 'N'),
        'join_count': len(result.joins),
    }

    # Check unresolved rate (for mappings only)
    total = result.stats['mapping_count']
    unresolved = sum(1 for e in mapping_edges if e.source_type == SourceType.UNRESOLVED.value)
    if total > 0:
        pct = (unresolved / total) * 100
        if pct > 20:
            logger.warning(
                f"HIGH UNRESOLVED RATE: {pct:.1f}% ({unresolved}/{total}) in {object_name}"
            )

    # Return result or write output
    if output_dir is None:
        # Return LineageResult for combined output mode
        return result
    else:
        # Write individual output file
        return write_output_workbook(result, output_dir, sql_content)


def process_batch_with_progress(
    sql_files: List[Path],
    mappings_df: pd.DataFrame,
    dm_dict: Dict[str, Set[str]],
    output_dir: Path,
    max_depth: int = 50,
    resume: bool = False,
    force: bool = False,
    separate: bool = False
) -> List[Path]:
    """Process multiple SQL files with progress.

    Args:
        sql_files: List of SQL file paths
        mappings_df: DataFrame with T2T-F2T mappings
        dm_dict: Data model dictionary
        output_dir: Output directory
        max_depth: Maximum resolution depth
        resume: Resume from previous run
        force: Force reprocessing
        separate: If True, write separate Excel per SQL (legacy). If False (default), write combined.

    Returns:
        List of output file paths
    """
    total = len(sql_files)
    success = 0
    failed = 0
    skipped = 0
    outputs: List[Path] = []

    # For combined output mode, collect all results
    all_results: List[LineageResult] = []

    # Load manifest for resume
    manifest = load_manifest(output_dir) if resume else {"files": {}}

    for i, sql_file in enumerate(sql_files, 1):
        status_prefix = f"[{i}/{total}]"
        obj_name = sql_file.stem.upper()

        # Check resume
        if resume and not force:
            if should_process(obj_name, manifest):
                pass  # Process it
            else:
                print(f"{status_prefix} Skipping {sql_file.name} (already processed)")
                skipped += 1
                continue

        print(f"{status_prefix} Processing {sql_file.name}...", end='', flush=True)

        try:
            if separate:
                # Legacy mode: write separate Excel per SQL
                result = process_sql_file(
                    sql_file, mappings_df, dm_dict, output_dir, max_depth,
                    sql_file_name=sql_file.stem
                )
                if result:
                    print(" OK")
                    success += 1
                    outputs.append(result)
                    manifest["files"][obj_name] = {"status": "success", "output": str(result)}
                else:
                    print(" SKIPPED (no mappings)")
                    skipped += 1
                    manifest["files"][obj_name] = {"status": "skipped", "reason": "no mappings"}
            else:
                # Combined mode: collect results, write at end
                result = process_sql_file(
                    sql_file, mappings_df, dm_dict, None, max_depth,
                    sql_file_name=sql_file.stem
                )
                if result and isinstance(result, LineageResult):
                    print(" OK")
                    success += 1
                    all_results.append(result)
                    manifest["files"][obj_name] = {"status": "success"}
                else:
                    print(" SKIPPED (no mappings)")
                    skipped += 1
                    manifest["files"][obj_name] = {"status": "skipped", "reason": "no mappings"}
        except Exception as e:
            print(f" FAILED: {e}")
            logger.exception(f"Error processing {sql_file}")
            failed += 1
            manifest["files"][obj_name] = {"status": "failed", "error": str(e)}

        # Save manifest after each file
        if resume:
            save_manifest(output_dir, manifest)

    # Write combined output if not in separate mode
    if not separate and all_results:
        print(f"\nWriting combined output...")
        combined_path = write_combined_excel(all_results, output_dir, dm_dict)
        if combined_path:
            outputs.append(combined_path)
            print(f"Combined output: {combined_path}")

    # Summary
    print(f"\n{'='*50}")
    print(f"Processed: {total} | Success: {success} | Skipped: {skipped} | Failed: {failed}")

    return outputs


def load_manifest(output_dir: Path) -> dict:
    """Load processing manifest."""
    manifest_path = output_dir / MANIFEST_FILE
    if manifest_path.exists():
        try:
            return json.loads(manifest_path.read_text())
        except Exception:
            pass
    return {"created": datetime.now().isoformat(), "files": {}}


def save_manifest(output_dir: Path, manifest: dict) -> None:
    """Save processing manifest."""
    manifest["updated"] = datetime.now().isoformat()
    manifest_path = output_dir / MANIFEST_FILE
    manifest_path.write_text(json.dumps(manifest, indent=2))


def should_process(obj_name: str, manifest: dict, force: bool = False) -> bool:
    """Check if file should be processed."""
    if force:
        return True
    entry = manifest.get("files", {}).get(obj_name)
    if entry and entry.get("status") == "success":
        return False
    return True


def dry_run(doc_support: Path, sql_source: Path) -> None:
    """Validate inputs without processing."""
    print("=== DRY RUN MODE ===\n")

    # Load Excel
    mappings_df, dm_dict = load_doc_support(doc_support)
    print(f"Excel loaded: {len(mappings_df)} mapping rows, {len(dm_dict)} tables in DM")

    # Find SQL files
    if sql_source.is_file():
        sql_files = [sql_source]
    else:
        sql_files = load_sql_directory(sql_source)

    print(f"SQL files found: {len(sql_files)}")

    # Check matches (case-insensitive column lookup)
    name_col = find_column_case_insensitive(mappings_df, 'Name')
    if not name_col:
        print(f"ERROR: Required column 'Name' not found. Available columns: {list(mappings_df.columns)}")
        return
    mapping_names = set(mappings_df[name_col].str.upper().str.strip())
    would_process = []
    would_skip = []

    for sql_file in sql_files:
        obj_name = sql_file.stem.upper()
        if obj_name in mapping_names:
            would_process.append(sql_file.name)
        else:
            would_skip.append(sql_file.name)

    print(f"\nWould process: {len(would_process)}")
    print(f"Would skip (no mapping): {len(would_skip)}")

    if would_skip:
        print(f"\nSkipped files: {would_skip[:5]}{'...' if len(would_skip) > 5 else ''}")

    print(f"\nEstimated output: {len(would_process)} Excel files")


# =============================================================================
# SECTION 10: CLI
# =============================================================================

def setup_logging(level: str, output_dir: Path) -> None:
    """Configure logging."""
    log_levels = {
        'normal': logging.WARNING,
        'verbose': logging.INFO,
        'debug': logging.DEBUG,
        'trace': TRACE_LEVEL
    }

    # Ensure output dir exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # File handler
    file_handler = logging.FileHandler(
        output_dir / 'lineage_parser.log',
        mode='w',
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s'
    ))

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_levels.get(level, logging.WARNING))
    console_handler.setFormatter(logging.Formatter('[%(levelname)s] %(message)s'))

    # Configure root logger
    logging.basicConfig(
        level=min(log_levels.values()),
        handlers=[file_handler, console_handler]
    )


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='SQL Lineage Parser - Extract column-level lineage from Oracle T2T SQL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --doc-support DOC-SUPPORT.xlsx --sql T2T_CUSTOMER.sql
  %(prog)s --doc-support DOC-SUPPORT.xlsx --sql-dir sql_files/ --verbose
  %(prog)s --doc-support DOC-SUPPORT.xlsx --sql-dir sql_files/ --dry-run
        """
    )

    # Required
    parser.add_argument('--doc-support', required=True, type=Path,
                        help='Path to DOC-SUPPORT.xlsx')

    # Input (one required)
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument('--sql', type=Path, help='Single SQL file')
    input_group.add_argument('--sql-dir', type=Path, help='Directory of SQL files')

    # Output
    parser.add_argument('--output', type=Path, default=Path('output/'),
                        help='Output directory (default: output/)')

    # Logging
    log_group = parser.add_mutually_exclusive_group()
    log_group.add_argument('--verbose', action='store_true', help='Verbose output')
    log_group.add_argument('--debug', action='store_true', help='Debug output')
    log_group.add_argument('--trace', action='store_true', help='Trace output')

    # Modes
    parser.add_argument('--dry-run', action='store_true',
                        help='Validate without processing')
    parser.add_argument('--resume', action='store_true',
                        help='Skip already-processed files')
    parser.add_argument('--force', action='store_true',
                        help='Reprocess even if output exists')
    parser.add_argument('--separate', action='store_true',
                        help='Write separate Excel per SQL file (legacy mode). Default: combined output')

    # Advanced
    parser.add_argument('--max-depth', type=int, default=50,
                        help='Max resolution depth (default: 50)')
    parser.add_argument('--version', action='version', version=f'%(prog)s {VERSION}')

    args = parser.parse_args()

    # Determine log level
    log_level = 'normal'
    if args.trace:
        log_level = 'trace'
    elif args.debug:
        log_level = 'debug'
    elif args.verbose:
        log_level = 'verbose'

    # Setup logging
    setup_logging(log_level, args.output)

    logger.info(f"SQL Lineage Parser v{VERSION}")

    # Determine SQL source
    sql_source = args.sql if args.sql else args.sql_dir

    # Dry run mode
    if args.dry_run:
        dry_run(args.doc_support, sql_source)
        return 0

    # Load Excel
    mappings_df, dm_dict = load_doc_support(args.doc_support)

    # Process
    if args.sql:
        # Single file mode
        result = process_sql_file(
            args.sql, mappings_df, dm_dict, args.output, args.max_depth
        )
        if result:
            print(f"\nOutput: {result}")
            return 0
        else:
            return 1
    else:
        # Batch mode
        sql_files = load_sql_directory(args.sql_dir)
        outputs = process_batch_with_progress(
            sql_files, mappings_df, dm_dict, args.output,
            args.max_depth, args.resume, args.force, args.separate
        )

        if outputs:
            if args.separate:
                print(f"\nOutputs written to: {args.output}")
            else:
                print(f"\nCombined output written to: {args.output}")
            return 0
        else:
            return 1


if __name__ == '__main__':
    sys.exit(main())
