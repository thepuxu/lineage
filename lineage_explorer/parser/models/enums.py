"""Enumerations for lineage data types."""

from enum import Enum


class SourceType(str, Enum):
    """Type of source in lineage mapping."""
    PHYSICAL = "PHYSICAL"       # Resolved to table.column
    CONSTANT = "CONSTANT"       # NULL, SYSDATE, literals, etc.
    UNRESOLVED = "UNRESOLVED"   # Could not resolve
    DERIVED = "DERIVED"         # Computed expression (optional)


class UsageType(str, Enum):
    """Type of usage in mapping."""
    MAPPING = "MAPPING"         # Column mapping (SELECT → target)
    JOIN = "JOIN"               # Join dependency


class UsageRole(str, Enum):
    """Role of the mapping."""
    VALUE = "VALUE"             # Direct value mapping
    JOIN_KEY = "JOIN_KEY"       # Join key predicate (A.col = B.col)
    JOIN_FILTER = "JOIN_FILTER" # Join filter predicate


class TransformationType(str, Enum):
    """Type of transformation applied."""
    DIRECT = "DIRECT"           # No transformation (col → col)
    AGGREGATE = "AGGREGATE"     # SUM, COUNT, AVG, etc.
    CONDITIONAL = "CONDITIONAL" # CASE WHEN
    CALCULATE = "CALCULATE"     # Math operations (+, -, *, /)
    FORMAT = "FORMAT"           # String functions (UPPER, SUBSTR, etc.)
    TYPE_CAST = "TYPE_CAST"     # CAST, TO_DATE, etc.
    WINDOW = "WINDOW"           # Window/analytic functions
    LOOKUP = "LOOKUP"           # Join for value lookup
    FILTER = "FILTER"           # WHERE/HAVING condition
    OTHER = "OTHER"             # Other transformations


class LineageConfidence(str, Enum):
    """Confidence level of lineage resolution."""
    HIGH = "HIGH"       # Direct qualified reference (ALIAS.COL)
    MEDIUM = "MEDIUM"   # Inferred from single table or projection
    LOW = "LOW"         # Ambiguous, best guess
    NONE = "NONE"       # Unresolved


class UnresolvedReason(str, Enum):
    """Reason codes for unresolved lineage."""
    DEPTH_GUARD = "DEPTH_GUARD"                     # Recursion limit exceeded
    ALIAS_NOT_FOUND = "ALIAS_NOT_FOUND"             # Alias missing from scope
    MISSING_PROJECTION = "MISSING_PROJECTION"       # Token not in projections
    PARSER_LIMITATION = "PARSER_LIMITATION"         # Unsupported syntax
    TOKEN_NOT_IN_DM = "TOKEN_NOT_IN_DM_SINGLE_BASE" # DM lacks token
    AMBIGUOUS_UNQUALIFIED = "AMBIGUOUS_UNQUALIFIED" # Multiple candidates
    DYNAMIC_SQL = "DYNAMIC_SQL"                     # Cannot trace dynamic SQL
