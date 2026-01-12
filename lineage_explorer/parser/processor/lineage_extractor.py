"""
Extract column-level lineage from SQL using SQLGlot.

This module provides the core lineage extraction functionality,
wrapping SQLGlot's parsing and lineage capabilities.
"""

from typing import Dict, List, Optional, Set, Tuple
import re

try:
    import sqlglot
    from sqlglot import exp, parse_one
    from sqlglot.lineage import lineage
    from sqlglot.optimizer.qualify import qualify
    from sqlglot.optimizer.scope import build_scope
    HAS_SQLGLOT = True
except ImportError:
    HAS_SQLGLOT = False

from ..models.enums import (
    SourceType, UsageType, UsageRole,
    TransformationType, LineageConfidence, UnresolvedReason
)
from ..models.dataclasses import LineageEdge, LineageData, ColumnRef


# Constants for detection
CONSTANT_PATTERNS = [
    r"^NULL$",
    r"^SYSDATE$",
    r"^SYSTIMESTAMP$",
    r"^CURRENT_DATE$",
    r"^CURRENT_TIMESTAMP$",
    r"^ROWNUM$",
    r"^ROWID$",
    r"^'[^']*'$",  # String literals
    r"^\d+(\.\d+)?$",  # Numbers
    r"^\$[A-Z_]+",  # Parameters
]

AGGREGATE_FUNCTIONS = {
    "SUM", "COUNT", "AVG", "MIN", "MAX", "LISTAGG",
    "GROUP_CONCAT", "ARRAY_AGG", "STRING_AGG"
}

WINDOW_FUNCTIONS = {
    "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE",
    "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE",
    "NTH_VALUE", "PERCENT_RANK", "CUME_DIST"
}


class LineageExtractor:
    """Extract column-level lineage from SQL statements."""

    def __init__(self, schema: Optional[Dict[str, Dict[str, str]]] = None,
                 dialect: str = "oracle"):
        """
        Initialize the extractor.

        Args:
            schema: Optional schema dict {table: {column: type}}
            dialect: SQL dialect (default: oracle)
        """
        if not HAS_SQLGLOT:
            raise ImportError("sqlglot is required for lineage extraction. "
                            "Install with: pip install sqlglot")

        self.schema = schema or {}
        self.dialect = dialect
        self._dm_columns: Dict[str, Set[str]] = {}

        # Build column sets for DM matching
        for table, columns in self.schema.items():
            self._dm_columns[table.upper()] = set(c.upper() for c in columns.keys())

    def extract(self, sql: str, object_name: str,
                target_table: str) -> LineageData:
        """
        Extract lineage from SQL.

        Args:
            sql: SQL SELECT statement
            object_name: Name of the object (e.g., T2T name)
            target_table: Target table name for the mapping

        Returns:
            LineageData with extracted edges
        """
        result = LineageData(source=object_name)
        result.metadata["object_name"] = object_name
        result.metadata["target_table"] = target_table

        try:
            # Parse SQL
            ast = parse_one(sql, dialect=self.dialect)

            # Qualify columns with table references where possible
            if self.schema:
                try:
                    ast = qualify(ast, schema=self.schema, dialect=self.dialect)
                except Exception:
                    pass  # Continue with unqualified AST

            # Build alias map from FROM clause
            alias_map = self._build_alias_map(ast)

            # Extract column mappings (SELECT items)
            self._extract_select_mappings(ast, object_name, target_table, result, alias_map)

            # Extract JOIN dependencies
            self._extract_joins(ast, object_name, target_table, result, alias_map)

        except Exception as e:
            # Add error edge
            edge = LineageEdge(
                target_table=target_table,
                target_column="*",
                source_type=SourceType.UNRESOLVED,
                usage_type=UsageType.MAPPING,
                usage_role=UsageRole.VALUE,
                object_name=object_name,
                notes=f"Parse error: {str(e)}"
            )
            result.add_edge(edge)

        return result

    def _build_alias_map(self, ast: exp.Expression) -> Dict[str, str]:
        """Build a map from alias to physical table name."""
        alias_map = {}

        # Find all table references in FROM clauses
        for table in ast.find_all(exp.Table):
            table_name = table.name.upper() if table.name else ""
            alias = table.alias.upper() if table.alias else table_name

            if alias and table_name:
                alias_map[alias] = table_name
                # Also map the table to itself
                alias_map[table_name] = table_name

        return alias_map

    def _extract_select_mappings(self, ast: exp.Expression, object_name: str,
                                  target_table: str, result: LineageData,
                                  alias_map: Dict[str, str]):
        """Extract column mappings from SELECT clause."""
        # Find all SELECT statements (including CTEs, subqueries)
        for select in ast.find_all(exp.Select):
            # Get SELECT items
            for i, expr in enumerate(select.expressions):
                # Determine target column name
                if isinstance(expr, exp.Alias):
                    target_col = expr.alias
                    source_expr = expr.this
                elif hasattr(expr, "alias") and expr.alias:
                    target_col = expr.alias
                    source_expr = expr
                else:
                    target_col = self._infer_column_name(expr, i)
                    source_expr = expr

                # Extract sources from expression
                sources = self._extract_sources(source_expr, target_col, alias_map)

                for src in sources:
                    edge = LineageEdge(
                        target_table=target_table,
                        target_column=target_col.upper(),
                        source_table=src.get("table", ""),
                        source_column=src.get("column", ""),
                        source_type=src.get("source_type", SourceType.PHYSICAL),
                        usage_type=UsageType.MAPPING,
                        usage_role=UsageRole.VALUE,
                        transformation=src.get("transformation", TransformationType.DIRECT),
                        object_name=object_name,
                        expression=src.get("expression", ""),
                        constant_value=src.get("constant_value", ""),
                        confidence=src.get("confidence", LineageConfidence.HIGH),
                        dm_match=self._dm_match(src.get("table", ""), src.get("column", "")),
                        trace_path=src.get("trace_path", ""),
                        notes=src.get("notes", "")
                    )
                    result.add_edge(edge)

    def _extract_sources(self, expr: exp.Expression,
                         target_col: str,
                         alias_map: Optional[Dict[str, str]] = None) -> List[Dict]:
        """Extract source columns from an expression."""
        sources = []
        alias_map = alias_map or {}

        # Handle different expression types
        expr_str = expr.sql(dialect=self.dialect) if expr else ""

        # Check for constants
        if self._is_constant(expr):
            sources.append({
                "table": "",
                "column": "",
                "source_type": SourceType.CONSTANT,
                "constant_value": expr_str,
                "transformation": TransformationType.DIRECT,
                "confidence": LineageConfidence.HIGH,
                "expression": expr_str
            })
            return sources

        # Check for column references
        columns = list(expr.find_all(exp.Column))

        if not columns:
            # Expression with no column refs (might be function/constant)
            if expr_str:
                sources.append({
                    "table": "",
                    "column": "",
                    "source_type": SourceType.CONSTANT,
                    "constant_value": expr_str,
                    "transformation": self._detect_transformation(expr),
                    "confidence": LineageConfidence.MEDIUM,
                    "expression": expr_str
                })
            return sources

        # Process each column reference
        transformation = self._detect_transformation(expr)

        for col in columns:
            table_name = ""
            column_name = col.name.upper() if col.name else ""

            # Get table from column reference
            if col.table:
                table_name = col.table.upper()
            elif hasattr(col, "this") and hasattr(col.this, "table"):
                table_name = col.this.table.upper() if col.this.table else ""

            # Resolve alias to physical table name using alias_map
            original_alias = table_name
            if table_name and table_name in alias_map:
                table_name = alias_map[table_name]

            # Determine source type
            if table_name and column_name:
                source_type = SourceType.PHYSICAL
                confidence = LineageConfidence.HIGH
            elif column_name:
                source_type = SourceType.PHYSICAL
                confidence = LineageConfidence.MEDIUM
            else:
                source_type = SourceType.UNRESOLVED
                confidence = LineageConfidence.LOW

            # Build trace path showing alias -> table resolution
            if original_alias and original_alias != table_name:
                trace_path = f"{original_alias}({table_name}).{column_name}"
            elif table_name:
                trace_path = f"{table_name}.{column_name}"
            else:
                trace_path = column_name

            sources.append({
                "table": table_name,
                "column": column_name,
                "source_type": source_type,
                "transformation": transformation,
                "confidence": confidence,
                "expression": expr_str,
                "trace_path": trace_path
            })

        return sources

    def _extract_joins(self, ast: exp.Expression, object_name: str,
                       target_table: str, result: LineageData,
                       alias_map: Dict[str, str]):
        """Extract JOIN dependencies."""
        # Find all JOINs
        for join in ast.find_all(exp.Join):
            join_type = self._get_join_type(join)

            # Get joined table
            join_table = ""
            if isinstance(join.this, exp.Table):
                join_table = join.this.name.upper() if join.this.name else ""

            # Get alias
            join_alias = ""
            if hasattr(join.this, "alias") and join.this.alias:
                join_alias = join.this.alias.upper()

            # Extract ON conditions
            if join.args.get("on"):
                on_expr = join.args["on"]
                self._extract_join_conditions(
                    on_expr, object_name, target_table,
                    join_table, join_alias, join_type, result, alias_map
                )

    def _extract_join_conditions(self, on_expr: exp.Expression,
                                  object_name: str, target_table: str,
                                  join_table: str, join_alias: str,
                                  join_type: str, result: LineageData,
                                  alias_map: Dict[str, str]):
        """Extract individual join conditions."""
        # Split on AND
        conditions = self._split_on_and(on_expr)

        for cond in conditions:
            # Determine if this is a key or filter
            is_key = self._is_join_key(cond)
            usage_role = UsageRole.JOIN_KEY if is_key else UsageRole.JOIN_FILTER

            # Get columns involved
            columns = list(cond.find_all(exp.Column))

            for col in columns:
                # Get alias or table name from column reference
                col_table = col.table.upper() if col.table else ""
                column_name = col.name.upper() if col.name else ""

                # Resolve alias to physical table name
                if col_table and col_table in alias_map:
                    table_name = alias_map[col_table]
                elif col_table:
                    table_name = col_table
                else:
                    table_name = join_table

                edge = LineageEdge(
                    target_table=target_table,
                    target_column="",  # JOIN deps don't map to specific target col
                    source_table=table_name,
                    source_column=column_name,
                    source_type=SourceType.PHYSICAL,
                    usage_type=UsageType.JOIN,
                    usage_role=usage_role,
                    object_name=object_name,
                    expression=cond.sql(dialect=self.dialect),
                    join_alias=join_alias or table_name,
                    join_keys=cond.sql(dialect=self.dialect) if is_key else "",
                    join_filters=cond.sql(dialect=self.dialect) if not is_key else "",
                    dm_match=self._dm_match(table_name, column_name),
                    notes=f"{join_type} JOIN"
                )
                result.add_edge(edge)

    def _is_constant(self, expr: exp.Expression) -> bool:
        """Check if expression is a constant."""
        if expr is None:
            return True

        if isinstance(expr, (exp.Literal, exp.Null)):
            return True

        expr_str = expr.sql(dialect=self.dialect).upper()

        for pattern in CONSTANT_PATTERNS:
            if re.match(pattern, expr_str):
                return True

        return False

    def _detect_transformation(self, expr: exp.Expression) -> TransformationType:
        """Detect the type of transformation in expression."""
        if expr is None:
            return TransformationType.DIRECT

        # Check for CASE
        if expr.find(exp.Case):
            return TransformationType.CONDITIONAL

        # Check for aggregate functions
        for func in expr.find_all(exp.AggFunc):
            func_name = func.sql_name().upper() if hasattr(func, "sql_name") else ""
            if func_name in AGGREGATE_FUNCTIONS:
                return TransformationType.AGGREGATE

        # Check for window functions
        if expr.find(exp.Window):
            return TransformationType.WINDOW

        # Check for CAST/type conversion
        if expr.find(exp.Cast):
            return TransformationType.TYPE_CAST

        # Check for arithmetic
        if expr.find(exp.Add) or expr.find(exp.Sub) or \
           expr.find(exp.Mul) or expr.find(exp.Div):
            return TransformationType.CALCULATE

        # Check for string functions
        func_exprs = list(expr.find_all(exp.Func))
        if func_exprs:
            func_names = [f.sql_name().upper() if hasattr(f, "sql_name") else ""
                         for f in func_exprs]
            string_funcs = {"UPPER", "LOWER", "TRIM", "SUBSTR", "SUBSTRING",
                          "CONCAT", "REPLACE", "LPAD", "RPAD", "NVL", "COALESCE"}
            if any(fn in string_funcs for fn in func_names):
                return TransformationType.FORMAT

        return TransformationType.DIRECT

    def _infer_column_name(self, expr: exp.Expression, index: int) -> str:
        """Infer column name from expression."""
        if isinstance(expr, exp.Column):
            return expr.name.upper() if expr.name else f"COL_{index}"

        # For complex expressions, use index
        return f"EXPR_{index}"

    def _resolve_alias(self, alias: str, context: exp.Expression) -> Optional[str]:
        """Try to resolve table alias to actual table name."""
        # This is a simplified version - full implementation would
        # walk the AST to find FROM/JOIN clauses
        return None

    def _get_join_type(self, join: exp.Join) -> str:
        """Get the join type as string."""
        if hasattr(join, "kind"):
            return join.kind.upper() if join.kind else "INNER"
        return "INNER"

    def _split_on_and(self, expr: exp.Expression) -> List[exp.Expression]:
        """Split expression on top-level AND."""
        if isinstance(expr, exp.And):
            result = []
            result.extend(self._split_on_and(expr.left))
            result.extend(self._split_on_and(expr.right))
            return result
        return [expr]

    def _is_join_key(self, cond: exp.Expression) -> bool:
        """Check if condition is a join key (table.col = table.col)."""
        if isinstance(cond, exp.EQ):
            left_cols = list(cond.left.find_all(exp.Column))
            right_cols = list(cond.right.find_all(exp.Column))

            # Both sides should have exactly one column
            if len(left_cols) == 1 and len(right_cols) == 1:
                # Columns should be from different tables
                left_table = left_cols[0].table or ""
                right_table = right_cols[0].table or ""
                if left_table and right_table and left_table != right_table:
                    return True

        return False

    def _dm_match(self, table: str, column: str) -> str:
        """Check if table.column exists in data model."""
        if not table or not column:
            return "N"

        table_upper = table.upper()
        column_upper = column.upper()

        if table_upper in self._dm_columns:
            if column_upper in self._dm_columns[table_upper]:
                return "Y"

        return "N"

    def extract_simple_lineage(self, sql: str, object_name: str,
                               target_table: str) -> LineageData:
        """
        Extract simple lineage (direct column references only).

        This is used for QA comparison against deep lineage.
        """
        result = LineageData(source=object_name)

        # Simple regex extraction of TABLE.COLUMN patterns
        pattern = r'\b([A-Z_][A-Z0-9_]*)\s*\.\s*([A-Z_][A-Z0-9_]*)\b'

        sql_upper = sql.upper()
        matches = re.findall(pattern, sql_upper)

        seen = set()
        for table, column in matches:
            # Skip if single letter (likely alias)
            if len(table) == 1:
                continue

            key = (table, column)
            if key in seen:
                continue
            seen.add(key)

            edge = LineageEdge(
                target_table=target_table,
                target_column="",
                source_table=table,
                source_column=column,
                source_type=SourceType.PHYSICAL,
                usage_type=UsageType.MAPPING,
                usage_role=UsageRole.VALUE,
                object_name=object_name,
                dm_match=self._dm_match(table, column)
            )
            result.add_edge(edge)

        return result

    def set_schema(self, schema: Dict[str, Dict[str, str]]):
        """Update the schema for better resolution."""
        self.schema = schema
        self._dm_columns = {}
        for table, columns in schema.items():
            self._dm_columns[table.upper()] = set(c.upper() for c in columns.keys())
