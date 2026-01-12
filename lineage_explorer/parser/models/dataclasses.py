"""Data classes for lineage structures."""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set
from .enums import SourceType, UsageType, UsageRole, TransformationType, LineageConfidence


@dataclass
class ColumnRef:
    """Reference to a column in a table."""
    table: str
    column: str
    alias: Optional[str] = None
    confidence: LineageConfidence = LineageConfidence.HIGH

    def __str__(self) -> str:
        return f"{self.table}.{self.column}"

    def __hash__(self) -> int:
        return hash((self.table.upper(), self.column.upper()))

    def __eq__(self, other) -> bool:
        if not isinstance(other, ColumnRef):
            return False
        return (self.table.upper() == other.table.upper() and
                self.column.upper() == other.column.upper())


@dataclass
class LineageEdge:
    """A single lineage relationship (source → target)."""
    # Target (destination)
    target_table: str
    target_column: str

    # Source
    source_table: str = ""
    source_column: str = ""

    # Classification
    source_type: SourceType = SourceType.PHYSICAL
    usage_type: UsageType = UsageType.MAPPING
    usage_role: UsageRole = UsageRole.VALUE
    transformation: TransformationType = TransformationType.DIRECT

    # Metadata
    object_name: str = ""
    expression: str = ""
    constant_value: str = ""
    confidence: LineageConfidence = LineageConfidence.HIGH
    dm_match: str = "N"
    trace_path: str = ""
    notes: str = ""

    # Join-specific
    join_alias: str = ""
    join_keys: str = ""
    join_filters: str = ""

    def to_ofsaa_dict(self) -> Dict:
        """Convert to OFSAA format dictionary."""
        return {
            "object_name": self.object_name,
            "destination_table": self.target_table,
            "destination_field": self.target_column,
            "usage_type": self.usage_type.value,
            "usage_role": self.usage_role.value,
            "source_type": self.source_type.value,
            "source_table": self.source_table,
            "source_field": self.source_column,
            "constant_value": self.constant_value,
            "derived_output": self.target_column,
            "derived_expression": self.expression,
            "join_alias": self.join_alias,
            "join_keys": self.join_keys,
            "join_filters": self.join_filters,
            "dm_match": self.dm_match,
            "trace_path": self.trace_path,
            "notes": self.notes,
        }


@dataclass
class TableInfo:
    """Information about a table."""
    name: str
    layer: str = "staging"  # staging, dimension, fact, report
    columns: Set[str] = field(default_factory=set)

    @classmethod
    def detect_layer(cls, table_name: str) -> str:
        """Detect layer from table naming convention."""
        name = table_name.upper()
        if name.startswith(("SRC_", "STG_", "STAGING_")):
            return "staging"
        elif name.startswith(("DIM_", "DIMENSION_")):
            return "dimension"
        elif name.startswith(("FCT_", "FACT_")):
            return "fact"
        elif name.startswith(("RPT_", "REPORT_")):
            return "report"
        else:
            return "staging"


@dataclass
class LineageData:
    """Complete lineage data set."""
    edges: List[LineageEdge] = field(default_factory=list)
    tables: Dict[str, TableInfo] = field(default_factory=dict)
    metadata: Dict = field(default_factory=dict)
    source: str = ""

    def add_edge(self, edge: LineageEdge):
        """Add a lineage edge and update tables."""
        self.edges.append(edge)

        # Track source table
        if edge.source_table and edge.source_table not in self.tables:
            self.tables[edge.source_table] = TableInfo(
                name=edge.source_table,
                layer=TableInfo.detect_layer(edge.source_table)
            )
        if edge.source_table and edge.source_column:
            self.tables[edge.source_table].columns.add(edge.source_column)

        # Track target table
        if edge.target_table and edge.target_table not in self.tables:
            self.tables[edge.target_table] = TableInfo(
                name=edge.target_table,
                layer=TableInfo.detect_layer(edge.target_table)
            )
        if edge.target_table and edge.target_column:
            self.tables[edge.target_table].columns.add(edge.target_column)

    def get_upstream(self, table: str, column: str) -> List[LineageEdge]:
        """Get all edges flowing INTO this column (backward lineage)."""
        return [
            e for e in self.edges
            if e.target_table.upper() == table.upper()
            and e.target_column.upper() == column.upper()
        ]

    def get_downstream(self, table: str, column: str) -> List[LineageEdge]:
        """Get all edges flowing FROM this column (forward lineage / impact)."""
        return [
            e for e in self.edges
            if e.source_table.upper() == table.upper()
            and e.source_column.upper() == column.upper()
        ]
