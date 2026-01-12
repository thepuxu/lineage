#!/usr/bin/env python3
"""
Lineage Explorer Generator

Processes Excel mapping files and generates a static HTML lineage visualization.

Usage:
    python generate_lineage.py \
        --v1 mappings_v1.xlsx \
        --v1-name "January Baseline" \
        --v2 mappings_v2.xlsx \
        --v2-name "February Changes" \
        --data-model data_model.xlsx \
        --output ./output
"""

import argparse
import json
import shutil
from pathlib import Path
from collections import defaultdict
from typing import Optional

import pandas as pd


class LineageProcessor:
    """Processes Excel mappings into lineage data structures."""

    def __init__(
        self,
        v1_path: str,
        v1_name: str,
        v2_path: Optional[str] = None,
        v2_name: Optional[str] = None,
        data_model_path: Optional[str] = None,
    ):
        self.v1_path = Path(v1_path)
        self.v1_name = v1_name
        self.v2_path = Path(v2_path) if v2_path else None
        self.v2_name = v2_name
        self.data_model_path = Path(data_model_path) if data_model_path else None

        # Data structures
        self.v1_mappings = None
        self.v2_mappings = None
        self.data_model = None
        self.tables_metadata = {}
        self.delta = None

    def load_data(self):
        """Load all Excel files."""
        print(f"Loading {self.v1_path}...")
        self.v1_mappings = self._load_mappings(self.v1_path)
        print(f"  Loaded {len(self.v1_mappings)} mappings")

        if self.v2_path and self.v2_path.exists():
            print(f"Loading {self.v2_path}...")
            self.v2_mappings = self._load_mappings(self.v2_path)
            print(f"  Loaded {len(self.v2_mappings)} mappings")

        if self.data_model_path and self.data_model_path.exists():
            print(f"Loading {self.data_model_path}...")
            self.data_model = self._load_data_model(self.data_model_path)
            print(f"  Loaded {len(self.data_model)} columns")

    def _load_mappings(self, path: Path) -> pd.DataFrame:
        """Load mappings Excel file."""
        df = pd.read_excel(path)
        # Normalize column names
        df.columns = [c.strip() for c in df.columns]
        # Expected columns (will be mapped to internal names)
        column_map = {
            "Source Table": "source_table",
            "Source Field": "source_field",
            "Dest Table": "dest_table",
            "Dest Field": "dest_field",
            "Destination Table": "dest_table",
            "Destination Field": "dest_field",
            "destination_table": "dest_table",
            "destination_field": "dest_field",
            "Rules": "rules",
            "Mapping Type": "mapping_type",
        }
        df = df.rename(columns=column_map)
        return df

    def _load_data_model(self, path: Path) -> pd.DataFrame:
        """Load data model Excel file."""
        df = pd.read_excel(path)
        df.columns = [c.strip() for c in df.columns]
        column_map = {
            "Table Name": "table_name",
            "Column Name": "column_name",
            "Data Type": "data_type",
            "Is PK": "is_pk",
            "Is FK": "is_fk",
            "Description": "description",
        }
        df = df.rename(columns=column_map)
        return df

    def compute_delta(self):
        """Compute differences between v1 and v2."""
        if self.v2_mappings is None:
            return

        print("Computing delta...")

        # Create mapping keys
        def make_key(row):
            return (
                row["source_table"],
                row["source_field"],
                row["dest_table"],
                row["dest_field"],
            )

        v1_dict = {}
        for _, row in self.v1_mappings.iterrows():
            key = make_key(row)
            v1_dict[key] = {
                "rules": row.get("rules", ""),
                "mapping_type": row.get("mapping_type", ""),
            }

        v2_dict = {}
        for _, row in self.v2_mappings.iterrows():
            key = make_key(row)
            v2_dict[key] = {
                "rules": row.get("rules", ""),
                "mapping_type": row.get("mapping_type", ""),
            }

        # Compute delta
        added = []
        removed = []
        modified = []
        unchanged = []

        all_keys = set(v1_dict.keys()) | set(v2_dict.keys())

        for key in all_keys:
            in_v1 = key in v1_dict
            in_v2 = key in v2_dict

            mapping_info = {
                "source_table": key[0],
                "source_field": key[1],
                "dest_table": key[2],
                "dest_field": key[3],
            }

            if in_v1 and not in_v2:
                mapping_info["v1_rules"] = v1_dict[key]["rules"]
                mapping_info["v1_mapping_type"] = v1_dict[key]["mapping_type"]
                removed.append(mapping_info)
            elif in_v2 and not in_v1:
                mapping_info["v2_rules"] = v2_dict[key]["rules"]
                mapping_info["v2_mapping_type"] = v2_dict[key]["mapping_type"]
                added.append(mapping_info)
            else:
                # In both - check if changed
                v1_data = v1_dict[key]
                v2_data = v2_dict[key]

                mapping_info["v1_rules"] = v1_data["rules"]
                mapping_info["v2_rules"] = v2_data["rules"]
                mapping_info["v1_mapping_type"] = v1_data["mapping_type"]
                mapping_info["v2_mapping_type"] = v2_data["mapping_type"]

                if (
                    str(v1_data["rules"]) != str(v2_data["rules"])
                    or str(v1_data["mapping_type"]) != str(v2_data["mapping_type"])
                ):
                    modified.append(mapping_info)
                else:
                    unchanged.append(mapping_info)

        self.delta = {
            "added": added,
            "removed": removed,
            "modified": modified,
            "unchanged": unchanged,
            "summary": {
                "added_count": len(added),
                "removed_count": len(removed),
                "modified_count": len(modified),
                "unchanged_count": len(unchanged),
            },
        }

        print(f"  Added: {len(added)}, Removed: {len(removed)}, Modified: {len(modified)}, Unchanged: {len(unchanged)}")

    def build_table_metadata(self):
        """Build metadata for all tables."""
        print("Building table metadata...")

        # Collect all tables from mappings
        tables = set()
        for df in [self.v1_mappings, self.v2_mappings]:
            if df is not None:
                tables.update(df["source_table"].unique())
                tables.update(df["dest_table"].unique())

        # Build metadata per table
        for table in tables:
            self.tables_metadata[table] = {
                "name": table,
                "columns": [],
                "upstream_tables": set(),
                "downstream_tables": set(),
            }

        # Add columns from data model
        if self.data_model is not None:
            for _, row in self.data_model.iterrows():
                table = row["table_name"]
                if table in self.tables_metadata:
                    self.tables_metadata[table]["columns"].append({
                        "name": row["column_name"],
                        "data_type": row.get("data_type", ""),
                        "is_pk": bool(row.get("is_pk", False)),
                        "is_fk": bool(row.get("is_fk", False)),
                        "description": row.get("description", ""),
                    })

        # Build relationships from v2 (or v1 if no v2)
        df = self.v2_mappings if self.v2_mappings is not None else self.v1_mappings

        for _, row in df.iterrows():
            src_table = row["source_table"]
            dst_table = row["dest_table"]

            if src_table in self.tables_metadata:
                self.tables_metadata[src_table]["downstream_tables"].add(dst_table)
            if dst_table in self.tables_metadata:
                self.tables_metadata[dst_table]["upstream_tables"].add(src_table)

        # Convert sets to lists for JSON serialization
        for table in self.tables_metadata:
            self.tables_metadata[table]["upstream_tables"] = list(
                self.tables_metadata[table]["upstream_tables"]
            )
            self.tables_metadata[table]["downstream_tables"] = list(
                self.tables_metadata[table]["downstream_tables"]
            )

        print(f"  Built metadata for {len(self.tables_metadata)} tables")

    def build_lineage_graph(self, mappings_df: pd.DataFrame, version: str) -> dict:
        """Build lineage graph structure for a version."""

        # Build table-level graph
        table_edges = defaultdict(lambda: {"count": 0, "mappings": []})

        for _, row in mappings_df.iterrows():
            edge_key = (row["source_table"], row["dest_table"])
            table_edges[edge_key]["count"] += 1
            table_edges[edge_key]["mappings"].append({
                "source_field": row["source_field"],
                "dest_field": row["dest_field"],
                "rules": str(row.get("rules", "")),
                "mapping_type": str(row.get("mapping_type", "")),
            })

        # Build nodes
        nodes = {}
        for table in self.tables_metadata:
            nodes[table] = {
                "id": table,
                "label": table,
                "columns": self.tables_metadata[table]["columns"],
                "upstream_count": len(self.tables_metadata[table]["upstream_tables"]),
                "downstream_count": len(self.tables_metadata[table]["downstream_tables"]),
            }

        # Build edges
        edges = []
        for (src, dst), data in table_edges.items():
            edges.append({
                "source": src,
                "target": dst,
                "mapping_count": data["count"],
                "mappings": data["mappings"],
            })

        return {
            "version": version,
            "nodes": nodes,
            "edges": edges,
        }

    def compute_table_lineage(self, table_name: str, direction: str = "both", max_depth: int = 5) -> dict:
        """Compute upstream/downstream lineage for a specific table."""

        df = self.v2_mappings if self.v2_mappings is not None else self.v1_mappings

        # Build adjacency lists
        upstream = defaultdict(set)  # table -> tables that feed into it
        downstream = defaultdict(set)  # table -> tables it feeds into

        for _, row in df.iterrows():
            src = row["source_table"]
            dst = row["dest_table"]
            upstream[dst].add(src)
            downstream[src].add(dst)

        def traverse(start: str, adj: dict, max_d: int) -> dict:
            """BFS traversal."""
            result = {"nodes": {start}, "edges": []}
            queue = [(start, 0)]
            visited = {start}

            while queue:
                current, depth = queue.pop(0)
                if depth >= max_d:
                    continue

                for neighbor in adj.get(current, []):
                    if neighbor not in visited:
                        visited.add(neighbor)
                        result["nodes"].add(neighbor)
                        queue.append((neighbor, depth + 1))
                    result["edges"].append((current, neighbor) if adj == downstream else (neighbor, current))

            return result

        result = {"center": table_name, "nodes": {table_name}, "edges": []}

        if direction in ["upstream", "both"]:
            up = traverse(table_name, upstream, max_depth)
            result["nodes"].update(up["nodes"])
            result["edges"].extend(up["edges"])

        if direction in ["downstream", "both"]:
            down = traverse(table_name, downstream, max_depth)
            result["nodes"].update(down["nodes"])
            result["edges"].extend(down["edges"])

        # Deduplicate edges
        result["edges"] = list(set(result["edges"]))
        result["nodes"] = list(result["nodes"])

        return result

    def generate_output(self, output_dir: Path):
        """Generate all output files."""
        output_dir = Path(output_dir)
        data_dir = output_dir / "data"
        assets_dir = output_dir / "assets"

        # Create directories
        data_dir.mkdir(parents=True, exist_ok=True)
        assets_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "lineage" / "v1").mkdir(parents=True, exist_ok=True)
        if self.v2_mappings is not None:
            (data_dir / "lineage" / "v2").mkdir(parents=True, exist_ok=True)

        print(f"\nGenerating output to {output_dir}...")

        # 1. Tables metadata
        tables_file = data_dir / "tables.json"
        with open(tables_file, "w") as f:
            json.dump(self.tables_metadata, f, indent=2)
        print(f"  Created: {tables_file}")

        # 2. Version 1 graph
        v1_graph = self.build_lineage_graph(self.v1_mappings, self.v1_name)
        v1_file = data_dir / "v1_graph.json"
        with open(v1_file, "w") as f:
            json.dump(v1_graph, f, indent=2)
        print(f"  Created: {v1_file}")

        # 3. Version 2 graph (if exists)
        if self.v2_mappings is not None:
            v2_graph = self.build_lineage_graph(self.v2_mappings, self.v2_name)
            v2_file = data_dir / "v2_graph.json"
            with open(v2_file, "w") as f:
                json.dump(v2_graph, f, indent=2)
            print(f"  Created: {v2_file}")

        # 4. Delta
        if self.delta:
            delta_file = data_dir / "delta.json"
            with open(delta_file, "w") as f:
                json.dump(self.delta, f, indent=2)
            print(f"  Created: {delta_file}")

        # 5. Pre-compute lineage for each table
        all_tables = list(self.tables_metadata.keys())
        for table in all_tables:
            lineage = self.compute_table_lineage(table)
            # V1 lineage
            lineage_file = data_dir / "lineage" / "v1" / f"{table}.json"
            with open(lineage_file, "w") as f:
                json.dump(lineage, f, indent=2)

            if self.v2_mappings is not None:
                lineage_file = data_dir / "lineage" / "v2" / f"{table}.json"
                with open(lineage_file, "w") as f:
                    json.dump(lineage, f, indent=2)

        print(f"  Created lineage files for {len(all_tables)} tables")

        # 6. Config file
        config = {
            "v1_name": self.v1_name,
            "v2_name": self.v2_name if self.v2_name else None,
            "has_v2": self.v2_mappings is not None,
            "has_delta": self.delta is not None,
            "table_count": len(self.tables_metadata),
            "v1_mapping_count": len(self.v1_mappings),
            "v2_mapping_count": len(self.v2_mappings) if self.v2_mappings is not None else 0,
        }
        config_file = data_dir / "config.json"
        with open(config_file, "w") as f:
            json.dump(config, f, indent=2)
        print(f"  Created: {config_file}")

        # 7. Copy/generate HTML and assets
        self._generate_html(output_dir)
        self._generate_js(assets_dir)
        self._generate_css(assets_dir)

        print(f"\nDone! Open {output_dir / 'index.html'} in a browser.")

    def _generate_html(self, output_dir: Path):
        """Generate the main HTML file."""
        html_content = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Lineage Explorer</title>
    <link rel="stylesheet" href="assets/style.css">
</head>
<body>
    <div class="app-container">
        <!-- Header -->
        <header class="header">
            <h1>Lineage Explorer</h1>
            <div class="tabs" id="version-tabs">
                <button class="tab active" data-version="v1">Version 1</button>
                <button class="tab" data-version="v2">Version 2</button>
                <button class="tab" data-version="delta">Delta</button>
            </div>
        </header>

        <!-- Main Content -->
        <div class="main-content">
            <!-- Left Sidebar: Search & Table List -->
            <aside class="sidebar">
                <div class="search-container">
                    <input type="text" id="search-input" placeholder="Search tables...">
                </div>
                <div class="table-list" id="table-list">
                    <!-- Populated by JS -->
                </div>
            </aside>

            <!-- Center: Graph Visualization -->
            <main class="graph-container">
                <div class="graph-controls">
                    <button id="btn-zoom-in" title="Zoom In">+</button>
                    <button id="btn-zoom-out" title="Zoom Out">-</button>
                    <button id="btn-reset" title="Reset View">Reset</button>
                    <span class="separator"></span>
                    <label><input type="checkbox" id="show-upstream" checked> Upstream</label>
                    <label><input type="checkbox" id="show-downstream" checked> Downstream</label>
                </div>
                <div class="delta-filters" id="delta-filters" style="display: none;">
                    <label><input type="checkbox" id="filter-added" checked> <span class="badge added">Added</span></label>
                    <label><input type="checkbox" id="filter-removed" checked> <span class="badge removed">Removed</span></label>
                    <label><input type="checkbox" id="filter-modified" checked> <span class="badge modified">Modified</span></label>
                    <label><input type="checkbox" id="filter-unchanged"> <span class="badge unchanged">Unchanged</span></label>
                </div>
                <div class="delta-summary" id="delta-summary" style="display: none;">
                    <!-- Populated by JS -->
                </div>
                <svg id="graph-svg"></svg>
            </main>

            <!-- Right Sidebar: Details Panel -->
            <aside class="details-panel" id="details-panel">
                <div class="panel-header">
                    <h3>Details</h3>
                    <button id="close-panel">&times;</button>
                </div>
                <div class="panel-content" id="panel-content">
                    <p class="placeholder">Select a table or edge to view details</p>
                </div>
            </aside>
        </div>
    </div>

    <!-- D3.js (bundled) -->
    <script src="assets/d3.min.js"></script>
    <script src="assets/app.js"></script>
</body>
</html>
'''
        html_file = output_dir / "index.html"
        with open(html_file, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"  Created: {html_file}")

    def _generate_css(self, assets_dir: Path):
        """Generate the CSS file."""
        css_content = '''/* Lineage Explorer Styles */

:root {
    --bg-primary: #1a1a2e;
    --bg-secondary: #16213e;
    --bg-tertiary: #0f3460;
    --text-primary: #eee;
    --text-secondary: #aaa;
    --accent: #00d9ff;
    --accent-hover: #00b8d9;
    --success: #4caf50;
    --danger: #f44336;
    --warning: #ff9800;
    --muted: #666;
    --border: #333;
}

* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
    background: var(--bg-primary);
    color: var(--text-primary);
    height: 100vh;
    overflow: hidden;
}

.app-container {
    display: flex;
    flex-direction: column;
    height: 100vh;
}

/* Header */
.header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 12px 20px;
    background: var(--bg-secondary);
    border-bottom: 1px solid var(--border);
}

.header h1 {
    font-size: 1.25rem;
    font-weight: 600;
    color: var(--accent);
}

.tabs {
    display: flex;
    gap: 4px;
}

.tab {
    padding: 8px 16px;
    background: transparent;
    border: 1px solid var(--border);
    border-radius: 4px;
    color: var(--text-secondary);
    cursor: pointer;
    transition: all 0.2s;
}

.tab:hover {
    background: var(--bg-tertiary);
    color: var(--text-primary);
}

.tab.active {
    background: var(--accent);
    border-color: var(--accent);
    color: var(--bg-primary);
}

/* Main Content */
.main-content {
    display: flex;
    flex: 1;
    overflow: hidden;
}

/* Sidebar */
.sidebar {
    width: 250px;
    background: var(--bg-secondary);
    border-right: 1px solid var(--border);
    display: flex;
    flex-direction: column;
}

.search-container {
    padding: 12px;
    border-bottom: 1px solid var(--border);
}

#search-input {
    width: 100%;
    padding: 8px 12px;
    background: var(--bg-tertiary);
    border: 1px solid var(--border);
    border-radius: 4px;
    color: var(--text-primary);
    font-size: 0.875rem;
}

#search-input:focus {
    outline: none;
    border-color: var(--accent);
}

#search-input::placeholder {
    color: var(--text-secondary);
}

.table-list {
    flex: 1;
    overflow-y: auto;
    padding: 8px;
}

.table-item {
    padding: 8px 12px;
    border-radius: 4px;
    cursor: pointer;
    font-size: 0.875rem;
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 2px;
}

.table-item:hover {
    background: var(--bg-tertiary);
}

.table-item.selected {
    background: var(--accent);
    color: var(--bg-primary);
}

.table-item .layer-badge {
    font-size: 0.625rem;
    padding: 2px 6px;
    border-radius: 3px;
    background: var(--bg-tertiary);
    color: var(--text-secondary);
}

.table-item.selected .layer-badge {
    background: rgba(0,0,0,0.2);
    color: var(--bg-primary);
}

/* Graph Container */
.graph-container {
    flex: 1;
    display: flex;
    flex-direction: column;
    position: relative;
}

.graph-controls {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 8px 12px;
    background: var(--bg-secondary);
    border-bottom: 1px solid var(--border);
}

.graph-controls button {
    width: 32px;
    height: 32px;
    background: var(--bg-tertiary);
    border: 1px solid var(--border);
    border-radius: 4px;
    color: var(--text-primary);
    cursor: pointer;
    font-size: 1rem;
}

.graph-controls button:hover {
    background: var(--accent);
    color: var(--bg-primary);
}

.graph-controls .separator {
    width: 1px;
    height: 24px;
    background: var(--border);
    margin: 0 8px;
}

.graph-controls label {
    display: flex;
    align-items: center;
    gap: 4px;
    font-size: 0.875rem;
    color: var(--text-secondary);
    cursor: pointer;
}

.graph-controls input[type="checkbox"] {
    accent-color: var(--accent);
}

.delta-filters {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 8px 12px;
    background: var(--bg-tertiary);
    border-bottom: 1px solid var(--border);
}

.delta-filters label {
    display: flex;
    align-items: center;
    gap: 4px;
    font-size: 0.875rem;
    cursor: pointer;
}

.badge {
    padding: 2px 8px;
    border-radius: 3px;
    font-size: 0.75rem;
    font-weight: 500;
}

.badge.added {
    background: var(--success);
    color: white;
}

.badge.removed {
    background: var(--danger);
    color: white;
}

.badge.modified {
    background: var(--warning);
    color: black;
}

.badge.unchanged {
    background: var(--muted);
    color: white;
}

.delta-summary {
    padding: 8px 12px;
    background: var(--bg-secondary);
    font-size: 0.875rem;
    color: var(--text-secondary);
    border-bottom: 1px solid var(--border);
}

.delta-summary span {
    margin-right: 16px;
}

#graph-svg {
    flex: 1;
    background: var(--bg-primary);
}

/* Graph Elements */
.node {
    cursor: pointer;
}

.node rect {
    fill: var(--bg-tertiary);
    stroke: var(--border);
    stroke-width: 1px;
    rx: 4;
    ry: 4;
    transition: all 0.2s;
}

.node:hover rect {
    stroke: var(--accent);
    stroke-width: 2px;
}

.node.selected rect {
    stroke: var(--accent);
    stroke-width: 2px;
    fill: var(--bg-secondary);
}

.node text {
    fill: var(--text-primary);
    font-size: 12px;
    pointer-events: none;
}

.node .node-label {
    font-weight: 500;
}

.node .node-count {
    fill: var(--text-secondary);
    font-size: 10px;
}

.link {
    fill: none;
    stroke: var(--muted);
    stroke-width: 1.5px;
    opacity: 0.6;
}

.link:hover {
    stroke: var(--accent);
    stroke-width: 2px;
    opacity: 1;
}

.link.added {
    stroke: var(--success);
    stroke-width: 2px;
    opacity: 0.8;
}

.link.removed {
    stroke: var(--danger);
    stroke-width: 2px;
    stroke-dasharray: 5,5;
    opacity: 0.8;
}

.link.modified {
    stroke: var(--warning);
    stroke-width: 2px;
    opacity: 0.8;
}

.link-arrow {
    fill: var(--muted);
}

/* Details Panel */
.details-panel {
    width: 320px;
    background: var(--bg-secondary);
    border-left: 1px solid var(--border);
    display: flex;
    flex-direction: column;
    transform: translateX(100%);
    transition: transform 0.3s ease;
}

.details-panel.open {
    transform: translateX(0);
}

.panel-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 12px 16px;
    border-bottom: 1px solid var(--border);
}

.panel-header h3 {
    font-size: 1rem;
    font-weight: 600;
}

#close-panel {
    background: none;
    border: none;
    color: var(--text-secondary);
    font-size: 1.5rem;
    cursor: pointer;
    line-height: 1;
}

#close-panel:hover {
    color: var(--text-primary);
}

.panel-content {
    flex: 1;
    overflow-y: auto;
    padding: 16px;
}

.panel-content .placeholder {
    color: var(--text-secondary);
    font-style: italic;
}

.panel-section {
    margin-bottom: 16px;
}

.panel-section h4 {
    font-size: 0.75rem;
    text-transform: uppercase;
    color: var(--text-secondary);
    margin-bottom: 8px;
    letter-spacing: 0.5px;
}

.column-list {
    max-height: 200px;
    overflow-y: auto;
}

.column-item {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 4px 0;
    font-size: 0.875rem;
    border-bottom: 1px solid var(--border);
}

.column-item:last-child {
    border-bottom: none;
}

.column-item .col-name {
    flex: 1;
}

.column-item .col-type {
    color: var(--text-secondary);
    font-size: 0.75rem;
}

.column-item .col-badge {
    font-size: 0.625rem;
    padding: 1px 4px;
    border-radius: 2px;
    background: var(--accent);
    color: var(--bg-primary);
}

.mapping-list {
    max-height: 300px;
    overflow-y: auto;
}

.mapping-item {
    padding: 8px;
    background: var(--bg-tertiary);
    border-radius: 4px;
    margin-bottom: 8px;
    font-size: 0.875rem;
}

.mapping-item .mapping-fields {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 4px;
}

.mapping-item .mapping-arrow {
    color: var(--accent);
}

.mapping-item .mapping-rule {
    font-size: 0.75rem;
    color: var(--text-secondary);
    font-family: monospace;
    background: var(--bg-primary);
    padding: 4px 8px;
    border-radius: 2px;
    margin-top: 4px;
    word-break: break-all;
}

.mapping-item.added {
    border-left: 3px solid var(--success);
}

.mapping-item.removed {
    border-left: 3px solid var(--danger);
}

.mapping-item.modified {
    border-left: 3px solid var(--warning);
}

/* Scrollbar */
::-webkit-scrollbar {
    width: 8px;
    height: 8px;
}

::-webkit-scrollbar-track {
    background: var(--bg-primary);
}

::-webkit-scrollbar-thumb {
    background: var(--border);
    border-radius: 4px;
}

::-webkit-scrollbar-thumb:hover {
    background: var(--muted);
}
'''
        css_file = assets_dir / "style.css"
        with open(css_file, "w", encoding="utf-8") as f:
            f.write(css_content)
        print(f"  Created: {css_file}")

    def _generate_js(self, assets_dir: Path):
        """Generate the main JavaScript file."""
        js_content = '''// Lineage Explorer Application

class LineageExplorer {
    constructor() {
        this.config = null;
        this.tables = null;
        this.v1Graph = null;
        this.v2Graph = null;
        this.delta = null;
        this.currentVersion = 'v1';
        this.selectedTable = null;
        this.svg = null;
        this.g = null;
        this.zoom = null;
        this.simulation = null;

        this.init();
    }

    async init() {
        await this.loadData();
        this.setupUI();
        this.renderTableList();
        this.setupGraph();
        this.renderGraph();
    }

    async loadData() {
        try {
            const [config, tables, v1Graph] = await Promise.all([
                fetch('data/config.json').then(r => r.json()),
                fetch('data/tables.json').then(r => r.json()),
                fetch('data/v1_graph.json').then(r => r.json()),
            ]);

            this.config = config;
            this.tables = tables;
            this.v1Graph = v1Graph;

            if (config.has_v2) {
                this.v2Graph = await fetch('data/v2_graph.json').then(r => r.json());
            }

            if (config.has_delta) {
                this.delta = await fetch('data/delta.json').then(r => r.json());
            }

            // Update tab labels
            document.querySelector('[data-version="v1"]').textContent = config.v1_name || 'Version 1';
            if (config.v2_name) {
                document.querySelector('[data-version="v2"]').textContent = config.v2_name;
            }

            // Hide v2/delta tabs if not available
            if (!config.has_v2) {
                document.querySelector('[data-version="v2"]').style.display = 'none';
                document.querySelector('[data-version="delta"]').style.display = 'none';
            }

        } catch (error) {
            console.error('Failed to load data:', error);
        }
    }

    setupUI() {
        // Tab switching
        document.querySelectorAll('.tab').forEach(tab => {
            tab.addEventListener('click', () => {
                document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
                tab.classList.add('active');
                this.currentVersion = tab.dataset.version;

                // Show/hide delta filters
                const deltaFilters = document.getElementById('delta-filters');
                const deltaSummary = document.getElementById('delta-summary');
                if (this.currentVersion === 'delta') {
                    deltaFilters.style.display = 'flex';
                    deltaSummary.style.display = 'block';
                    this.renderDeltaSummary();
                } else {
                    deltaFilters.style.display = 'none';
                    deltaSummary.style.display = 'none';
                }

                this.renderGraph();
            });
        });

        // Search
        document.getElementById('search-input').addEventListener('input', (e) => {
            this.filterTableList(e.target.value);
        });

        // Zoom controls
        document.getElementById('btn-zoom-in').addEventListener('click', () => {
            this.svg.transition().call(this.zoom.scaleBy, 1.3);
        });

        document.getElementById('btn-zoom-out').addEventListener('click', () => {
            this.svg.transition().call(this.zoom.scaleBy, 0.7);
        });

        document.getElementById('btn-reset').addEventListener('click', () => {
            this.svg.transition().call(this.zoom.transform, d3.zoomIdentity);
        });

        // Direction checkboxes
        document.getElementById('show-upstream').addEventListener('change', () => this.renderGraph());
        document.getElementById('show-downstream').addEventListener('change', () => this.renderGraph());

        // Delta filters
        ['filter-added', 'filter-removed', 'filter-modified', 'filter-unchanged'].forEach(id => {
            document.getElementById(id).addEventListener('change', () => this.renderGraph());
        });

        // Close panel
        document.getElementById('close-panel').addEventListener('click', () => {
            document.getElementById('details-panel').classList.remove('open');
        });
    }

    renderTableList() {
        const container = document.getElementById('table-list');
        const tableNames = Object.keys(this.tables).sort();

        container.innerHTML = tableNames.map(name => {
            const layer = this.getTableLayer(name);
            return `
                <div class="table-item" data-table="${name}">
                    <span class="layer-badge">${layer}</span>
                    <span>${name}</span>
                </div>
            `;
        }).join('');

        container.querySelectorAll('.table-item').forEach(item => {
            item.addEventListener('click', () => {
                this.selectTable(item.dataset.table);
            });
        });
    }

    getTableLayer(name) {
        if (name.startsWith('src_')) return 'SRC';
        if (name.startsWith('stg_')) return 'STG';
        if (name.startsWith('dim_')) return 'DIM';
        if (name.startsWith('fact_')) return 'FACT';
        if (name.startsWith('rpt_')) return 'RPT';
        return 'OTH';
    }

    filterTableList(query) {
        const items = document.querySelectorAll('.table-item');
        const q = query.toLowerCase();

        items.forEach(item => {
            const name = item.dataset.table.toLowerCase();
            item.style.display = name.includes(q) ? 'flex' : 'none';
        });
    }

    selectTable(tableName) {
        this.selectedTable = tableName;

        // Update UI
        document.querySelectorAll('.table-item').forEach(item => {
            item.classList.toggle('selected', item.dataset.table === tableName);
        });

        this.renderGraph();
        this.showTableDetails(tableName);
    }

    setupGraph() {
        const container = document.querySelector('.graph-container');
        const svg = d3.select('#graph-svg');

        const width = container.clientWidth;
        const height = container.clientHeight - 50; // Account for controls

        svg.attr('width', width).attr('height', height);

        this.svg = svg;
        this.g = svg.append('g');

        // Zoom behavior
        this.zoom = d3.zoom()
            .scaleExtent([0.1, 4])
            .on('zoom', (event) => {
                this.g.attr('transform', event.transform);
            });

        svg.call(this.zoom);

        // Arrow marker
        svg.append('defs').append('marker')
            .attr('id', 'arrow')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 20)
            .attr('refY', 0)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .append('path')
            .attr('d', 'M0,-5L10,0L0,5')
            .attr('class', 'link-arrow');
    }

    renderGraph() {
        const graph = this.currentVersion === 'v1' ? this.v1Graph :
                      this.currentVersion === 'v2' ? this.v2Graph :
                      this.buildDeltaGraph();

        if (!graph) return;

        // Clear previous
        this.g.selectAll('*').remove();

        // Filter based on selected table
        let nodes, edges;

        if (this.selectedTable) {
            const showUpstream = document.getElementById('show-upstream').checked;
            const showDownstream = document.getElementById('show-downstream').checked;

            const relevantTables = new Set([this.selectedTable]);
            const relevantEdges = [];

            // Find connected tables
            graph.edges.forEach(edge => {
                if (edge.source === this.selectedTable && showDownstream) {
                    relevantTables.add(edge.target);
                    relevantEdges.push(edge);
                }
                if (edge.target === this.selectedTable && showUpstream) {
                    relevantTables.add(edge.source);
                    relevantEdges.push(edge);
                }
            });

            // Add second-level connections
            graph.edges.forEach(edge => {
                if (relevantTables.has(edge.source) && showDownstream) {
                    relevantTables.add(edge.target);
                    relevantEdges.push(edge);
                }
                if (relevantTables.has(edge.target) && showUpstream) {
                    relevantTables.add(edge.source);
                    relevantEdges.push(edge);
                }
            });

            nodes = Object.values(graph.nodes).filter(n => relevantTables.has(n.id));
            edges = relevantEdges;
        } else {
            nodes = Object.values(graph.nodes);
            edges = graph.edges;
        }

        // Apply delta filters if in delta view
        if (this.currentVersion === 'delta') {
            const showAdded = document.getElementById('filter-added').checked;
            const showRemoved = document.getElementById('filter-removed').checked;
            const showModified = document.getElementById('filter-modified').checked;
            const showUnchanged = document.getElementById('filter-unchanged').checked;

            edges = edges.filter(e => {
                if (e.changeType === 'added') return showAdded;
                if (e.changeType === 'removed') return showRemoved;
                if (e.changeType === 'modified') return showModified;
                if (e.changeType === 'unchanged') return showUnchanged;
                return true;
            });
        }

        // Deduplicate edges
        const edgeMap = new Map();
        edges.forEach(e => {
            const key = `${e.source}->${e.target}`;
            if (!edgeMap.has(key)) {
                edgeMap.set(key, e);
            }
        });
        edges = Array.from(edgeMap.values());

        // Layout using force simulation
        const width = this.svg.attr('width');
        const height = this.svg.attr('height');

        // Create simulation
        this.simulation = d3.forceSimulation(nodes)
            .force('link', d3.forceLink(edges).id(d => d.id).distance(150))
            .force('charge', d3.forceManyBody().strength(-500))
            .force('center', d3.forceCenter(width / 2, height / 2))
            .force('collision', d3.forceCollide().radius(80));

        // Draw edges
        const link = this.g.append('g')
            .selectAll('path')
            .data(edges)
            .enter()
            .append('path')
            .attr('class', d => `link ${d.changeType || ''}`)
            .attr('marker-end', 'url(#arrow)')
            .on('click', (event, d) => this.showEdgeDetails(d));

        // Draw nodes
        const node = this.g.append('g')
            .selectAll('g')
            .data(nodes)
            .enter()
            .append('g')
            .attr('class', d => `node ${d.id === this.selectedTable ? 'selected' : ''}`)
            .call(d3.drag()
                .on('start', (event, d) => {
                    if (!event.active) this.simulation.alphaTarget(0.3).restart();
                    d.fx = d.x;
                    d.fy = d.y;
                })
                .on('drag', (event, d) => {
                    d.fx = event.x;
                    d.fy = event.y;
                })
                .on('end', (event, d) => {
                    if (!event.active) this.simulation.alphaTarget(0);
                    d.fx = null;
                    d.fy = null;
                }))
            .on('click', (event, d) => this.selectTable(d.id));

        node.append('rect')
            .attr('width', 140)
            .attr('height', 40)
            .attr('x', -70)
            .attr('y', -20);

        node.append('text')
            .attr('class', 'node-label')
            .attr('text-anchor', 'middle')
            .attr('dy', -2)
            .text(d => d.label.length > 18 ? d.label.substring(0, 16) + '...' : d.label);

        node.append('text')
            .attr('class', 'node-count')
            .attr('text-anchor', 'middle')
            .attr('dy', 12)
            .text(d => `${d.columns?.length || 0} columns`);

        // Update positions
        this.simulation.on('tick', () => {
            link.attr('d', d => {
                const dx = d.target.x - d.source.x;
                const dy = d.target.y - d.source.y;
                return `M${d.source.x},${d.source.y}L${d.target.x},${d.target.y}`;
            });

            node.attr('transform', d => `translate(${d.x},${d.y})`);
        });
    }

    buildDeltaGraph() {
        if (!this.delta || !this.v2Graph) return this.v1Graph;

        const graph = JSON.parse(JSON.stringify(this.v2Graph));

        // Mark edges with change type
        const v1EdgeKeys = new Set(this.v1Graph.edges.map(e => `${e.source}->${e.target}`));
        const v2EdgeKeys = new Set(this.v2Graph.edges.map(e => `${e.source}->${e.target}`));

        // Build edge change map from delta
        const edgeChanges = new Map();

        this.delta.added.forEach(m => {
            const key = `${m.source_table}->${m.dest_table}`;
            edgeChanges.set(key, 'added');
        });

        this.delta.removed.forEach(m => {
            const key = `${m.source_table}->${m.dest_table}`;
            if (!edgeChanges.has(key) || edgeChanges.get(key) !== 'added') {
                edgeChanges.set(key, 'removed');
            }
        });

        this.delta.modified.forEach(m => {
            const key = `${m.source_table}->${m.dest_table}`;
            if (!edgeChanges.has(key)) {
                edgeChanges.set(key, 'modified');
            }
        });

        // Apply to edges
        graph.edges.forEach(edge => {
            const key = `${edge.source}->${edge.target}`;
            edge.changeType = edgeChanges.get(key) || 'unchanged';
        });

        // Add removed edges from v1 that aren't in v2
        this.v1Graph.edges.forEach(edge => {
            const key = `${edge.source}->${edge.target}`;
            if (!v2EdgeKeys.has(key)) {
                graph.edges.push({
                    ...edge,
                    changeType: 'removed'
                });

                // Ensure nodes exist
                if (!graph.nodes[edge.source]) {
                    graph.nodes[edge.source] = this.v1Graph.nodes[edge.source];
                }
                if (!graph.nodes[edge.target]) {
                    graph.nodes[edge.target] = this.v1Graph.nodes[edge.target];
                }
            }
        });

        return graph;
    }

    renderDeltaSummary() {
        if (!this.delta) return;

        const container = document.getElementById('delta-summary');
        const s = this.delta.summary;

        container.innerHTML = `
            <span><strong>Added:</strong> ${s.added_count}</span>
            <span><strong>Removed:</strong> ${s.removed_count}</span>
            <span><strong>Modified:</strong> ${s.modified_count}</span>
            <span><strong>Unchanged:</strong> ${s.unchanged_count}</span>
        `;
    }

    showTableDetails(tableName) {
        const panel = document.getElementById('details-panel');
        const content = document.getElementById('panel-content');
        const table = this.tables[tableName];

        if (!table) return;

        const columns = table.columns || [];

        content.innerHTML = `
            <div class="panel-section">
                <h4>Table: ${tableName}</h4>
            </div>

            <div class="panel-section">
                <h4>Connections</h4>
                <p>Upstream: ${table.upstream_tables?.length || 0} tables</p>
                <p>Downstream: ${table.downstream_tables?.length || 0} tables</p>
            </div>

            <div class="panel-section">
                <h4>Columns (${columns.length})</h4>
                <div class="column-list">
                    ${columns.map(col => `
                        <div class="column-item">
                            <span class="col-name">${col.name}</span>
                            ${col.is_pk ? '<span class="col-badge">PK</span>' : ''}
                            ${col.is_fk ? '<span class="col-badge">FK</span>' : ''}
                            <span class="col-type">${col.data_type || ''}</span>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;

        panel.classList.add('open');
    }

    showEdgeDetails(edge) {
        const panel = document.getElementById('details-panel');
        const content = document.getElementById('panel-content');

        const mappings = edge.mappings || [];

        content.innerHTML = `
            <div class="panel-section">
                <h4>Edge: ${edge.source} → ${edge.target}</h4>
                ${edge.changeType ? `<span class="badge ${edge.changeType}">${edge.changeType}</span>` : ''}
            </div>

            <div class="panel-section">
                <h4>Field Mappings (${mappings.length})</h4>
                <div class="mapping-list">
                    ${mappings.map(m => `
                        <div class="mapping-item ${m.changeType || ''}">
                            <div class="mapping-fields">
                                <span>${m.source_field}</span>
                                <span class="mapping-arrow">→</span>
                                <span>${m.dest_field}</span>
                            </div>
                            ${m.rules ? `<div class="mapping-rule">${m.rules}</div>` : ''}
                            ${m.mapping_type ? `<div style="font-size: 0.75rem; color: var(--text-secondary);">Type: ${m.mapping_type}</div>` : ''}
                        </div>
                    `).join('')}
                </div>
            </div>
        `;

        panel.classList.add('open');
    }
}

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    new LineageExplorer();
});
'''
        js_file = assets_dir / "app.js"
        with open(js_file, "w", encoding="utf-8") as f:
            f.write(js_content)
        print(f"  Created: {js_file}")

        # Handle D3.js for offline use
        d3_file = assets_dir / "d3.min.js"

        # Priority 1: Check for local D3.js file
        d3_sources = [
            Path(__file__).parent / "assets" / "d3.min.js",
            Path(__file__).parent / "d3.min.js",
            Path.cwd() / "d3.min.js",
            Path.cwd() / "assets" / "d3.min.js",
        ]

        d3_found = False
        for src in d3_sources:
            if src.exists() and src.stat().st_size > 100000:  # Valid D3 file
                print(f"  Found local D3.js: {src}")
                shutil.copy(src, d3_file)
                print(f"  Copied to: {d3_file}")
                d3_found = True
                break

        # Priority 2: Try to download from CDN
        if not d3_found:
            try:
                import urllib.request
                print("  Downloading D3.js from CDN...")
                urllib.request.urlretrieve("https://d3js.org/d3.v7.min.js", d3_file)
                print(f"  Created: {d3_file}")
                d3_found = True
            except Exception as e:
                print(f"  Warning: Could not download D3.js ({e})")

        if not d3_found:
            print("  ERROR: D3.js not available. Viewer will not work.")
            print("  Run: python download_d3.py")


def main():
    parser = argparse.ArgumentParser(description="Generate Lineage Explorer from Excel files")
    parser.add_argument("--v1", required=True, help="Path to version 1 mappings Excel file")
    parser.add_argument("--v1-name", default="Version 1", help="Display name for version 1")
    parser.add_argument("--v2", help="Path to version 2 mappings Excel file (optional)")
    parser.add_argument("--v2-name", default="Version 2", help="Display name for version 2")
    parser.add_argument("--data-model", help="Path to data model Excel file (optional)")
    parser.add_argument("--output", default="./output", help="Output directory")

    args = parser.parse_args()

    processor = LineageProcessor(
        v1_path=args.v1,
        v1_name=args.v1_name,
        v2_path=args.v2,
        v2_name=args.v2_name,
        data_model_path=args.data_model,
    )

    processor.load_data()
    processor.compute_delta()
    processor.build_table_metadata()
    processor.generate_output(args.output)


if __name__ == "__main__":
    main()
