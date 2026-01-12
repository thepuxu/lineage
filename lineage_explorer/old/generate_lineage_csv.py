#!/usr/bin/env python3
"""
Lineage Explorer Generator - CSV Version (No External Dependencies)

Uses only built-in Python modules. No pandas, no pip install required.

Usage:
    python generate_lineage_csv.py \
        --v1 mappings_v1.csv \
        --v1-name "January Baseline" \
        --v2 mappings_v2.csv \
        --v2-name "February Changes" \
        --data-model data_model.csv \
        --output ./output
"""

import argparse
import csv
import json
import urllib.request
from pathlib import Path
from collections import defaultdict
from typing import Optional, List, Dict, Any

# D3.js will be embedded for offline use
D3_JS_CACHE = None

def get_d3_js() -> str:
    """Get D3.js library content, downloading if needed."""
    global D3_JS_CACHE
    if D3_JS_CACHE is not None:
        return D3_JS_CACHE

    # Check for local cached copy first
    script_dir = Path(__file__).parent
    d3_cache_file = script_dir / "d3.v7.min.js"

    if d3_cache_file.exists():
        print("  Loading D3.js from local cache...")
        D3_JS_CACHE = d3_cache_file.read_text(encoding='utf-8')
        return D3_JS_CACHE

    # Try to download
    d3_url = "https://d3js.org/d3.v7.min.js"
    print(f"  Downloading D3.js from {d3_url}...")
    try:
        with urllib.request.urlopen(d3_url, timeout=30) as response:
            D3_JS_CACHE = response.read().decode('utf-8')
        # Cache for future runs
        d3_cache_file.write_text(D3_JS_CACHE, encoding='utf-8')
        print(f"  Cached D3.js to {d3_cache_file}")
        return D3_JS_CACHE
    except Exception as e:
        raise RuntimeError(
            f"Could not load D3.js: {e}\n"
            f"Please download manually from {d3_url} and save as {d3_cache_file}"
        )


class LineageProcessor:
    """Processes CSV mappings into lineage data structures."""

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

        self.v1_mappings = None
        self.v2_mappings = None
        self.data_model = None
        self.tables_metadata = {}
        self.delta = None

    def load_data(self):
        """Load all CSV files."""
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

    def _normalize_column_name(self, col: str) -> str:
        """Normalize column name to internal format."""
        col_lower = col.strip().lower()

        # Maps lowercase variants -> internal name
        column_variants = {
            # Source table variants
            "source table": "source_table",
            "source_table": "source_table",
            "sourcetable": "source_table",
            # Source field variants
            "source field": "source_field",
            "source_field": "source_field",
            "sourcefield": "source_field",
            # Destination table variants
            "dest table": "dest_table",
            "dest_table": "dest_table",
            "desttable": "dest_table",
            "destination table": "dest_table",
            "destination_table": "dest_table",
            "target table": "dest_table",
            "target_table": "dest_table",
            # Destination field variants
            "dest field": "dest_field",
            "dest_field": "dest_field",
            "destfield": "dest_field",
            "destination field": "dest_field",
            "destination_field": "dest_field",
            "target field": "dest_field",
            "target_field": "dest_field",
            # Rules/transformation variants
            "rules": "rules",
            "rule": "rules",
            "transformation": "rules",
            "derived_expression": "rules",
            "derived expression": "rules",
            "derivedexpression": "rules",
            # Mapping type variants (including OFSAA usage_type)
            "mapping type": "mapping_type",
            "mapping_type": "mapping_type",
            "mappingtype": "mapping_type",
            "type": "mapping_type",
            "usage_type": "mapping_type",
            "usage type": "mapping_type",
            "usagetype": "mapping_type",
            # Object name variants
            "object name": "object_name",
            "object_name": "object_name",
            "objectname": "object_name",
            "object": "object_name",
            "etl object": "object_name",
            "etl_object": "object_name",
            # Additional OFSAA columns (preserved for future use)
            "usage_role": "usage_role",
            "usage role": "usage_role",
            "source_type": "source_type",
            "source type": "source_type",
            "constant_value": "constant_value",
            "constant value": "constant_value",
            "derived_output": "derived_output",
            "derived output": "derived_output",
            "join_alias": "join_alias",
            "join alias": "join_alias",
            "join_keys": "join_keys",
            "join keys": "join_keys",
            "join_filters": "join_filters",
            "join filters": "join_filters",
            "dm_match": "dm_match",
            "dm match": "dm_match",
            "trace_path": "trace_path",
            "trace path": "trace_path",
            "notes": "notes",
        }

        return column_variants.get(col_lower, col_lower)

    def _load_mappings(self, path: Path) -> List[Dict[str, Any]]:
        """Load mappings from CSV. Column order doesn't matter - matches by name."""
        mappings = []

        with open(path, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)

            # Build column mapping
            col_map = {}
            for col in reader.fieldnames:
                normalized = self._normalize_column_name(col)
                col_map[col] = normalized

            for row in reader:
                # Normalize row keys
                normalized_row = {}
                for orig_col, value in row.items():
                    normalized_col = col_map.get(orig_col, orig_col.lower())
                    normalized_row[normalized_col] = value.strip() if value else ""

                # Ensure required columns have defaults
                if "object_name" not in normalized_row:
                    normalized_row["object_name"] = ""
                if "rules" not in normalized_row:
                    normalized_row["rules"] = ""
                if "mapping_type" not in normalized_row:
                    normalized_row["mapping_type"] = "MAP"

                mappings.append(normalized_row)

        return mappings

    def _load_data_model(self, path: Path) -> List[Dict[str, Any]]:
        """Load data model from CSV."""
        columns = []

        # Column name mapping for data model
        dm_col_map = {
            "table name": "table_name",
            "table_name": "table_name",
            "tablename": "table_name",
            "column name": "column_name",
            "column_name": "column_name",
            "columnname": "column_name",
            "data type": "data_type",
            "data_type": "data_type",
            "datatype": "data_type",
            "is pk": "is_pk",
            "is_pk": "is_pk",
            "ispk": "is_pk",
            "pk": "is_pk",
            "is fk": "is_fk",
            "is_fk": "is_fk",
            "isfk": "is_fk",
            "fk": "is_fk",
            "description": "description",
        }

        with open(path, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)

            for row in reader:
                normalized_row = {}
                for orig_col, value in row.items():
                    col_lower = orig_col.strip().lower()
                    normalized_col = dm_col_map.get(col_lower, col_lower)
                    normalized_row[normalized_col] = value.strip() if value else ""

                columns.append(normalized_row)

        return columns

    def compute_delta(self):
        if self.v2_mappings is None:
            return

        print("Computing delta...")

        def make_key(row):
            return (row["source_table"], row["source_field"], row["dest_table"], row["dest_field"])

        v1_dict = {}
        for row in self.v1_mappings:
            key = make_key(row)
            v1_dict[key] = {
                "rules": str(row.get("rules", "")),
                "mapping_type": str(row.get("mapping_type", "")),
                "object_name": str(row.get("object_name", ""))
            }

        v2_dict = {}
        for row in self.v2_mappings:
            key = make_key(row)
            v2_dict[key] = {
                "rules": str(row.get("rules", "")),
                "mapping_type": str(row.get("mapping_type", "")),
                "object_name": str(row.get("object_name", ""))
            }

        added, removed, modified, unchanged = [], [], [], []
        all_keys = set(v1_dict.keys()) | set(v2_dict.keys())

        for key in all_keys:
            in_v1, in_v2 = key in v1_dict, key in v2_dict
            mapping_info = {"source_table": key[0], "source_field": key[1], "dest_table": key[2], "dest_field": key[3]}

            if in_v1 and not in_v2:
                mapping_info.update({
                    "v1_rules": v1_dict[key]["rules"],
                    "v1_mapping_type": v1_dict[key]["mapping_type"],
                    "v1_object_name": v1_dict[key]["object_name"]
                })
                removed.append(mapping_info)
            elif in_v2 and not in_v1:
                mapping_info.update({
                    "v2_rules": v2_dict[key]["rules"],
                    "v2_mapping_type": v2_dict[key]["mapping_type"],
                    "v2_object_name": v2_dict[key]["object_name"]
                })
                added.append(mapping_info)
            else:
                v1_data, v2_data = v1_dict[key], v2_dict[key]
                mapping_info.update({
                    "v1_rules": v1_data["rules"], "v2_rules": v2_data["rules"],
                    "v1_mapping_type": v1_data["mapping_type"], "v2_mapping_type": v2_data["mapping_type"],
                    "v1_object_name": v1_data["object_name"], "v2_object_name": v2_data["object_name"]
                })
                if v1_data["rules"] != v2_data["rules"] or v1_data["mapping_type"] != v2_data["mapping_type"]:
                    modified.append(mapping_info)
                else:
                    unchanged.append(mapping_info)

        self.delta = {
            "added": added, "removed": removed, "modified": modified, "unchanged": unchanged,
            "summary": {"added_count": len(added), "removed_count": len(removed),
                       "modified_count": len(modified), "unchanged_count": len(unchanged)}
        }
        print(f"  Added: {len(added)}, Removed: {len(removed)}, Modified: {len(modified)}, Unchanged: {len(unchanged)}")

    def build_table_metadata(self):
        print("Building table metadata...")
        tables = set()
        for mappings in [self.v1_mappings, self.v2_mappings]:
            if mappings is not None:
                for row in mappings:
                    tables.add(row["source_table"])
                    tables.add(row["dest_table"])

        for table in tables:
            self.tables_metadata[table] = {
                "name": table, "columns": [],
                "upstream_tables": set(), "downstream_tables": set()
            }

        if self.data_model is not None:
            for row in self.data_model:
                table = row.get("table_name", "")
                if table in self.tables_metadata:
                    is_pk = str(row.get("is_pk", "")).upper() in ("TRUE", "YES", "1", "Y")
                    is_fk = str(row.get("is_fk", "")).upper() in ("TRUE", "YES", "1", "Y")
                    self.tables_metadata[table]["columns"].append({
                        "name": row.get("column_name", ""),
                        "data_type": str(row.get("data_type", "")),
                        "is_pk": is_pk,
                        "is_fk": is_fk,
                    })

        mappings = self.v2_mappings if self.v2_mappings is not None else self.v1_mappings
        for row in mappings:
            src, dst = row["source_table"], row["dest_table"]
            if src in self.tables_metadata:
                self.tables_metadata[src]["downstream_tables"].add(dst)
            if dst in self.tables_metadata:
                self.tables_metadata[dst]["upstream_tables"].add(src)

        for table in self.tables_metadata:
            self.tables_metadata[table]["upstream_tables"] = list(self.tables_metadata[table]["upstream_tables"])
            self.tables_metadata[table]["downstream_tables"] = list(self.tables_metadata[table]["downstream_tables"])

        print(f"  Built metadata for {len(self.tables_metadata)} tables")

    def build_lineage_graph(self, mappings: List[Dict], version: str) -> dict:
        table_edges = defaultdict(lambda: {"count": 0, "mappings": []})
        for row in mappings:
            edge_key = (row["source_table"], row["dest_table"])
            table_edges[edge_key]["count"] += 1
            table_edges[edge_key]["mappings"].append({
                "source_field": row["source_field"], "dest_field": row["dest_field"],
                "rules": str(row.get("rules", "")), "mapping_type": str(row.get("mapping_type", "")),
                "object_name": str(row.get("object_name", ""))
            })

        nodes = {}
        for table in self.tables_metadata:
            nodes[table] = {
                "id": table, "label": table,
                "columns": self.tables_metadata[table]["columns"],
                "upstream_count": len(self.tables_metadata[table]["upstream_tables"]),
                "downstream_count": len(self.tables_metadata[table]["downstream_tables"]),
            }

        edges = [{"source": src, "target": dst, "mapping_count": data["count"], "mappings": data["mappings"]}
                 for (src, dst), data in table_edges.items()]

        return {"version": version, "nodes": nodes, "edges": edges}

    def generate_output(self, output_dir: Path):
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        print(f"\nGenerating output to {output_dir}...")

        # Build all data
        config = {
            "v1_name": self.v1_name,
            "v2_name": self.v2_name if self.v2_name else None,
            "has_v2": self.v2_mappings is not None,
            "has_delta": self.delta is not None,
            "table_count": len(self.tables_metadata),
        }

        v1_graph = self.build_lineage_graph(self.v1_mappings, self.v1_name)
        v2_graph = self.build_lineage_graph(self.v2_mappings, self.v2_name) if self.v2_mappings is not None else None

        # Generate single HTML file with embedded data
        html_content = self._generate_html(config, self.tables_metadata, v1_graph, v2_graph, self.delta)

        html_file = output_dir / "lineage_explorer.html"
        with open(html_file, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"  Created: {html_file}")
        print(f"\nDone! Open {html_file} in a browser.")

    def _generate_html(self, config, tables, v1_graph, v2_graph, delta):
        # Serialize data to JSON
        config_json = json.dumps(config)
        tables_json = json.dumps(tables)
        v1_json = json.dumps(v1_graph)
        v2_json = json.dumps(v2_graph) if v2_graph else "null"
        delta_json = json.dumps(delta) if delta else "null"

        # Get D3.js for embedding (fully offline)
        d3_js = get_d3_js()

        return f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Lineage Explorer</title>
    <!-- D3.js embedded for offline use -->
    <script>{d3_js}</script>
    <style>
{self._get_css()}
    </style>
</head>
<body>
    <div class="app">
        <!-- Header -->
        <header class="header">
            <div class="header-left">
                <svg class="logo" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5"/>
                </svg>
                <h1>Lineage Explorer</h1>
            </div>
            <div class="tabs" id="tabs">
                <button class="tab active" data-version="v1">{config.get('v1_name', 'Version 1')}</button>
                <button class="tab" data-version="v2" style="display: {'block' if config.get('has_v2') else 'none'}">{config.get('v2_name', 'Version 2')}</button>
                <button class="tab" data-version="delta" style="display: {'block' if config.get('has_delta') else 'none'}">Delta</button>
            </div>
        </header>

        <div class="main">
            <!-- Sidebar -->
            <aside class="sidebar">
                <div class="search-container">
                    <div class="search-box">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/>
                        </svg>
                        <input type="text" id="search" placeholder="Search tables or fields..." autocomplete="off">
                    </div>
                    <div class="search-dropdown" id="search-dropdown"></div>
                </div>
                <div class="table-list" id="table-list"></div>
            </aside>

            <!-- Graph Area -->
            <main class="graph-area">
                <div class="toolbar">
                    <div class="toolbar-group">
                        <button id="zoom-in" title="Zoom In">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35M11 8v6M8 11h6"/>
                            </svg>
                        </button>
                        <button id="zoom-out" title="Zoom Out">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35M8 11h6"/>
                            </svg>
                        </button>
                        <button id="reset-view" title="Reset View">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/>
                                <path d="M3 3v5h5"/>
                            </svg>
                        </button>
                    </div>
                    <div class="toolbar-group">
                        <label class="checkbox-label">
                            <input type="checkbox" id="show-upstream" checked>
                            <span>Upstream</span>
                        </label>
                        <label class="checkbox-label">
                            <input type="checkbox" id="show-downstream" checked>
                            <span>Downstream</span>
                        </label>
                    </div>
                    <div class="toolbar-group delta-filters" id="delta-filters" style="display: none;">
                        <label class="checkbox-label"><input type="checkbox" id="filter-added" checked><span class="badge green">Added</span></label>
                        <label class="checkbox-label"><input type="checkbox" id="filter-removed" checked><span class="badge red">Removed</span></label>
                        <label class="checkbox-label"><input type="checkbox" id="filter-modified" checked><span class="badge yellow">Modified</span></label>
                        <label class="checkbox-label"><input type="checkbox" id="filter-unchanged"><span class="badge gray">Unchanged</span></label>
                    </div>
                </div>
                <div class="delta-summary" id="delta-summary" style="display: none;"></div>
                <div class="graph-container" id="graph-container">
                    <svg id="graph"></svg>
                </div>
            </main>

            <!-- Details Panel -->
            <aside class="details-panel" id="details-panel">
                <div class="panel-header">
                    <h3 id="panel-title">Details</h3>
                    <button id="close-panel">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M18 6L6 18M6 6l12 12"/>
                        </svg>
                    </button>
                </div>
                <div class="panel-body" id="panel-body">
                    <p class="placeholder">Click a table or connection to view details</p>
                </div>
            </aside>
        </div>
    </div>

    <script>
// Embedded Data
const CONFIG = {config_json};
const TABLES = {tables_json};
const V1_GRAPH = {v1_json};
const V2_GRAPH = {v2_json};
const DELTA = {delta_json};

{self._get_js()}
    </script>
</body>
</html>'''

    def _get_css(self):
        return '''
* { margin: 0; padding: 0; box-sizing: border-box; }

body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: #f5f7fa;
    color: #1a1a2e;
    height: 100vh;
    overflow: hidden;
}

.app {
    display: flex;
    flex-direction: column;
    height: 100vh;
}

/* Header */
.header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 12px 24px;
    background: white;
    border-bottom: 1px solid #e1e5eb;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
}

.header-left {
    display: flex;
    align-items: center;
    gap: 12px;
}

.logo {
    width: 28px;
    height: 28px;
    color: #1890ff;
}

.header h1 {
    font-size: 18px;
    font-weight: 600;
    color: #1a1a2e;
}

.tabs {
    display: flex;
    gap: 8px;
}

.tab {
    padding: 8px 20px;
    background: #f0f2f5;
    border: none;
    border-radius: 6px;
    font-size: 14px;
    font-weight: 500;
    color: #666;
    cursor: pointer;
    transition: all 0.2s;
}

.tab:hover {
    background: #e6e8eb;
}

.tab.active {
    background: #1890ff;
    color: white;
}

/* Main Layout */
.main {
    display: flex;
    flex: 1;
    overflow: hidden;
}

/* Sidebar */
.sidebar {
    width: 280px;
    background: white;
    border-right: 1px solid #e1e5eb;
    display: flex;
    flex-direction: column;
}

.search-box {
    padding: 16px;
    border-bottom: 1px solid #e1e5eb;
    display: flex;
    align-items: center;
    gap: 10px;
}

.search-box svg {
    width: 18px;
    height: 18px;
    color: #999;
    flex-shrink: 0;
}

.search-box input {
    flex: 1;
    border: none;
    outline: none;
    font-size: 14px;
    color: #333;
}

.search-box input::placeholder {
    color: #999;
}

.search-container {
    position: relative;
}

.search-dropdown {
    position: absolute;
    top: 100%;
    left: 0;
    right: 0;
    background: white;
    border: 1px solid #e1e5eb;
    border-top: none;
    border-radius: 0 0 8px 8px;
    max-height: 300px;
    overflow-y: auto;
    z-index: 100;
    box-shadow: 0 4px 12px rgba(0,0,0,0.1);
    display: none;
}

.search-dropdown.open {
    display: block;
}

.search-dropdown-item {
    padding: 10px 16px;
    cursor: pointer;
    border-bottom: 1px solid #f0f0f0;
    transition: background 0.15s;
}

.search-dropdown-item:last-child {
    border-bottom: none;
}

.search-dropdown-item:hover {
    background: #f5f7fa;
}

.search-dropdown-item.highlighted {
    background: #e6f4ff;
}

.search-item-table {
    font-weight: 600;
    color: #1a1a2e;
    font-size: 13px;
}

.search-item-field {
    color: #1890ff;
    font-size: 13px;
}

.search-item-object {
    color: #722ed1;
    font-size: 13px;
    font-weight: 500;
}

.search-item-type {
    font-size: 11px;
    color: #8c8c8c;
    margin-top: 2px;
}

.table-list {
    flex: 1;
    overflow-y: auto;
    padding: 8px;
}

.table-item {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 10px 12px;
    border-radius: 8px;
    cursor: pointer;
    transition: all 0.15s;
    margin-bottom: 4px;
}

.table-item:hover {
    background: #f5f7fa;
}

.table-item.active {
    background: #e6f4ff;
}

.table-icon {
    width: 32px;
    height: 32px;
    border-radius: 6px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 12px;
    font-weight: 600;
    color: white;
    flex-shrink: 0;
}

.table-icon.src { background: #722ed1; }
.table-icon.stg { background: #13c2c2; }
.table-icon.dim { background: #1890ff; }
.table-icon.fct { background: #52c41a; }
.table-icon.fact { background: #52c41a; }
.table-icon.rpt { background: #fa8c16; }
.table-icon.oth { background: #8c8c8c; }

.table-info {
    flex: 1;
    min-width: 0;
}

.table-name {
    font-size: 14px;
    font-weight: 500;
    color: #1a1a2e;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}

.table-meta {
    font-size: 12px;
    color: #8c8c8c;
    margin-top: 2px;
}

/* Graph Area */
.graph-area {
    flex: 1;
    display: flex;
    flex-direction: column;
    background: #f5f7fa;
}

.toolbar {
    display: flex;
    align-items: center;
    gap: 16px;
    padding: 12px 16px;
    background: white;
    border-bottom: 1px solid #e1e5eb;
}

.toolbar-group {
    display: flex;
    align-items: center;
    gap: 8px;
}

.toolbar button {
    width: 36px;
    height: 36px;
    border: 1px solid #e1e5eb;
    border-radius: 6px;
    background: white;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    transition: all 0.15s;
}

.toolbar button:hover {
    background: #f5f7fa;
    border-color: #1890ff;
}

.toolbar button svg {
    width: 18px;
    height: 18px;
    color: #666;
}

.checkbox-label {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 13px;
    color: #666;
    cursor: pointer;
}

.checkbox-label input {
    accent-color: #1890ff;
}

.badge {
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 12px;
    font-weight: 500;
}

.badge.green { background: #d9f7be; color: #389e0d; }
.badge.red { background: #ffccc7; color: #cf1322; }
.badge.yellow { background: #fff1b8; color: #d48806; }
.badge.gray { background: #f0f0f0; color: #8c8c8c; }

.delta-summary {
    padding: 10px 16px;
    background: #e6f4ff;
    font-size: 13px;
    color: #1890ff;
    display: flex;
    gap: 24px;
}

.graph-container {
    flex: 1;
    position: relative;
    overflow: hidden;
}

#graph {
    width: 100%;
    height: 100%;
}

/* Graph Node Cards */
.node-card {
    cursor: pointer;
}

.node-card rect.card-bg {
    fill: white;
    stroke: #e1e5eb;
    stroke-width: 1;
    rx: 8;
    ry: 8;
    filter: drop-shadow(0 2px 4px rgba(0,0,0,0.08));
    transition: all 0.15s;
}

.node-card:hover rect.card-bg {
    stroke: #1890ff;
    filter: drop-shadow(0 4px 12px rgba(24,144,255,0.15));
}

.node-card.selected rect.card-bg {
    stroke: #1890ff;
    stroke-width: 2;
}

.node-card .card-header {
    fill: #f5f7fa;
}

.node-card .card-title {
    font-size: 13px;
    font-weight: 600;
    fill: #1a1a2e;
}

.node-card .card-subtitle {
    font-size: 11px;
    fill: #8c8c8c;
}

.node-card .column-text {
    font-size: 11px;
    fill: #666;
}

.node-card .column-badge {
    font-size: 9px;
    fill: white;
}

/* Field rows in node cards */
.field-row {
    cursor: pointer;
    transition: all 0.15s;
}

.field-row:hover rect {
    fill: #e6f4ff !important;
}

.field-row.highlighted rect {
    fill: #bae7ff !important;
}

.field-row.highlighted text {
    font-weight: 600 !important;
}

.field-text {
    font-size: 11px;
    fill: #333;
}

.field-type-badge {
    font-size: 9px;
    fill: white;
}

/* Connection Lines */
.link {
    fill: none;
    stroke: #d9d9d9;
    stroke-width: 1.5;
    opacity: 0.7;
    cursor: pointer;
    transition: all 0.15s;
}

.link:hover {
    stroke-width: 3;
    opacity: 1;
}

/* Mapping type colors */
.link.map-type { stroke: #1890ff; }
.link.join-type { stroke: #722ed1; }
.link.lookup-type { stroke: #13c2c2; }
.link.transform-type { stroke: #fa8c16; }

/* Delta change colors (override mapping type when in delta view) */
.link.added { stroke: #52c41a; stroke-width: 2; }
.link.removed { stroke: #ff4d4f; stroke-width: 2; stroke-dasharray: 6,3; }
.link.modified { stroke: #faad14; stroke-width: 2; }

.link-arrow {
    fill: #d9d9d9;
}

/* Details Panel */
.details-panel {
    width: 340px;
    background: white;
    border-left: 1px solid #e1e5eb;
    display: flex;
    flex-direction: column;
    transform: translateX(100%);
    transition: transform 0.25s ease;
}

.details-panel.open {
    transform: translateX(0);
}

.panel-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 16px 20px;
    border-bottom: 1px solid #e1e5eb;
}

.panel-header h3 {
    font-size: 16px;
    font-weight: 600;
}

.panel-header button {
    background: none;
    border: none;
    cursor: pointer;
    padding: 4px;
}

.panel-header button svg {
    width: 20px;
    height: 20px;
    color: #999;
}

.panel-body {
    flex: 1;
    overflow-y: auto;
    padding: 16px 20px;
}

.panel-body .placeholder {
    color: #999;
    font-style: italic;
    text-align: center;
    padding: 40px 20px;
}

.panel-section {
    margin-bottom: 20px;
}

.panel-section h4 {
    font-size: 12px;
    font-weight: 600;
    text-transform: uppercase;
    color: #8c8c8c;
    margin-bottom: 12px;
    letter-spacing: 0.5px;
}

.column-list {
    display: flex;
    flex-direction: column;
    gap: 6px;
}

.column-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 8px 10px;
    background: #f5f7fa;
    border-radius: 6px;
    font-size: 13px;
}

.column-row .col-name {
    font-weight: 500;
    color: #1a1a2e;
}

.column-row .col-badges {
    display: flex;
    gap: 4px;
}

.column-row .mini-badge {
    padding: 2px 6px;
    border-radius: 3px;
    font-size: 10px;
    font-weight: 600;
}

.mini-badge.pk { background: #1890ff; color: white; }
.mini-badge.fk { background: #722ed1; color: white; }

.mapping-item {
    padding: 12px;
    background: #f5f7fa;
    border-radius: 8px;
    margin-bottom: 8px;
    border-left: 3px solid #e1e5eb;
}

.mapping-item.added { border-left-color: #52c41a; }
.mapping-item.removed { border-left-color: #ff4d4f; }
.mapping-item.modified { border-left-color: #faad14; }

.mapping-fields {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 13px;
    font-weight: 500;
    margin-bottom: 6px;
}

.mapping-arrow {
    color: #1890ff;
}

.mapping-rule {
    font-size: 12px;
    font-family: 'SF Mono', Monaco, monospace;
    background: white;
    padding: 8px 10px;
    border-radius: 4px;
    color: #666;
    word-break: break-all;
    border: 1px solid #e1e5eb;
}

/* Scrollbar */
::-webkit-scrollbar { width: 6px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: #d9d9d9; border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: #bfbfbf; }

/* Compact Table Feature */
.compact-btn {
    cursor: pointer;
    opacity: 0;
    transition: opacity 0.2s;
}

.node-card:hover .compact-btn {
    opacity: 0.7;
}

.compact-btn:hover {
    opacity: 1 !important;
}

.compact-btn rect {
    fill: #f5f7fa;
    stroke: #d9d9d9;
    stroke-width: 1;
    rx: 3;
    ry: 3;
}

.compact-btn:hover rect {
    fill: #e6f4ff;
    stroke: #1890ff;
}

.compact-btn text {
    font-size: 10px;
    fill: #666;
    font-weight: 600;
    pointer-events: none;
}

.node-card.compacted rect.card-bg {
    fill: #fafafa;
}

.node-card.compacted .card-header {
    fill: #f0f0f0;
}

.compacted-info {
    font-size: 11px;
    fill: #8c8c8c;
}
'''

    def _get_js(self):
        return '''
class LineageExplorer {
    constructor() {
        this.currentVersion = 'v1';
        this.selectedTable = null;
        this.selectedField = null;
        this.selectedObject = null;
        this.searchQuery = '';
        this.svg = null;
        this.g = null;
        this.zoom = null;
        this.width = 0;
        this.height = 0;
        this.nodePositions = new Map();
        this.mappedFields = new Map();
        this.objectsIndex = new Map();
        this.searchIndex = [];
        this.compactedTables = new Set();

        this.init();
    }

    init() {
        this.buildMappedFieldsIndex();
        this.buildObjectsIndex();
        this.buildSearchIndex();
        this.setupUI();
        this.renderTableList();
        this.setupGraph();
        this.renderGraph();
    }

    buildObjectsIndex() {
        const graph = V2_GRAPH || V1_GRAPH;
        if (!graph) return;

        graph.edges.forEach(edge => {
            const mappings = edge.mappings || [];
            mappings.forEach(m => {
                const objName = m.object_name;
                if (objName && objName !== '' && objName !== 'nan') {
                    if (!this.objectsIndex.has(objName)) {
                        this.objectsIndex.set(objName, []);
                    }
                    this.objectsIndex.get(objName).push({
                        sourceTable: edge.source,
                        targetTable: edge.target,
                        source_field: m.source_field,
                        dest_field: m.dest_field,
                        rules: m.rules,
                        mapping_type: m.mapping_type,
                        object_name: objName
                    });
                }
            });
        });
    }

    buildMappedFieldsIndex() {
        const graph = V2_GRAPH || V1_GRAPH;
        if (!graph) return;

        graph.edges.forEach(edge => {
            const mappings = edge.mappings || [];
            mappings.forEach(m => {
                if (!this.mappedFields.has(edge.source)) {
                    this.mappedFields.set(edge.source, new Set());
                }
                this.mappedFields.get(edge.source).add(m.source_field);

                if (!this.mappedFields.has(edge.target)) {
                    this.mappedFields.set(edge.target, new Set());
                }
                this.mappedFields.get(edge.target).add(m.dest_field);
            });
        });
    }

    buildSearchIndex() {
        this.searchIndex = [];

        Object.keys(TABLES).forEach(tableName => {
            this.searchIndex.push({
                type: 'table',
                table: tableName,
                field: null,
                object: null,
                display: tableName,
                searchText: tableName.toLowerCase()
            });

            const mappedFieldSet = this.mappedFields.get(tableName);
            if (mappedFieldSet) {
                mappedFieldSet.forEach(fieldName => {
                    this.searchIndex.push({
                        type: 'field',
                        table: tableName,
                        field: fieldName,
                        object: null,
                        display: `${tableName}.${fieldName}`,
                        searchText: `${tableName}.${fieldName}`.toLowerCase()
                    });
                });
            }
        });

        this.objectsIndex.forEach((mappings, objectName) => {
            this.searchIndex.push({
                type: 'object',
                table: null,
                field: null,
                object: objectName,
                display: objectName,
                searchText: objectName.toLowerCase(),
                mappingCount: mappings.length
            });
        });
    }

    setupUI() {
        const searchInput = document.getElementById('search');
        const dropdown = document.getElementById('search-dropdown');

        document.querySelectorAll('.tab').forEach(tab => {
            tab.addEventListener('click', () => {
                document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
                tab.classList.add('active');
                this.currentVersion = tab.dataset.version;

                const deltaFilters = document.getElementById('delta-filters');
                const deltaSummary = document.getElementById('delta-summary');
                if (this.currentVersion === 'delta' && DELTA) {
                    deltaFilters.style.display = 'flex';
                    deltaSummary.style.display = 'flex';
                    this.renderDeltaSummary();
                } else {
                    deltaFilters.style.display = 'none';
                    deltaSummary.style.display = 'none';
                }
                this.nodePositions.clear();
                this.renderGraph();
            });
        });

        searchInput.addEventListener('input', (e) => {
            const query = e.target.value.trim();
            this.searchQuery = query;

            if (!query) {
                this.selectedField = null;
                if (this.selectedTable) {
                    this.nodePositions.clear();
                    this.renderGraph();
                }
            }

            this.showSearchDropdown(query);
            this.filterTables(query);
        });

        searchInput.addEventListener('focus', () => {
            if (searchInput.value.trim()) {
                this.showSearchDropdown(searchInput.value.trim());
            }
        });

        document.addEventListener('click', (e) => {
            if (!e.target.closest('.search-container')) {
                dropdown.classList.remove('open');
            }
        });

        searchInput.addEventListener('keydown', (e) => {
            const items = dropdown.querySelectorAll('.search-dropdown-item');
            const highlighted = dropdown.querySelector('.search-dropdown-item.highlighted');

            if (e.key === 'ArrowDown') {
                e.preventDefault();
                if (!highlighted && items.length > 0) {
                    items[0].classList.add('highlighted');
                } else if (highlighted) {
                    const next = highlighted.nextElementSibling;
                    if (next) {
                        highlighted.classList.remove('highlighted');
                        next.classList.add('highlighted');
                        next.scrollIntoView({ block: 'nearest' });
                    }
                }
            } else if (e.key === 'ArrowUp') {
                e.preventDefault();
                if (highlighted) {
                    const prev = highlighted.previousElementSibling;
                    if (prev) {
                        highlighted.classList.remove('highlighted');
                        prev.classList.add('highlighted');
                        prev.scrollIntoView({ block: 'nearest' });
                    }
                }
            } else if (e.key === 'Enter') {
                e.preventDefault();
                if (highlighted) {
                    highlighted.click();
                }
            } else if (e.key === 'Escape') {
                dropdown.classList.remove('open');
            }
        });

        document.getElementById('zoom-in').addEventListener('click', () => {
            this.svg.transition().duration(300).call(this.zoom.scaleBy, 1.4);
        });
        document.getElementById('zoom-out').addEventListener('click', () => {
            this.svg.transition().duration(300).call(this.zoom.scaleBy, 0.7);
        });
        document.getElementById('reset-view').addEventListener('click', () => {
            this.svg.transition().duration(500).call(this.zoom.transform, d3.zoomIdentity);
        });

        ['show-upstream', 'show-downstream', 'filter-added', 'filter-removed', 'filter-modified', 'filter-unchanged'].forEach(id => {
            const el = document.getElementById(id);
            if (el) el.addEventListener('change', () => {
                this.nodePositions.clear();
                this.renderGraph();
            });
        });

        document.getElementById('close-panel').addEventListener('click', () => {
            document.getElementById('details-panel').classList.remove('open');
        });
    }

    showSearchDropdown(query) {
        const dropdown = document.getElementById('search-dropdown');

        if (!query) {
            dropdown.classList.remove('open');
            return;
        }

        const q = query.toLowerCase();
        const matches = this.searchIndex
            .filter(item => item.searchText.includes(q))
            .slice(0, 15);

        if (matches.length === 0) {
            dropdown.classList.remove('open');
            return;
        }

        dropdown.innerHTML = matches.map(item => `
            <div class="search-dropdown-item" data-type="${item.type}" data-table="${item.table || ''}" data-field="${item.field || ''}" data-object="${item.object || ''}">
                ${item.type === 'table'
                    ? `<div class="search-item-table">${this.highlightMatch(item.display, q)}</div>
                       <div class="search-item-type">Table</div>`
                    : item.type === 'field'
                    ? `<div class="search-item-field">${this.highlightMatch(item.display, q)}</div>
                       <div class="search-item-type">Field</div>`
                    : `<div class="search-item-object">${this.highlightMatch(item.display, q)}</div>
                       <div class="search-item-type">Object (${item.mappingCount} mappings)</div>`
                }
            </div>
        `).join('');

        dropdown.querySelectorAll('.search-dropdown-item').forEach(item => {
            item.addEventListener('click', () => {
                const type = item.dataset.type;
                const table = item.dataset.table || null;
                const field = item.dataset.field || null;
                const object = item.dataset.object || null;
                this.selectFromSearch(table, field, object);
                dropdown.classList.remove('open');
                document.getElementById('search').value = item.querySelector('.search-item-table, .search-item-field, .search-item-object').textContent;
            });
        });

        dropdown.classList.add('open');
    }

    highlightMatch(text, query) {
        const idx = text.toLowerCase().indexOf(query);
        if (idx === -1) return text;
        return text.substring(0, idx) +
               '<strong style="color:#1890ff">' + text.substring(idx, idx + query.length) + '</strong>' +
               text.substring(idx + query.length);
    }

    selectFromSearch(tableName, fieldName, objectName) {
        if (objectName) {
            this.selectedTable = null;
            this.selectedField = null;
            this.selectedObject = objectName;
            this.searchQuery = objectName;

            document.querySelectorAll('.table-item').forEach(item => {
                item.classList.remove('active');
            });

            this.nodePositions.clear();
            this.renderGraph();
            this.showObjectDetails(objectName);
        } else {
            this.selectedTable = tableName;
            this.selectedField = fieldName;
            this.selectedObject = null;
            this.searchQuery = fieldName ? `${tableName}.${fieldName}` : tableName;

            document.querySelectorAll('.table-item').forEach(item => {
                item.classList.toggle('active', item.dataset.table === tableName);
            });

            this.nodePositions.clear();
            this.renderGraph();
            this.showTableDetails(tableName);
        }
    }

    renderTableList() {
        const container = document.getElementById('table-list');
        const tableNames = Object.keys(TABLES).sort();

        container.innerHTML = tableNames.map(name => {
            const table = TABLES[name];
            const layer = this.getLayer(name);
            const mappedCount = this.mappedFields.get(name)?.size || 0;
            return `
                <div class="table-item" data-table="${name}">
                    <div class="table-icon ${layer.toLowerCase()}">${layer}</div>
                    <div class="table-info">
                        <div class="table-name">${name}</div>
                        <div class="table-meta">${mappedCount} mapped fields</div>
                    </div>
                </div>
            `;
        }).join('');

        container.querySelectorAll('.table-item').forEach(item => {
            item.addEventListener('click', () => {
                this.selectedField = null;
                this.selectTable(item.dataset.table);
            });
        });
    }

    getLayer(name) {
        if (name.startsWith('src_')) return 'SRC';
        if (name.startsWith('stg_')) return 'STG';
        if (name.startsWith('dim_')) return 'DIM';
        if (name.startsWith('fct_')) return 'FCT';
        if (name.startsWith('fact_')) return 'FACT';
        if (name.startsWith('rpt_')) return 'RPT';
        return 'OTH';
    }

    getLayerOrder(name) {
        const layer = this.getLayer(name);
        const order = { SRC: 0, STG: 1, DIM: 2, FCT: 2, FACT: 2, RPT: 3, OTH: 4 };
        return order[layer] ?? 4;
    }

    getMappingTypeClass(mappingType) {
        const type = (mappingType || '').toLowerCase();
        if (type.includes('join')) return 'join-type';
        if (type.includes('lookup')) return 'lookup-type';
        if (type.includes('transform') || type.includes('calc')) return 'transform-type';
        return 'map-type';
    }

    traceFieldLineage(graph, tableName, fieldName, showUp, showDown) {
        const relevantMappings = [];
        const visitedUp = new Set();
        const visitedDown = new Set();

        const traceUp = (table, field) => {
            const key = `${table}.${field}`;
            if (visitedUp.has(key)) return;
            visitedUp.add(key);

            graph.edges.forEach(edge => {
                if (edge.target === table) {
                    const mappings = edge.mappings || [];
                    mappings.forEach(m => {
                        if (m.dest_field === field) {
                            relevantMappings.push({
                                ...m,
                                sourceTable: edge.source,
                                targetTable: edge.target,
                                changeType: edge.changeType
                            });
                            traceUp(edge.source, m.source_field);
                        }
                    });
                }
            });
        };

        const traceDown = (table, field) => {
            const key = `${table}.${field}`;
            if (visitedDown.has(key)) return;
            visitedDown.add(key);

            graph.edges.forEach(edge => {
                if (edge.source === table) {
                    const mappings = edge.mappings || [];
                    mappings.forEach(m => {
                        if (m.source_field === field) {
                            relevantMappings.push({
                                ...m,
                                sourceTable: edge.source,
                                targetTable: edge.target,
                                changeType: edge.changeType
                            });
                            traceDown(edge.target, m.dest_field);
                        }
                    });
                }
            });
        };

        if (showUp) traceUp(tableName, fieldName);
        if (showDown) traceDown(tableName, fieldName);

        const relevantTables = new Set([tableName]);
        const relevantFields = new Map();

        relevantFields.set(tableName, new Set([fieldName]));

        relevantMappings.forEach(m => {
            relevantTables.add(m.sourceTable);
            relevantTables.add(m.targetTable);

            if (!relevantFields.has(m.sourceTable)) {
                relevantFields.set(m.sourceTable, new Set());
            }
            relevantFields.get(m.sourceTable).add(m.source_field);

            if (!relevantFields.has(m.targetTable)) {
                relevantFields.set(m.targetTable, new Set());
            }
            relevantFields.get(m.targetTable).add(m.dest_field);
        });

        return { relevantTables, relevantFields, relevantMappings };
    }

    traceObjectLineage(graph, objectName, showUp, showDown) {
        const objectMappings = this.objectsIndex.get(objectName) || [];

        const relevantMappings = [];
        const relevantTables = new Set();
        const relevantFields = new Map();
        const visitedUp = new Set();
        const visitedDown = new Set();

        const traceUp = (table, field) => {
            const key = `${table}.${field}`;
            if (visitedUp.has(key)) return;
            visitedUp.add(key);

            graph.edges.forEach(edge => {
                if (edge.target === table) {
                    const mappings = edge.mappings || [];
                    mappings.forEach(m => {
                        if (m.dest_field === field) {
                            relevantMappings.push({
                                ...m,
                                sourceTable: edge.source,
                                targetTable: edge.target,
                                changeType: edge.changeType
                            });
                            relevantTables.add(edge.source);
                            if (!relevantFields.has(edge.source)) {
                                relevantFields.set(edge.source, new Set());
                            }
                            relevantFields.get(edge.source).add(m.source_field);
                            traceUp(edge.source, m.source_field);
                        }
                    });
                }
            });
        };

        const traceDown = (table, field) => {
            const key = `${table}.${field}`;
            if (visitedDown.has(key)) return;
            visitedDown.add(key);

            graph.edges.forEach(edge => {
                if (edge.source === table) {
                    const mappings = edge.mappings || [];
                    mappings.forEach(m => {
                        if (m.source_field === field) {
                            relevantMappings.push({
                                ...m,
                                sourceTable: edge.source,
                                targetTable: edge.target,
                                changeType: edge.changeType
                            });
                            relevantTables.add(edge.target);
                            if (!relevantFields.has(edge.target)) {
                                relevantFields.set(edge.target, new Set());
                            }
                            relevantFields.get(edge.target).add(m.dest_field);
                            traceDown(edge.target, m.dest_field);
                        }
                    });
                }
            });
        };

        objectMappings.forEach(m => {
            relevantMappings.push({
                ...m,
                isObjectMapping: true
            });
            relevantTables.add(m.sourceTable);
            relevantTables.add(m.targetTable);

            if (!relevantFields.has(m.sourceTable)) {
                relevantFields.set(m.sourceTable, new Set());
            }
            relevantFields.get(m.sourceTable).add(m.source_field);

            if (!relevantFields.has(m.targetTable)) {
                relevantFields.set(m.targetTable, new Set());
            }
            relevantFields.get(m.targetTable).add(m.dest_field);

            if (showUp) {
                traceUp(m.sourceTable, m.source_field);
            }
            if (showDown) {
                traceDown(m.targetTable, m.dest_field);
            }
        });

        return { relevantTables, relevantFields, relevantMappings };
    }

    filterTables(query) {
        const q = query.toLowerCase().trim();

        if (!q) {
            document.querySelectorAll('.table-item').forEach(item => {
                item.style.display = 'flex';
                const mappedCount = this.mappedFields.get(item.dataset.table)?.size || 0;
                item.querySelector('.table-meta').textContent = `${mappedCount} mapped fields`;
            });
            return;
        }

        document.querySelectorAll('.table-item').forEach(item => {
            const tableName = item.dataset.table.toLowerCase();
            const mappedFieldSet = this.mappedFields.get(item.dataset.table);

            const tableMatch = tableName.includes(q);
            let fieldMatch = false;
            let matchingFieldCount = 0;

            if (mappedFieldSet) {
                mappedFieldSet.forEach(f => {
                    if (f.toLowerCase().includes(q)) {
                        fieldMatch = true;
                        matchingFieldCount++;
                    }
                });
            }

            if (tableMatch || fieldMatch) {
                item.style.display = 'flex';
                if (fieldMatch && !tableMatch) {
                    item.querySelector('.table-meta').innerHTML =
                        `<span style="color:#1890ff">${matchingFieldCount} matching field(s)</span>`;
                } else {
                    const mappedCount = mappedFieldSet?.size || 0;
                    item.querySelector('.table-meta').textContent = `${mappedCount} mapped fields`;
                }
            } else {
                item.style.display = 'none';
            }
        });
    }

    selectTable(tableName) {
        this.selectedTable = tableName;
        document.querySelectorAll('.table-item').forEach(item => {
            item.classList.toggle('active', item.dataset.table === tableName);
        });
        this.nodePositions.clear();
        this.renderGraph();
        this.showTableDetails(tableName);
    }

    toggleCompact(tableName) {
        if (this.compactedTables.has(tableName)) {
            this.compactedTables.delete(tableName);
        } else {
            this.compactedTables.add(tableName);
        }
        this.renderGraph();
    }

    setupGraph() {
        const container = document.getElementById('graph-container');
        this.width = container.clientWidth;
        this.height = container.clientHeight;

        this.svg = d3.select('#graph')
            .attr('width', this.width)
            .attr('height', this.height);

        this.g = this.svg.append('g');

        this.zoom = d3.zoom()
            .scaleExtent([0.1, 3])
            .on('zoom', (event) => this.g.attr('transform', event.transform));

        this.svg.call(this.zoom);

        const defs = this.svg.append('defs');

        ['arrow', 'arrow-map', 'arrow-join', 'arrow-lookup', 'arrow-transform'].forEach((id, i) => {
            const colors = ['#d9d9d9', '#1890ff', '#722ed1', '#13c2c2', '#fa8c16'];
            defs.append('marker')
                .attr('id', id)
                .attr('viewBox', '0 -4 8 8')
                .attr('refX', 6)
                .attr('refY', 0)
                .attr('markerWidth', 6)
                .attr('markerHeight', 6)
                .attr('orient', 'auto')
                .append('path')
                .attr('d', 'M0,-3L6,0L0,3')
                .attr('fill', colors[i]);
        });
    }

    getMappedFieldsForTable(tableName, graph) {
        const fields = new Map();

        graph.edges.forEach(edge => {
            const mappings = edge.mappings || [];
            mappings.forEach(m => {
                if (edge.source === tableName) {
                    if (!fields.has(m.source_field)) {
                        fields.set(m.source_field, { isSource: true, isTarget: false, mappings: [] });
                    }
                    fields.get(m.source_field).isSource = true;
                    fields.get(m.source_field).mappings.push({ ...m, direction: 'out', targetTable: edge.target });
                }
                if (edge.target === tableName) {
                    if (!fields.has(m.dest_field)) {
                        fields.set(m.dest_field, { isSource: false, isTarget: true, mappings: [] });
                    }
                    fields.get(m.dest_field).isTarget = true;
                    fields.get(m.dest_field).mappings.push({ ...m, direction: 'in', sourceTable: edge.source });
                }
            });
        });

        return fields;
    }

    calculateCardHeight(fieldCount, tableName = null) {
        if (tableName && this.compactedTables.has(tableName)) {
            return 48;
        }
        const headerHeight = 32;
        const fieldRowHeight = 20;
        const padding = 16;
        const minFields = 2;
        const maxFields = 8;
        const displayFields = Math.min(Math.max(fieldCount, minFields), maxFields);
        return headerHeight + (displayFields * fieldRowHeight) + padding;
    }

    calculateHierarchicalLayout(nodes, edges, graph) {
        const cardW = 220;
        const horizontalGap = 320;
        const verticalGap = 20;
        const padding = 60;

        nodes.forEach(n => {
            let fields = this.getMappedFieldsForTable(n.id, graph);

            if (this.fieldLineageMode && this.relevantFieldsMap) {
                const relevantFieldSet = this.relevantFieldsMap.get(n.id);
                if (relevantFieldSet) {
                    const filteredFields = new Map();
                    fields.forEach((value, key) => {
                        if (relevantFieldSet.has(key)) {
                            filteredFields.set(key, value);
                        }
                    });
                    fields = filteredFields;
                }
            }

            n.mappedFields = fields;
            n.isCompacted = this.compactedTables.has(n.id);
            n.cardH = this.calculateCardHeight(fields.size, n.id);
        });

        const layers = {};
        nodes.forEach(n => {
            const layer = this.getLayerOrder(n.id);
            if (!layers[layer]) layers[layer] = [];
            layers[layer].push(n);
        });

        const layerKeys = Object.keys(layers).sort((a, b) => a - b);

        layerKeys.forEach((layerKey, layerIndex) => {
            const layerNodes = layers[layerKey];
            const x = padding + layerIndex * horizontalGap;

            let currentY = padding;
            layerNodes.forEach((node) => {
                const savedPos = this.nodePositions.get(node.id);
                if (savedPos) {
                    node.x = savedPos.x;
                    node.y = savedPos.y;
                } else {
                    node.x = x;
                    node.y = currentY;
                }
                currentY += node.cardH + verticalGap;
            });
        });

        return { nodes, edges, cardW };
    }

    renderGraph() {
        const graph = this.currentVersion === 'v1' ? V1_GRAPH :
                      this.currentVersion === 'v2' ? V2_GRAPH :
                      this.buildDeltaGraph();

        if (!graph) return;

        this.g.selectAll('*').remove();

        let nodes, edges;
        let fieldLineageMode = false;
        let relevantFieldsMap = null;

        const showUp = document.getElementById('show-upstream').checked;
        const showDown = document.getElementById('show-downstream').checked;

        if (this.selectedObject) {
            fieldLineageMode = true;
            const lineage = this.traceObjectLineage(graph, this.selectedObject, showUp, showDown);

            nodes = Object.values(graph.nodes).filter(n => lineage.relevantTables.has(n.id));
            relevantFieldsMap = lineage.relevantFields;

            const edgeMap = new Map();
            lineage.relevantMappings.forEach(m => {
                const key = `${m.sourceTable}->${m.targetTable}`;
                if (!edgeMap.has(key)) {
                    edgeMap.set(key, {
                        source: m.sourceTable,
                        target: m.targetTable,
                        changeType: m.changeType,
                        mappings: []
                    });
                }
                edgeMap.get(key).mappings.push(m);
            });
            edges = Array.from(edgeMap.values());

        } else if (this.selectedTable && this.selectedField) {
            fieldLineageMode = true;
            const lineage = this.traceFieldLineage(graph, this.selectedTable, this.selectedField, showUp, showDown);

            nodes = Object.values(graph.nodes).filter(n => lineage.relevantTables.has(n.id));
            relevantFieldsMap = lineage.relevantFields;

            const edgeMap = new Map();
            lineage.relevantMappings.forEach(m => {
                const key = `${m.sourceTable}->${m.targetTable}`;
                if (!edgeMap.has(key)) {
                    edgeMap.set(key, {
                        source: m.sourceTable,
                        target: m.targetTable,
                        changeType: m.changeType,
                        mappings: []
                    });
                }
                edgeMap.get(key).mappings.push(m);
            });
            edges = Array.from(edgeMap.values());

        } else if (this.selectedTable) {
            const relevantTables = new Set([this.selectedTable]);
            const relevantEdges = [];

            graph.edges.forEach(e => {
                if (e.source === this.selectedTable && showDown) {
                    relevantTables.add(e.target);
                    relevantEdges.push(e);
                }
                if (e.target === this.selectedTable && showUp) {
                    relevantTables.add(e.source);
                    relevantEdges.push(e);
                }
            });

            const firstLevel = new Set(relevantTables);
            graph.edges.forEach(e => {
                if (firstLevel.has(e.source) && showDown && e.source !== this.selectedTable) {
                    relevantTables.add(e.target);
                    relevantEdges.push(e);
                }
                if (firstLevel.has(e.target) && showUp && e.target !== this.selectedTable) {
                    relevantTables.add(e.source);
                    relevantEdges.push(e);
                }
            });

            nodes = Object.values(graph.nodes).filter(n => relevantTables.has(n.id));
            edges = [...new Map(relevantEdges.map(e => [`${e.source}->${e.target}`, e])).values()];
        } else {
            nodes = Object.values(graph.nodes);
            edges = graph.edges;
        }

        this.fieldLineageMode = fieldLineageMode;
        this.relevantFieldsMap = relevantFieldsMap;

        if (this.currentVersion === 'delta') {
            const filters = {
                added: document.getElementById('filter-added')?.checked,
                removed: document.getElementById('filter-removed')?.checked,
                modified: document.getElementById('filter-modified')?.checked,
                unchanged: document.getElementById('filter-unchanged')?.checked,
            };
            edges = edges.filter(e => {
                if (!e.changeType) return true;
                return filters[e.changeType];
            });
        }

        if (nodes.length === 0) return;

        const layout = this.calculateHierarchicalLayout([...nodes], edges, graph);
        const cardW = layout.cardW;

        const nodeMap = new Map(layout.nodes.map(n => [n.id, n]));
        const fieldPositions = new Map();

        const self = this;

        const node = this.g.append('g')
            .attr('class', 'nodes')
            .selectAll('g')
            .data(layout.nodes)
            .enter()
            .append('g')
            .attr('class', d => `node-card ${d.id === this.selectedTable ? 'selected' : ''} ${d.isCompacted ? 'compacted' : ''}`)
            .attr('transform', d => `translate(${d.x},${d.y})`)
            .call(d3.drag()
                .on('drag', function(event, d) {
                    d.x = event.x;
                    d.y = event.y;
                    self.nodePositions.set(d.id, { x: d.x, y: d.y });
                    d3.select(this).attr('transform', `translate(${d.x},${d.y})`);
                    self.updateFieldPositions(layout.nodes, cardW, fieldPositions);
                    self.updateEdges(edges, nodeMap, cardW, fieldPositions);
                }))
            .on('click', (event, d) => {
                event.stopPropagation();
                this.selectedField = null;
                this.selectTable(d.id);
            });

        node.append('rect')
            .attr('class', 'card-bg')
            .attr('width', cardW)
            .attr('height', d => d.cardH);

        node.append('rect')
            .attr('class', 'card-header')
            .attr('width', cardW - 2)
            .attr('height', 28)
            .attr('x', 1)
            .attr('y', 1)
            .attr('rx', 7)
            .attr('ry', 7);

        node.append('rect')
            .attr('width', 36)
            .attr('height', 18)
            .attr('x', 8)
            .attr('y', 6)
            .attr('rx', 4)
            .attr('fill', d => {
                const l = this.getLayer(d.id);
                const colors = {SRC:'#722ed1',STG:'#13c2c2',DIM:'#1890ff',FCT:'#52c41a',FACT:'#52c41a',RPT:'#fa8c16',OTH:'#8c8c8c'};
                return colors[l] || '#8c8c8c';
            });

        node.append('text')
            .attr('x', 26)
            .attr('y', 18)
            .attr('text-anchor', 'middle')
            .attr('fill', 'white')
            .attr('font-size', '9px')
            .attr('font-weight', '600')
            .text(d => this.getLayer(d.id));

        node.append('text')
            .attr('class', 'card-title')
            .attr('x', 50)
            .attr('y', 18)
            .text(d => d.label.length > 20 ? d.label.substring(0, 18) + '...' : d.label);

        const compactBtn = node.append('g')
            .attr('class', 'compact-btn')
            .attr('transform', `translate(${cardW - 28}, 5)`)
            .on('click', (event, d) => {
                event.stopPropagation();
                this.toggleCompact(d.id);
            });

        compactBtn.append('rect')
            .attr('width', 22)
            .attr('height', 18);

        compactBtn.append('text')
            .attr('x', 11)
            .attr('y', 13)
            .attr('text-anchor', 'middle')
            .text(d => d.isCompacted ? '+' : '-');

        node.each(function(d) {
            const g = d3.select(this);

            if (d.isCompacted) {
                g.append('text')
                    .attr('class', 'compacted-info')
                    .attr('x', cardW / 2)
                    .attr('y', 38)
                    .attr('text-anchor', 'middle')
                    .text(`${d.mappedFields.size} fields`);

                const centerY = d.y + d.cardH / 2;
                d.mappedFields.forEach((fieldInfo, fieldName) => {
                    fieldPositions.set(`${d.id}.${fieldName}`, {
                        leftX: d.x,
                        rightX: d.x + cardW,
                        y: centerY
                    });
                });
            } else {
                const fields = Array.from(d.mappedFields.entries()).slice(0, 8);
                const rowHeight = 20;
                const startY = 34;

                fields.forEach(([fieldName, fieldInfo], i) => {
                    const isHighlighted = self.selectedField === fieldName &&
                                         self.selectedTable === d.id;
                    const matchesSearch = self.searchQuery &&
                        fieldName.toLowerCase().includes(self.searchQuery.toLowerCase());

                    const fieldGroup = g.append('g')
                        .attr('class', `field-row ${isHighlighted || matchesSearch ? 'highlighted' : ''}`)
                        .attr('transform', `translate(0, ${startY + i * rowHeight})`);

                    fieldGroup.append('rect')
                        .attr('x', 4)
                        .attr('y', 0)
                        .attr('width', cardW - 8)
                        .attr('height', rowHeight - 2)
                        .attr('rx', 3)
                        .attr('fill', (isHighlighted || matchesSearch) ? '#bae7ff' : '#f5f7fa');

                    const dirColor = fieldInfo.isSource && fieldInfo.isTarget ? '#faad14' :
                                    fieldInfo.isSource ? '#52c41a' : '#1890ff';
                    fieldGroup.append('circle')
                        .attr('cx', 14)
                        .attr('cy', rowHeight / 2 - 1)
                        .attr('r', 4)
                        .attr('fill', dirColor);

                    fieldGroup.append('text')
                        .attr('class', 'field-text')
                        .attr('x', 24)
                        .attr('y', rowHeight / 2 + 3)
                        .text(fieldName.length > 24 ? fieldName.substring(0, 22) + '...' : fieldName);

                    const fieldY = d.y + startY + i * rowHeight + rowHeight / 2;
                    fieldPositions.set(`${d.id}.${fieldName}`, {
                        leftX: d.x,
                        rightX: d.x + cardW,
                        y: fieldY
                    });
                });

                if (d.mappedFields.size > 8) {
                    g.append('text')
                        .attr('class', 'card-subtitle')
                        .attr('x', cardW / 2)
                        .attr('y', startY + 8 * rowHeight + 10)
                        .attr('text-anchor', 'middle')
                        .text(`+${d.mappedFields.size - 8} more fields`);
                }
            }
        });

        this.updateFieldPositions(layout.nodes, cardW, fieldPositions);
        this.drawFieldLevelEdges(edges, nodeMap, cardW, fieldPositions);
    }

    updateFieldPositions(nodes, cardW, fieldPositions) {
        nodes.forEach(d => {
            if (d.isCompacted) {
                const centerY = d.y + d.cardH / 2;
                d.mappedFields.forEach((fieldInfo, fieldName) => {
                    fieldPositions.set(`${d.id}.${fieldName}`, {
                        leftX: d.x,
                        rightX: d.x + cardW,
                        y: centerY
                    });
                });
            } else {
                const fields = Array.from(d.mappedFields.entries()).slice(0, 8);
                const rowHeight = 20;
                const startY = 34;

                fields.forEach(([fieldName, fieldInfo], i) => {
                    const fieldY = d.y + startY + i * rowHeight + rowHeight / 2;
                    fieldPositions.set(`${d.id}.${fieldName}`, {
                        leftX: d.x,
                        rightX: d.x + cardW,
                        y: fieldY
                    });
                });
            }
        });
    }

    drawFieldLevelEdges(edges, nodeMap, cardW, fieldPositions) {
        this.g.selectAll('.edges-group').remove();

        const edgesGroup = this.g.insert('g', '.nodes')
            .attr('class', 'edges-group');

        const fieldEdges = [];
        edges.forEach(edge => {
            const mappings = edge.mappings || [];
            mappings.forEach((m, idx) => {
                fieldEdges.push({
                    ...m,
                    sourceTable: edge.source,
                    targetTable: edge.target,
                    changeType: edge.changeType,
                    edgeIndex: idx,
                    totalMappings: mappings.length
                });
            });
        });

        edgesGroup.selectAll('path')
            .data(fieldEdges)
            .enter()
            .append('path')
            .attr('class', d => {
                const typeClass = this.getMappingTypeClass(d.mapping_type);
                const changeClass = d.changeType || '';
                return `link ${this.currentVersion === 'delta' ? changeClass : typeClass}`;
            })
            .attr('marker-end', d => {
                if (this.currentVersion === 'delta' && d.changeType) return 'url(#arrow)';
                const type = (d.mapping_type || '').toLowerCase();
                if (type.includes('join')) return 'url(#arrow-join)';
                if (type.includes('lookup')) return 'url(#arrow-lookup)';
                if (type.includes('transform') || type.includes('calc')) return 'url(#arrow-transform)';
                return 'url(#arrow-map)';
            })
            .attr('d', d => this.calculateFieldEdgePath(d, nodeMap, cardW, fieldPositions))
            .on('click', (event, d) => this.showMappingDetails(d))
            .append('title')
            .text(d => `${d.source_field} -> ${d.dest_field}${d.rules && d.rules !== 'nan' ? '\\n' + d.rules : ''}`);
    }

    calculateFieldEdgePath(mapping, nodeMap, cardW, fieldPositions) {
        const sourceKey = `${mapping.sourceTable}.${mapping.source_field}`;
        const targetKey = `${mapping.targetTable}.${mapping.dest_field}`;

        let sourcePos = fieldPositions.get(sourceKey);
        let targetPos = fieldPositions.get(targetKey);

        if (!sourcePos) {
            const sourceNode = nodeMap.get(mapping.sourceTable);
            if (sourceNode) {
                sourcePos = { rightX: sourceNode.x + cardW, y: sourceNode.y + sourceNode.cardH / 2 };
            }
        }
        if (!targetPos) {
            const targetNode = nodeMap.get(mapping.targetTable);
            if (targetNode) {
                targetPos = { leftX: targetNode.x, y: targetNode.y + targetNode.cardH / 2 };
            }
        }

        if (!sourcePos || !targetPos) return '';

        const sx = sourcePos.rightX;
        const sy = sourcePos.y;
        const tx = targetPos.leftX;
        const ty = targetPos.y;

        const midX = (sx + tx) / 2;
        return `M${sx},${sy} C${midX},${sy} ${midX},${ty} ${tx},${ty}`;
    }

    updateEdges(edges, nodeMap, cardW, fieldPositions) {
        this.g.selectAll('.edges-group path').attr('d', d =>
            this.calculateFieldEdgePath(d, nodeMap, cardW, fieldPositions)
        );
    }

    buildDeltaGraph() {
        if (!DELTA || !V2_GRAPH) return V1_GRAPH;

        const graph = JSON.parse(JSON.stringify(V2_GRAPH));
        const edgeChanges = new Map();

        DELTA.added.forEach(m => {
            const key = `${m.source_table}.${m.source_field}->${m.dest_table}.${m.dest_field}`;
            edgeChanges.set(key, 'added');
        });
        DELTA.removed.forEach(m => {
            const key = `${m.source_table}.${m.source_field}->${m.dest_table}.${m.dest_field}`;
            if (!edgeChanges.has(key)) edgeChanges.set(key, 'removed');
        });
        DELTA.modified.forEach(m => {
            const key = `${m.source_table}.${m.source_field}->${m.dest_table}.${m.dest_field}`;
            if (!edgeChanges.has(key)) edgeChanges.set(key, 'modified');
        });

        graph.edges.forEach(e => {
            const mappings = e.mappings || [];
            let hasAdded = false, hasRemoved = false, hasModified = false;

            mappings.forEach(m => {
                const key = `${e.source}.${m.source_field}->${e.target}.${m.dest_field}`;
                const change = edgeChanges.get(key);
                if (change === 'added') hasAdded = true;
                if (change === 'removed') hasRemoved = true;
                if (change === 'modified') hasModified = true;
            });

            if (hasAdded) e.changeType = 'added';
            else if (hasModified) e.changeType = 'modified';
            else if (hasRemoved) e.changeType = 'removed';
            else e.changeType = 'unchanged';
        });

        const v2Keys = new Set(V2_GRAPH.edges.map(e => `${e.source}->${e.target}`));
        V1_GRAPH.edges.forEach(e => {
            if (!v2Keys.has(`${e.source}->${e.target}`)) {
                graph.edges.push({...e, changeType: 'removed'});
                if (!graph.nodes[e.source]) graph.nodes[e.source] = V1_GRAPH.nodes[e.source];
                if (!graph.nodes[e.target]) graph.nodes[e.target] = V1_GRAPH.nodes[e.target];
            }
        });

        return graph;
    }

    renderDeltaSummary() {
        if (!DELTA) return;
        const s = DELTA.summary;
        document.getElementById('delta-summary').innerHTML = `
            <span><strong>Added:</strong> ${s.added_count}</span>
            <span><strong>Removed:</strong> ${s.removed_count}</span>
            <span><strong>Modified:</strong> ${s.modified_count}</span>
            <span><strong>Unchanged:</strong> ${s.unchanged_count}</span>
        `;
    }

    showTableDetails(tableName) {
        const panel = document.getElementById('details-panel');
        const body = document.getElementById('panel-body');
        const table = TABLES[tableName];

        if (!table) return;

        document.getElementById('panel-title').textContent = tableName;

        const mappedFieldSet = this.mappedFields.get(tableName);
        const mappedFields = mappedFieldSet ? Array.from(mappedFieldSet) : [];
        const allColumns = table.columns || [];

        body.innerHTML = `
            <div class="panel-section">
                <h4>Connections</h4>
                <p style="font-size:13px;color:#666;">
                    Upstream: ${table.upstream_tables?.length || 0} tables<br>
                    Downstream: ${table.downstream_tables?.length || 0} tables
                </p>
            </div>
            <div class="panel-section">
                <h4>Mapped Fields (${mappedFields.length})</h4>
                <div class="column-list">
                    ${mappedFields.map(fieldName => {
                        const colInfo = allColumns.find(c => c.name === fieldName) || {};
                        return `
                            <div class="column-row">
                                <span class="col-name">${fieldName}</span>
                                <span class="col-badges">
                                    ${colInfo.is_pk ? '<span class="mini-badge pk">PK</span>' : ''}
                                    ${colInfo.is_fk ? '<span class="mini-badge fk">FK</span>' : ''}
                                </span>
                            </div>
                        `;
                    }).join('')}
                </div>
            </div>
            ${allColumns.length > mappedFields.length ? `
                <div class="panel-section">
                    <h4>Other Columns (${allColumns.length - mappedFields.length})</h4>
                    <p style="font-size:12px;color:#999;">
                        ${allColumns.filter(c => !mappedFields.includes(c.name)).map(c => c.name).join(', ')}
                    </p>
                </div>
            ` : ''}
        `;

        panel.classList.add('open');
    }

    showMappingDetails(mapping) {
        const panel = document.getElementById('details-panel');
        const body = document.getElementById('panel-body');

        document.getElementById('panel-title').textContent = 'Field Mapping';

        body.innerHTML = `
            <div class="panel-section">
                <h4>Source</h4>
                <p style="font-size:14px;font-weight:500;">${mapping.sourceTable}.${mapping.source_field}</p>
            </div>
            <div class="panel-section">
                <h4>Destination</h4>
                <p style="font-size:14px;font-weight:500;">${mapping.targetTable}.${mapping.dest_field}</p>
            </div>
            ${mapping.object_name && mapping.object_name !== 'nan' && mapping.object_name !== '' ? `
                <div class="panel-section">
                    <h4>Created By Object</h4>
                    <p style="font-size:13px;font-weight:500;color:#722ed1;">${mapping.object_name}</p>
                </div>
            ` : ''}
            <div class="panel-section">
                <h4>Mapping Type</h4>
                <p style="font-size:13px;">${mapping.mapping_type || 'MAP'}</p>
            </div>
            ${mapping.rules && mapping.rules !== 'nan' ? `
                <div class="panel-section">
                    <h4>Transformation Rule</h4>
                    <div class="mapping-rule">${mapping.rules}</div>
                </div>
            ` : ''}
            ${mapping.changeType ? `
                <div class="panel-section">
                    <h4>Change Status</h4>
                    <span class="badge ${mapping.changeType === 'added' ? 'green' : mapping.changeType === 'removed' ? 'red' : mapping.changeType === 'modified' ? 'yellow' : 'gray'}">${mapping.changeType}</span>
                </div>
            ` : ''}
        `;

        panel.classList.add('open');
    }

    showObjectDetails(objectName) {
        const panel = document.getElementById('details-panel');
        const body = document.getElementById('panel-body');
        const mappings = this.objectsIndex.get(objectName) || [];

        document.getElementById('panel-title').textContent = objectName;

        const byTarget = new Map();
        mappings.forEach(m => {
            if (!byTarget.has(m.targetTable)) {
                byTarget.set(m.targetTable, []);
            }
            byTarget.get(m.targetTable).push(m);
        });

        const sourceTables = [...new Set(mappings.map(m => m.sourceTable))];
        const targetTables = [...new Set(mappings.map(m => m.targetTable))];

        body.innerHTML = `
            <div class="panel-section">
                <h4>Object Type</h4>
                <p style="font-size:13px;">ETL Object (${mappings.length} mappings)</p>
            </div>
            <div class="panel-section">
                <h4>Source Tables (${sourceTables.length})</h4>
                <div class="column-list">
                    ${sourceTables.map(t => `
                        <div class="column-row" style="cursor:pointer" onclick="window.lineageExplorer.selectTable('${t}')">
                            <span class="col-name">${t}</span>
                        </div>
                    `).join('')}
                </div>
            </div>
            <div class="panel-section">
                <h4>Target Tables (${targetTables.length})</h4>
                <div class="column-list">
                    ${targetTables.map(t => `
                        <div class="column-row" style="cursor:pointer" onclick="window.lineageExplorer.selectTable('${t}')">
                            <span class="col-name">${t}</span>
                        </div>
                    `).join('')}
                </div>
            </div>
            <div class="panel-section">
                <h4>Field Mappings</h4>
                ${Array.from(byTarget.entries()).map(([targetTable, maps]) => `
                    <div style="margin-bottom:12px;">
                        <div style="font-size:12px;font-weight:600;color:#1890ff;margin-bottom:6px;">-> ${targetTable}</div>
                        ${maps.map(m => `
                            <div class="mapping-item">
                                <div class="mapping-fields">
                                    <span>${m.sourceTable}.${m.source_field}</span>
                                    <span class="mapping-arrow">-></span>
                                    <span>${m.dest_field}</span>
                                </div>
                                ${m.rules && m.rules !== 'nan' ? `<div class="mapping-rule">${m.rules}</div>` : ''}
                            </div>
                        `).join('')}
                    </div>
                `).join('')}
            </div>
        `;

        panel.classList.add('open');
    }
}

window.lineageExplorer = new LineageExplorer();
'''


def main():
    parser = argparse.ArgumentParser(description="Generate Lineage Explorer from CSV files (No external dependencies)")
    parser.add_argument("--v1", required=True, help="Path to version 1 mappings CSV file")
    parser.add_argument("--v1-name", default="Version 1", help="Display name for version 1")
    parser.add_argument("--v2", help="Path to version 2 mappings CSV file (optional)")
    parser.add_argument("--v2-name", default="Version 2", help="Display name for version 2")
    parser.add_argument("--data-model", help="Path to data model CSV file (optional)")
    parser.add_argument("--output", default="./output", help="Output directory")

    args = parser.parse_args()

    processor = LineageProcessor(
        v1_path=args.v1, v1_name=args.v1_name,
        v2_path=args.v2, v2_name=args.v2_name,
        data_model_path=args.data_model,
    )

    processor.load_data()
    processor.compute_delta()
    processor.build_table_metadata()
    processor.generate_output(args.output)


if __name__ == "__main__":
    main()
