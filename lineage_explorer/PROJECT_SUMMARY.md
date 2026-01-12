# Local Lineage Explorer - Complete Project Documentation

## Overview

A **local, offline data lineage visualization tool** that reads Excel mapping files and generates a self-contained, interactive HTML visualization similar to DataHub/Atlan. No server required - works directly from `file://` protocol.

### Purpose
- **Impact Analysis**: Understand what downstream reports break when changing a source field
- **Change Management**: Track delta between mapping versions (what was added/removed/modified)
- **Documentation**: Visualize tribal knowledge captured in Excel mapping documents
- **Migration Support**: Ongoing project with weekly/monthly mapping changes

### Target Users
- Data engineers tracking ETL mappings
- Analysts performing impact analysis
- Teams with Excel-based mapping documentation

---

## Quick Start

### Installation
```bash
cd lineage_explorer
pip install pandas openpyxl
```

### Generate Visualization
```bash
# Minimal (single version)
python generate_lineage.py --v1 your_mappings.xlsx

# Full (two versions with delta comparison)
python generate_lineage.py \
    --v1 mappings_v1.xlsx \
    --v1-name "January Baseline" \
    --v2 mappings_v2.xlsx \
    --v2-name "February Changes" \
    --data-model data_model.xlsx

# Open result
start output/lineage_explorer.html   # Windows
open output/lineage_explorer.html    # Mac
```

**Output**: Always saved to `output/lineage_explorer.html`

---

## Features

### Core Features
| Feature | Description |
|---------|-------------|
| Self-contained HTML | All data embedded as JSON, D3.js embedded (no CDN) |
| Three tabs | Version 1, Version 2, Delta comparison |
| Hierarchical DAG layout | Ordered by actual lineage flow (sources left, destinations right) |
| Search | By table name, field name (TABLE.FIELD), or ETL object name |
| Click interactions | Tables show column details, edges show field mappings + rules |
| Delta highlighting | Added (green), Removed (red), Modified (yellow) |
| Drag nodes | Reposition manually, positions preserved on filter changes |
| Pan and zoom | Mouse wheel zoom, drag to pan |

### Interactive Features
| Feature | How to Use |
|---------|------------|
| **Click field in card** | Filters view to that field's upstream/downstream lineage |
| **Click table header** | Selects that table (same as clicking in sidebar) |
| **Click "+N more fields"** | Expands card to show all fields |
| **Click "- collapse"** | Shrinks expanded card back to 8 fields |
| **Double-click header** | Toggles compact mode (header only) |
| **Click connection line** | Shows field-level mappings with transformation rules |

### Filter Options
| Filter | Location | Purpose |
|--------|----------|---------|
| Upstream checkbox | Toolbar | Show/hide upstream tables |
| Downstream checkbox | Toolbar | Show/hide downstream tables |
| Direct only checkbox | Toolbar | Hide JOIN/LOOKUP/TRANSFORM types, show only MAP |
| Delta filters | Toolbar (delta tab) | Filter by Added/Removed/Modified/Unchanged |
| Reset button | Toolbar | Clear all filters and selections |
| Load All button | Toolbar | Force render all tables (bypasses lazy loading) |

### Performance Features
| Feature | Threshold | Behavior |
|---------|-----------|----------|
| **Lazy loading** | >100 tables | Shows empty state until user selects a table |
| **Load All button** | Any | Bypasses lazy loading, renders everything |
| **Position preservation** | Always | Node positions preserved when changing filters |

### Edge/Arrow Positioning
| Scenario | Arrow Position |
|----------|----------------|
| Visible field rows | Points to specific field row center |
| Hidden fields ("+N more") | Points to table header area |
| Collapsed/compact tables | All arrows point to table header area |
| Fallback (field not found) | Points to table header area |

---

## Input File Formats

### Mappings Excel (Required)

Column names are **case-insensitive** and support many variants:

| Internal Name | Accepted Variants |
|---------------|-------------------|
| `source_table` | Source Table, source_table, sourcetable |
| `source_field` | Source Field, source_field, sourcefield |
| `dest_table` | Dest Table, dest_table, destination table, target table |
| `dest_field` | Dest Field, dest_field, destination field, target field |
| `rules` | Rules, rule, transformation, derived_expression |
| `mapping_type` | Mapping Type, type, usage_type |
| `object_name` | Object Name, object, etl object |

**Additional supported columns** (preserved for future use):
- `usage_role`, `source_type`, `constant_value`, `derived_output`
- `join_alias`, `join_keys`, `join_filters`
- `dm_match`, `trace_path`, `notes`

**Example:**
| Source Table | Source Field | Dest Table | Dest Field | Rules | Mapping Type |
|--------------|--------------|------------|------------|-------|--------------|
| src_orders | order_id | stg_orders | order_id | DIRECT | MAP |
| src_orders | amount | stg_orders | amount_usd | amount * fx_rate | MAP |

### Data Model Excel (Optional)

| Column Name | Description |
|-------------|-------------|
| Table Name | Table name (case-insensitive) |
| Column Name | Column/field name |
| Data Type | STRING, INT, DATE, etc. |
| Is PK | TRUE/FALSE - Primary key flag |
| Is FK | TRUE/FALSE - Foreign key flag |
| Description | Column description |

---

## Architecture

### File Structure
```
lineage_explorer/
├── generate_lineage.py        # Main generator script
├── generate_mock_data.py      # Creates sample Excel files for testing
├── requirements.txt           # pandas, openpyxl
├── assets/
│   └── d3.min.js              # Embedded D3.js (no CDN dependency)
├── d3_data.txt                # Alternative: compressed D3.js (gzip+base64)
├── sample_data/
│   ├── mappings_v1.xlsx       # Sample V1 mappings (~125 rows)
│   ├── mappings_v2.xlsx       # Sample V2 mappings (~129 rows)
│   ├── mappings_v1_30k.xlsx   # Large scale test (~30k mappings)
│   ├── mappings_v2_30k.xlsx   # Large scale test (~30k mappings)
│   ├── data_model.xlsx        # Sample data model (217 columns)
│   └── data_model_30k.xlsx    # Large scale data model (17k columns)
├── output/
│   └── lineage_explorer.html  # Generated visualization
└── PROJECT_SUMMARY.md         # This file
```

### D3.js Loading Priority
1. `assets/d3.min.js` (preferred)
2. `d3.min.js` in same directory as script
3. `d3.min.js` in current working directory
4. `d3_data.txt` (compressed, gzip+base64)

---

## Code Reference

### Python Classes & Methods

```python
class LineageProcessor:
    def __init__(v1_path, v1_name, v2_path=None, v2_name=None, data_model_path=None)
    def load_data()              # Reads Excel files, normalizes to UPPERCASE
    def _load_mappings(path)     # Case-insensitive column matching
    def _load_data_model(path)   # Loads data model
    def compute_delta()          # Compares V1 vs V2 mappings
    def build_table_metadata()   # Builds upstream/downstream relationships
    def build_lineage_graph(df, version)  # Creates graph structure
    def generate_output(output_dir)       # Generates HTML file
    def _generate_html(...)      # Builds HTML with embedded D3.js + data
    def _get_css()               # Returns CSS styles
    def _get_js()                # Returns JavaScript application
```

### JavaScript Classes & Methods

```javascript
class LineageExplorer:
    constructor()                       # Initializes state, sets up UI

    // Index Building
    buildMappedFieldsIndex()            # Maps table -> Set of field names
    buildObjectsIndex()                 # Maps object -> array of mappings
    buildSearchIndex()                  # Builds autocomplete index

    // UI Setup
    setupUI()                           # Event listeners for tabs, search, buttons
    showSearchDropdown(query)           # Autocomplete dropdown
    filterTables(query)                 # Sidebar filtering

    // Graph Rendering
    setupGraph()                        # SVG initialization, zoom, arrow markers
    renderGraph()                       # Main render function
    calculateHierarchicalLayout()       # DAG layout with lineage depth
    calculateLineageDepth(nodes, edges) # BFS to order by data flow

    // Lineage Tracing
    traceFieldLineage(graph, table, field, showUp, showDown)   # Field-level
    traceObjectLineage(graph, objectName, showUp, showDown)    # Object-level

    // Selection & Details
    selectTable(tableName)              # Table selection
    selectFromSearch(table, field, obj) # Search result selection
    showTableDetails(tableName)         # Right panel: table info
    showEdgeDetails(edge)               # Right panel: edge mappings
    showMappingDetails(mapping)         # Right panel: single mapping
    showObjectDetails(objectName)       # Right panel: object info

    // Card Management
    toggleCompact(tableName)            # Collapse/expand table card
    calculateCardHeight(fieldCount, tableName)  # Dynamic card height
    getMappedFieldsForTable(tableName, graph)   # Get fields with mappings

    // Position Management
    updateFieldPositions(nodes, cardW, fieldPositions)  # Update on drag
    updateEdges(edges, nodeMap, cardW, fieldPositions)  # Update edge paths
    calculateFieldEdgePath(mapping, ...)                # Bezier curve path

    // Delta
    buildDeltaGraph()                   # Merge V1+V2 with change types
    renderDeltaSummary()                # Delta stats in toolbar
```

---

## Design Decisions

### 1. Self-Contained HTML
- All data embedded as JSON in `<script>` tags
- D3.js embedded inline (no CDN)
- **Reason**: Avoids CORS issues with `file://` protocol
- **Tradeoff**: Larger HTML file, but works completely offline

### 2. Hierarchical DAG Layout
- Not force-directed (no "bouncy balls" physics)
- Tables ordered by actual lineage depth (BFS from sources)
- Dragging moves nodes smoothly without simulation
- **Reason**: User requested stable, predictable layout

### 3. Case Normalization
- All table/field names normalized to UPPERCASE
- **Reason**: Excel mappings often have inconsistent casing
- **Location**: `_load_mappings()` method

### 4. Delta Logic
- **Identity key**: `(source_table, source_field, dest_table, dest_field)`
- If key in both but rules/type differ → **Modified**
- If key only in V1 → **Removed**
- If key only in V2 → **Added**

### 5. Lazy Loading
- Threshold: >100 tables triggers lazy mode
- Shows empty state with instructions
- User can click "Load All" to bypass
- **Reason**: Prevents browser freeze on large datasets

### 6. Edge Positioning
- Field-level arrows for precision
- Fallback to header center for hidden/compact fields
- **Reason**: Visual clarity, avoid misleading connections

---

## Edge Cases & Error Handling

### Data Loading
| Edge Case | Handling |
|-----------|----------|
| Missing columns | Default values: `object_name=""`, `rules=""`, `mapping_type="MAP"` |
| Empty values | `fillna('')` before normalization |
| Non-string values | `astype(str)` conversion |
| Missing V2 file | Works with V1 only, delta tab hidden |
| Missing data model | Works without column metadata |

### Graph Rendering
| Edge Case | Handling |
|-----------|----------|
| Circular dependencies | BFS handles cycles, uses max depth |
| Disconnected nodes | Assigned depth 0 |
| No root nodes | Start BFS from all nodes |
| Empty graph | Early return, no render |
| Diamond patterns | Uses max depth to position correctly |

### Field Positioning
| Edge Case | Handling |
|-----------|----------|
| Field not in visible rows | Uses header position (y + 16) |
| Compact mode | All fields use header position |
| No field position found | Fallback to header center |

---

## Testing & Validation

### Generate Test Data
```bash
cd lineage_explorer

# Generate small sample data
python generate_mock_data.py

# Generate 30k scale test data
python generate_mock_data.py --scale 30k
```

### Validation Checklist

#### Basic Functionality
- [ ] Generate HTML from sample data: `python generate_lineage.py --v1 sample_data/mappings_v1.xlsx`
- [ ] Open `output/lineage_explorer.html` in browser
- [ ] Verify sidebar shows table list
- [ ] Click a table → verify graph renders
- [ ] Click a field in card → verify field-level lineage
- [ ] Click "+N more fields" → verify card expands
- [ ] Click "- collapse" → verify card shrinks
- [ ] Double-click header → verify compact mode
- [ ] Drag a node → verify position is preserved
- [ ] Click Reset button → verify all cleared

#### Delta View
- [ ] Generate with two versions: `--v1 ... --v2 ...`
- [ ] Click Delta tab → verify summary shows
- [ ] Verify Added (green), Removed (red), Modified (yellow) highlighting
- [ ] Toggle filter checkboxes → verify filtering works

#### Performance
- [ ] Generate with 30k mappings: `--v1 sample_data/mappings_v1_30k.xlsx`
- [ ] Verify lazy loading message appears
- [ ] Click "Load All" → verify renders (may be slow)
- [ ] Select a table → verify renders quickly

#### Search
- [ ] Search by table name → verify dropdown and filtering
- [ ] Search by field name (TABLE.FIELD) → verify field lineage
- [ ] Search by object name → verify object lineage
- [ ] Use arrow keys + Enter → verify keyboard navigation

### Python Syntax Check
```bash
python -m py_compile generate_lineage.py
```

### Validate Generated HTML
```python
import re

def validate_html(html_path):
    with open(html_path, 'r', encoding='utf-8') as f:
        html = f.read()

    errors = []

    # 1. D3.js embedded (not CDN)
    if 'https://d3js.org' in html:
        errors.append("D3.js should be embedded, not loaded from CDN")

    # 2. Data embedded as JSON
    if 'const V1_GRAPH' not in html:
        errors.append("V1_GRAPH data not embedded")
    if 'const TABLES' not in html:
        errors.append("TABLES metadata not embedded")

    # 3. UI elements exist
    required_elements = [
        'id="show-only-maps"',
        'id="reset-filters"',
        'id="load-all"',
        'id="search"',
        'id="graph"'
    ]
    for elem in required_elements:
        if elem not in html:
            errors.append(f"Missing element: {elem}")

    return errors

# Usage: errors = validate_html("output/lineage_explorer.html")
```

---

## Command Line Reference

```bash
python generate_lineage.py [OPTIONS]

Required:
  --v1 PATH              Path to version 1 mappings Excel file

Optional:
  --v1-name NAME         Display name for version 1 (default: "Version 1")
  --v2 PATH              Path to version 2 mappings Excel file
  --v2-name NAME         Display name for version 2 (default: "Version 2")
  --data-model PATH      Path to data model Excel file
  --output DIR           Output directory (default: "./output")
```

### Examples
```bash
# Single version
python generate_lineage.py --v1 mappings.xlsx --v1-name "Current"

# Two versions with delta
python generate_lineage.py \
    --v1 mappings_jan.xlsx --v1-name "January" \
    --v2 mappings_feb.xlsx --v2-name "February"

# With data model
python generate_lineage.py \
    --v1 mappings.xlsx \
    --data-model data_model.xlsx

# Custom output directory
python generate_lineage.py --v1 mappings.xlsx --output ./my_output
```

---

## Known Limitations

1. **Version Comparison**: Only supports 2 versions at a time
   - Future: Timeline/history view for multiple versions

2. **Export**: No way to export graph as image/PDF yet
   - Future: Add screenshot/export functionality

3. **Layer Detection**: Uses prefix-based detection (src_, stg_, dim_, fact_, rpt_)
   - Lineage flow ordering now provides better layout regardless of naming

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "D3.js not found" error | Place `d3.min.js` in `assets/` folder or `d3_data.txt` next to script |
| Browser shows blank page | Check browser console for errors, ensure JavaScript is enabled |
| Graph doesn't render | Large dataset? Wait for lazy loading message, select a table or click "Load All" |
| Wrong case in table names | All names normalized to UPPERCASE - check your Excel files |
| Missing mappings | Check column names match expected variants (see Input File Formats) |
| Edges point wrong | Hidden fields/compact mode edges point to header by design |

---

## Change Log

### Features Implemented
1. Case normalization (UPPERCASE)
2. Lazy loading for large datasets (>100 tables)
3. Toggle to show only direct mappings (hide JOIN/LOOKUP/TRANSFORM)
4. Left-to-right ordering by actual lineage flow
5. Preserve node positions on filter changes
6. Reset/clear all filters button
7. Click field in card → field-level lineage filter
8. Click table header → select table
9. Click "+N more fields" → expand card to show all fields
10. Smart edge positioning (collapsed/hidden fields → header)

---

## Contact / Context

- **Use Case**: Data migration project with changing mappings
- **Update Frequency**: Weekly/monthly mapping changes
- **Audience**: User + team (shareable via HTML files)
- **Environment**: Local Windows PC, no network access for deployment
