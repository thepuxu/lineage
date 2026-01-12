# Local Data Lineage Explorer - Continuation Notes

## Project Status: COMPLETE

### What It Does
Reads Excel/CSV mapping files (OFSAA format), generates self-contained HTML visualization with:
- Hierarchical DAG layout (SRC → STG → DIM/FCT → RPT)
- Delta comparison (Added/Removed/Modified mappings)
- Search by table/field
- Compact table toggle (collapse to mini card with field count)
- Fully offline (D3.js embedded inline)

---

## Deployment to Work PC (No Network)

### Minimum Files Needed
```
lineage_explorer/
├── generate_lineage_csv.py   # Main generator (84KB)
├── d3.v7.min.js              # D3.js library (273KB)
└── your_mappings.csv         # Your real data
```

### Prerequisites
- Python 3.x (no pip install needed)
- Only uses built-in modules: `csv`, `json`, `pathlib`, `collections`, `typing`

### Run
```bash
python generate_lineage_csv.py --current mappings.csv --output output/
```

### Output
- `output/lineage_explorer.html` (~445KB, fully self-contained)
- Copy this single HTML file anywhere, opens in any browser offline

---

## File Overview

| File | Purpose | Dependencies |
|------|---------|--------------|
| `generate_lineage_csv.py` | CSV-based generator (production) | None |
| `generate_mock_data_csv.py` | Generate test data | None |
| `d3.v7.min.js` | Embedded in HTML | N/A |
| `generate_lineage_v2.py` | Excel-based (needs pandas) | pandas, openpyxl |
| `generate_mock_data.py` | Excel mock data (needs pandas) | pandas |

---

## OFSAA Column Format (17 columns)
```
object_name, destination_table, destination_field, usage_type, usage_role,
source_type, source_table, source_field, constant_value, derived_output,
derived_expression, join_alias, join_keys, join_filters, dm_match, trace_path, notes
```

---

## Features Implemented
- [x] OFSAA format mock data (~30 tables, ~300 columns, ~350 mappings)
- [x] Compact table toggle (click ▼ icon on hover)
- [x] Zero-dependency CSV version
- [x] Offline D3.js embedding
- [x] Delta comparison mode (--previous flag)
- [x] Search highlighting
- [x] Zoom/pan navigation

---

## Quick Test
```bash
cd lineage_explorer
python generate_mock_data_csv.py           # Creates mock_data/mappings.csv
python generate_lineage_csv.py --current mock_data/mappings.csv --output output_offline/
# Open output_offline/lineage_explorer.html in browser
```
