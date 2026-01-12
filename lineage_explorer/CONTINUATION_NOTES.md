# Continuation Notes for Claude

## Last Session Summary (Jan 2026)

### What Was Built
A local data lineage explorer that:
1. Reads Excel mapping files (Source→Dest field mappings)
2. Generates self-contained HTML visualization
3. Shows delta between two versions (Added/Removed/Modified)
4. Uses hierarchical layout (not force-directed physics)
5. Supports search by table name AND field name

### Files to Know About
- `generate_lineage_v2.py` - **THE MAIN FILE** - generates the HTML output
- `generate_mock_data.py` - Creates test Excel files
- `output/lineage_explorer.html` - The generated visualization

### Important Technical Decisions
1. **Embedded Data**: All JSON embedded in HTML to avoid fetch/CORS issues with file:// protocol
2. **Hierarchical Layout**: Tables positioned by layer (SRC→STG→DIM/FACT→RPT), no physics
3. **Delta Key**: `(source_table, source_field, dest_table, dest_field)` uniquely identifies a mapping
4. **Drag Behavior**: Stores positions in `nodePositions` Map, persists during session

### User Requirements Gathered
- Input: Excel files with columns: Source Table, Source Field, Dest Table, Dest Field, Rules, Mapping Type
- Optional: Data model Excel with: Table Name, Column Name, Data Type, Is PK, Is FK
- Scale: Expect 100k+ mappings (not yet tested at this scale)
- Offline: Must work without network access
- Audience: User + team (shareable by zipping output folder)

### User Feedback Already Addressed
1. ✅ No bouncing physics (switched to hierarchical)
2. ✅ Search by field name (added dual search)
3. ✅ DataHub-style UI (light theme, cards, shadows)
4. ✅ All tables showing (fixed data loading)

### Potential Next Steps (Not Yet Started)
- Test with real user data (when they provide Excel format)
- Scale testing with large datasets
- Consider lazy loading for 100k+ mappings
- Bundle D3.js for true offline (currently uses CDN)
- Add image/PDF export
- Consider version timeline view (beyond 2-version comparison)

### How to Test Current State
```bash
cd "c:/Users/EYUSER/Project/Local Lignage Explorer/lineage_explorer"
python generate_lineage_v2.py --v1 sample_data/mappings_v1.xlsx --v1-name "V1" --v2 sample_data/mappings_v2.xlsx --v2-name "V2" --data-model sample_data/data_model.xlsx --output output
start output/lineage_explorer.html
```

### Code Structure Quick Reference

```
generate_lineage_v2.py
├── LineageProcessor class
│   ├── load_data() - reads Excel with pandas
│   ├── compute_delta() - compares V1 vs V2 mappings
│   ├── build_table_metadata() - indexes all tables/columns
│   ├── build_lineage_graph() - creates nodes/edges structure
│   ├── generate_output() - orchestrates HTML generation
│   ├── _generate_html() - builds HTML with embedded JSON
│   ├── _get_css() - returns CSS string (~400 lines)
│   └── _get_js() - returns JavaScript string (~450 lines)
└── main() - CLI argument parsing
```

### JavaScript App Structure (inside HTML)
```
LineageExplorer class
├── init() → setupUI() → renderTableList() → setupGraph() → renderGraph()
├── filterTables(query) - searches tables AND fields
├── selectTable(name) - focuses lineage on one table
├── calculateHierarchicalLayout() - positions nodes by layer
├── renderGraph() - draws SVG with D3.js
├── showTableDetails() - right panel content
└── showEdgeDetails() - right panel for connections
```
