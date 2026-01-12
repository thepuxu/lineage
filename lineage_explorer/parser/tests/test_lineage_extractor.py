"""
Test suite for lineage extractor.

Run with:
    python -m pytest lineage_explorer/parser/tests/test_lineage_extractor.py -v

Or directly:
    python lineage_explorer/parser/tests/test_lineage_extractor.py
"""

import sys
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from lineage_explorer.parser.processor import LineageExtractor
from lineage_explorer.parser.loader import SQLFileLoader
from lineage_explorer.parser.models.enums import SourceType, UsageType, UsageRole, TransformationType


def test_simple_select():
    """Test simple SELECT with direct column mappings."""
    sql = """
    SELECT
        A.COL1,
        A.COL2 AS ALIAS_COL,
        B.COL3
    FROM
        TABLE_A A
        INNER JOIN TABLE_B B ON A.ID = B.A_ID
    """

    extractor = LineageExtractor()
    result = extractor.extract(sql, "TEST_OBJECT", "TARGET_TABLE")

    assert len(result.edges) > 0, "Should extract at least one edge"

    # Check for column mappings
    mapping_edges = [e for e in result.edges if e.usage_type == UsageType.MAPPING]
    assert len(mapping_edges) >= 3, "Should have at least 3 column mappings"

    # Check for join dependencies
    join_edges = [e for e in result.edges if e.usage_type == UsageType.JOIN]
    assert len(join_edges) >= 1, "Should have at least 1 join dependency"

    print("test_simple_select: PASSED")


def test_constants():
    """Test constant detection (NULL, SYSDATE, literals)."""
    sql = """
    SELECT
        A.COL1,
        NULL AS NULL_COL,
        SYSDATE AS DATE_COL,
        'LITERAL' AS STR_COL,
        123 AS NUM_COL
    FROM TABLE_A A
    """

    extractor = LineageExtractor()
    result = extractor.extract(sql, "TEST_OBJECT", "TARGET_TABLE")

    # Check for constants
    constant_edges = [e for e in result.edges if e.source_type == SourceType.CONSTANT]
    assert len(constant_edges) >= 3, f"Should have at least 3 constants, found {len(constant_edges)}"

    print("test_constants: PASSED")


def test_case_expression():
    """Test CASE WHEN transformation detection."""
    sql = """
    SELECT
        CASE
            WHEN A.STATUS = 'A' THEN 'ACTIVE'
            ELSE 'INACTIVE'
        END AS STATUS_DESC
    FROM TABLE_A A
    """

    extractor = LineageExtractor()
    result = extractor.extract(sql, "TEST_OBJECT", "TARGET_TABLE")

    # Check for conditional transformation
    case_edges = [e for e in result.edges if e.transformation == TransformationType.CONDITIONAL]
    assert len(case_edges) >= 1, "Should detect CASE as CONDITIONAL transformation"

    print("test_case_expression: PASSED")


def test_aggregate_functions():
    """Test aggregate function detection."""
    sql = """
    SELECT
        A.GROUP_COL,
        SUM(A.AMOUNT) AS TOTAL_AMOUNT,
        COUNT(*) AS ROW_COUNT,
        AVG(A.VALUE) AS AVG_VALUE
    FROM TABLE_A A
    GROUP BY A.GROUP_COL
    """

    extractor = LineageExtractor()
    result = extractor.extract(sql, "TEST_OBJECT", "TARGET_TABLE")

    # Check for aggregate transformations
    agg_edges = [e for e in result.edges if e.transformation == TransformationType.AGGREGATE]
    assert len(agg_edges) >= 2, f"Should detect at least 2 aggregate functions, found {len(agg_edges)}"

    print("test_aggregate_functions: PASSED")


def test_window_functions():
    """Test window function detection."""
    sql = """
    SELECT
        A.COL1,
        ROW_NUMBER() OVER (PARTITION BY A.GROUP_COL ORDER BY A.DATE_COL) AS RN
    FROM TABLE_A A
    """

    extractor = LineageExtractor()
    result = extractor.extract(sql, "TEST_OBJECT", "TARGET_TABLE")

    # Check for window transformation
    window_edges = [e for e in result.edges if e.transformation == TransformationType.WINDOW]
    assert len(window_edges) >= 1, "Should detect ROW_NUMBER as WINDOW transformation"

    print("test_window_functions: PASSED")


def test_join_key_vs_filter():
    """Test join key vs filter classification."""
    sql = """
    SELECT A.COL1
    FROM TABLE_A A
    INNER JOIN TABLE_B B ON A.ID = B.A_ID AND B.STATUS = 'ACTIVE'
    """

    extractor = LineageExtractor()
    result = extractor.extract(sql, "TEST_OBJECT", "TARGET_TABLE")

    join_edges = [e for e in result.edges if e.usage_type == UsageType.JOIN]

    # Should have both key and filter
    key_edges = [e for e in join_edges if e.usage_role == UsageRole.JOIN_KEY]
    filter_edges = [e for e in join_edges if e.usage_role == UsageRole.JOIN_FILTER]

    assert len(key_edges) >= 1, "Should detect join key (A.ID = B.A_ID)"
    assert len(filter_edges) >= 1, "Should detect join filter (B.STATUS = 'ACTIVE')"

    print("test_join_key_vs_filter: PASSED")


def test_cte():
    """Test CTE (WITH clause) handling."""
    sql = """
    WITH CTE_DATA AS (
        SELECT A.ID, A.VALUE
        FROM TABLE_A A
        WHERE A.STATUS = 'A'
    )
    SELECT
        C.ID,
        C.VALUE
    FROM CTE_DATA C
    """

    extractor = LineageExtractor()
    result = extractor.extract(sql, "TEST_OBJECT", "TARGET_TABLE")

    # Should extract mappings from both CTE and main query
    assert len(result.edges) > 0, "Should extract edges from CTE query"

    print("test_cte: PASSED")


def test_schema_dm_match():
    """Test DM match with provided schema."""
    schema = {
        "TABLE_A": {"COL1": "VARCHAR2", "COL2": "NUMBER"},
        "TABLE_B": {"COL3": "DATE"}
    }

    sql = """
    SELECT A.COL1, A.COL2, B.COL3, A.MISSING_COL
    FROM TABLE_A A
    JOIN TABLE_B B ON A.ID = B.ID
    """

    extractor = LineageExtractor(schema=schema)
    result = extractor.extract(sql, "TEST_OBJECT", "TARGET_TABLE")

    # Check DM match
    matched = [e for e in result.edges if e.dm_match == "Y"]
    unmatched = [e for e in result.edges if e.dm_match == "N" and e.source_column == "MISSING_COL"]

    assert len(matched) >= 2, "Should match COL1, COL2, COL3 in schema"
    # MISSING_COL should not match

    print("test_schema_dm_match: PASSED")


def test_simple_lineage():
    """Test simple lineage extraction (regex-based)."""
    sql = """
    SELECT
        TABLE_A.COL1,
        TABLE_B.COL2
    FROM TABLE_A
    JOIN TABLE_B ON TABLE_A.ID = TABLE_B.A_ID
    """

    extractor = LineageExtractor()
    result = extractor.extract_simple_lineage(sql, "TEST_OBJECT", "TARGET_TABLE")

    # Should find TABLE.COLUMN patterns
    assert len(result.edges) >= 4, "Should extract at least 4 column references"

    print("test_simple_lineage: PASSED")


def test_sample_file():
    """Test with actual sample SQL file."""
    sample_path = Path(__file__).parent / "sql_samples" / "T2T_LOAN_CONTRACTS.sql"

    if not sample_path.exists():
        print("test_sample_file: SKIPPED (sample file not found)")
        return

    loader = SQLFileLoader()
    object_name, sql_content = loader.load_file(sample_path)

    # Normalize
    sql_normalized = SQLFileLoader.extract_select(sql_content)

    extractor = LineageExtractor()
    result = extractor.extract(sql_normalized, object_name, "FCT_LOAN_CONTRACTS")

    # Basic checks
    assert len(result.edges) > 0, "Should extract edges from sample file"

    # Check for expected patterns
    mapping_edges = [e for e in result.edges if e.usage_type == UsageType.MAPPING]
    join_edges = [e for e in result.edges if e.usage_type == UsageType.JOIN]
    constant_edges = [e for e in result.edges if e.source_type == SourceType.CONSTANT]

    print(f"  Total edges: {len(result.edges)}")
    print(f"  Mapping edges: {len(mapping_edges)}")
    print(f"  Join edges: {len(join_edges)}")
    print(f"  Constant edges: {len(constant_edges)}")

    assert len(mapping_edges) >= 5, "Sample should have at least 5 column mappings"
    assert len(join_edges) >= 2, "Sample should have at least 2 join conditions"
    assert len(constant_edges) >= 2, "Sample should have at least 2 constants (SYSDATE, 'BATCH_001')"

    print("test_sample_file: PASSED")


def run_all_tests():
    """Run all tests."""
    print("=" * 60)
    print("SQL Lineage Extractor Tests")
    print("=" * 60)
    print()

    tests = [
        test_simple_select,
        test_constants,
        test_case_expression,
        test_aggregate_functions,
        test_window_functions,
        test_join_key_vs_filter,
        test_cte,
        test_schema_dm_match,
        test_simple_lineage,
        test_sample_file,
    ]

    passed = 0
    failed = 0
    skipped = 0

    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"{test.__name__}: FAILED - {e}")
            failed += 1
        except Exception as e:
            print(f"{test.__name__}: ERROR - {e}")
            failed += 1

    print()
    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)

    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
