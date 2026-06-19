# scripts/lineage_extraction/python/databricks_sql_parser.py
"""Regex-based parser for Databricks-specific SQL constructs.

Detects Databricks SQL patterns that need migration attention when moving
from Databricks (AWS) to BigQuery (GCP). Produces a structured result with:
  - Source/target tables with database prefixes preserved (not stripped)
  - List of Databricks-specific constructs found
  - Migration complexity assessment (direct / rewrite / manual)
  - Flags for UDF and Delta Lake operations

Design decisions:
  - Pure regex-based approach: zero external dependencies beyond stdlib.
  - Source/target extraction preserves the database prefix (e.g.
    "frontier_bronze.orders" → database="frontier_bronze", table="orders")
    because migration engineers need to see the full qualified name.
  - CTE alias names are detected and excluded from source_tables (same
    logic as sql_parser.py, since CTEs are inline subqueries, not real tables).
  - SQL keywords are filtered from source/target candidates.
  - Complexity is determined by the highest-severity construct found:
    direct < rewrite < manual.
"""

import re
from typing import TypedDict


class DatabricksSQLResult(TypedDict):
    """Structured result from parse_databricks_sql().

    Fields:
        source_db: Database/schema names from source tables.
        source_tables: Bare table names from FROM/JOIN clauses.
        target_db: Database/schema names from target tables.
        target_tables: Bare table names from DML targets.
        constructs_found: All Databricks-specific constructs detected.
        has_udf: Always False for SQL files (UDFs are a PySpark concern).
        has_delta_ops: True if any Delta Lake operations detected.
        complexity: "direct", "rewrite", or "manual".
    """

    source_db: list[str]
    source_tables: list[str]
    target_db: list[str]
    target_tables: list[str]
    constructs_found: list[str]
    has_udf: bool
    has_delta_ops: bool
    complexity: str


# ---------------------------------------------------------------------------
# Construct detection patterns
# ---------------------------------------------------------------------------
# Each tuple is (construct_name, compiled_regex).
# Multi-word patterns are listed first so the regex engine matches
# "LATERAL VIEW" before "LATERAL", "MERGE INTO" before standalone keywords, etc.
_CONSTRUCT_PATTERNS: list[tuple[str, re.Pattern]] = [
    # Multi-word constructs (must come before single-word counterparts)
    ("LATERAL VIEW", re.compile(r"\bLATERAL\s+VIEW\b", re.IGNORECASE)),
    ("MERGE INTO", re.compile(r"\bMERGE\s+INTO\b", re.IGNORECASE)),
    ("RESTORE TABLE", re.compile(r"\bRESTORE\s+TABLE\b", re.IGNORECASE)),
    ("USING DELTA", re.compile(r"\bUSING\s+DELTA\b", re.IGNORECASE)),
    ("CONVERT TO DELTA", re.compile(r"\bCONVERT\s+TO\s+DELTA\b", re.IGNORECASE)),
    # Date functions
    ("DATE_SUB", re.compile(r"\bDATE_SUB\b", re.IGNORECASE)),
    ("DATE_ADD", re.compile(r"\bDATE_ADD\b", re.IGNORECASE)),
    ("DATEDIFF", re.compile(r"\bDATEDIFF\b", re.IGNORECASE)),
    ("CURRENT_DATE", re.compile(r"\bCURRENT_DATE\b", re.IGNORECASE)),
    ("DATE_TRUNC", re.compile(r"\bDATE_TRUNC\b", re.IGNORECASE)),
    # Aggregate / collection
    ("COLLECT_LIST", re.compile(r"\bCOLLECT_LIST\b", re.IGNORECASE)),
    ("COLLECT_SET", re.compile(r"\bCOLLECT_SET\b", re.IGNORECASE)),
    ("ANY_VALUE", re.compile(r"\bANY_VALUE\b", re.IGNORECASE)),
    ("APPROX_COUNT_DISTINCT", re.compile(r"\bAPPROX_COUNT_DISTINCT\b", re.IGNORECASE)),
    # Type casting
    ("TRY_CAST", re.compile(r"\bTRY_CAST\b", re.IGNORECASE)),
    # Window functions (DENSE_RANK and PERCENT_RANK before RANK to avoid
    # any edge-case overlap — though \b prevents matching inside identifiers)
    ("ROW_NUMBER", re.compile(r"\bROW_NUMBER\b", re.IGNORECASE)),
    ("DENSE_RANK", re.compile(r"\bDENSE_RANK\b", re.IGNORECASE)),
    ("PERCENT_RANK", re.compile(r"\bPERCENT_RANK\b", re.IGNORECASE)),
    ("NTILE", re.compile(r"\bNTILE\b", re.IGNORECASE)),
    ("LAG", re.compile(r"\bLAG\b", re.IGNORECASE)),
    ("LEAD", re.compile(r"\bLEAD\b", re.IGNORECASE)),
    ("RANK", re.compile(r"\bRANK\b", re.IGNORECASE)),
    # QUALIFY clause
    ("QUALIFY", re.compile(r"\bQUALIFY\b", re.IGNORECASE)),
    # Complex type expansion
    ("EXPLODE", re.compile(r"\bEXPLODE\b", re.IGNORECASE)),
    ("POSEXPLODE", re.compile(r"\bPOSEXPLODE\b", re.IGNORECASE)),
    ("STACK", re.compile(r"\bSTACK\b", re.IGNORECASE)),
    ("INLINE", re.compile(r"\bINLINE\b", re.IGNORECASE)),
    # Type constructors (require opening paren to avoid false matches)
    ("MAP", re.compile(r"\bMAP\s*\(", re.IGNORECASE)),
    ("ARRAY", re.compile(r"\bARRAY\s*\(", re.IGNORECASE)),
    ("STRUCT", re.compile(r"\bSTRUCT\s*\(", re.IGNORECASE)),
    # Higher-order functions (require opening paren)
    ("TRANSFORM", re.compile(r"\bTRANSFORM\s*\(", re.IGNORECASE)),
    ("FILTER", re.compile(r"\bFILTER\s*\(", re.IGNORECASE)),
    ("AGGREGATE", re.compile(r"\bAGGREGATE\s*\(", re.IGNORECASE)),
    # Pivot operations
    ("PIVOT", re.compile(r"\bPIVOT\b", re.IGNORECASE)),
    ("UNPIVOT", re.compile(r"\bUNPIVOT\b", re.IGNORECASE)),
    # Delta Lake
    ("VACUUM", re.compile(r"\bVACUUM\b", re.IGNORECASE)),
    ("OPTIMIZE", re.compile(r"\bOPTIMIZE\b", re.IGNORECASE)),
]


# ---------------------------------------------------------------------------
# Complexity mapping
# ---------------------------------------------------------------------------
# "direct" = direct BigQuery equivalent exists
# "rewrite" = needs structural rewrite
# "manual" = no clean BigQuery equivalent
_COMPLEXITY_MAP: dict[str, str] = {
    # direct
    "DATE_SUB": "direct",
    "DATE_ADD": "direct",
    "DATEDIFF": "direct",
    "CURRENT_DATE": "direct",
    "DATE_TRUNC": "direct",
    "TRY_CAST": "direct",
    "LAG": "direct",
    "LEAD": "direct",
    "ROW_NUMBER": "direct",
    "RANK": "direct",
    "DENSE_RANK": "direct",
    "NTILE": "direct",
    "PERCENT_RANK": "direct",
    "ANY_VALUE": "direct",
    "APPROX_COUNT_DISTINCT": "direct",
    # rewrite
    "LATERAL VIEW": "rewrite",
    "EXPLODE": "rewrite",
    "POSEXPLODE": "rewrite",
    "STACK": "rewrite",
    "INLINE": "rewrite",
    "PIVOT": "rewrite",
    "UNPIVOT": "rewrite",
    "COLLECT_LIST": "rewrite",
    "COLLECT_SET": "rewrite",
    "MAP": "rewrite",
    "ARRAY": "rewrite",
    "STRUCT": "rewrite",
    "TRANSFORM": "rewrite",
    "FILTER": "rewrite",
    "AGGREGATE": "rewrite",
    # manual
    "QUALIFY": "manual",
    "MERGE INTO": "manual",
    "VACUUM": "manual",
    "OPTIMIZE": "manual",
    "RESTORE TABLE": "manual",
    "USING DELTA": "manual",
    "CONVERT TO DELTA": "manual",
}

_DELTA_CONSTRUCTS: set[str] = {
    "MERGE INTO", "VACUUM", "OPTIMIZE", "RESTORE TABLE",
    "USING DELTA", "CONVERT TO DELTA",
}

# SQL keywords that should never appear as source/target table names.
_KEYWORDS_TO_EXCLUDE: set[str] = {
    "SELECT", "FROM", "WHERE", "JOIN", "ON", "AND", "OR", "NOT",
    "IN", "IS", "NULL", "AS", "GROUP", "BY", "ORDER", "HAVING",
    "LIMIT", "UNION", "ALL", "EXISTS", "CASE", "WHEN", "THEN",
    "ELSE", "END", "SET", "INTO", "VALUES", "INSERT", "UPDATE",
    "DELETE", "CREATE", "DROP", "ALTER", "TABLE", "INDEX", "VIEW",
    "DUAL", "TRUE", "FALSE",
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _strip_comments(sql: str) -> str:
    """Remove SQL block and single-line comments."""
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    return sql


def _split_table_name(qualified: str) -> tuple[str, str]:
    """Split a qualified name into (database, bare_table).

    "frontier_bronze.orders" -> ("frontier_bronze", "orders")
    "catalog.schema.table"   -> ("catalog.schema", "table")
    "orders"                 -> ("", "orders")
    """
    if "." in qualified:
        parts = qualified.rsplit(".", maxsplit=1)
        return (parts[0], parts[1])
    return ("", qualified)


def _extract_cte_names(sql: str) -> set[str]:
    """Extract CTE alias names (lowercase) to exclude from source_tables."""
    names: set[str] = set()
    for match in re.finditer(r"\bWITH\s+(\w+)\s+AS\s*\(", sql, re.IGNORECASE):
        names.add(match.group(1).lower())
    for match in re.finditer(r"\)\s*,\s*(\w+)\s+AS\s*\(", sql, re.IGNORECASE):
        names.add(match.group(1).lower())
    return names


def _is_valid_table(name: str, cte_names: set[str]) -> bool:
    """True if name is not a keyword or CTE alias."""
    clean = name.rstrip(".")
    if clean.upper() in _KEYWORDS_TO_EXCLUDE:
        return False
    if clean.lower() in cte_names:
        return False
    return True


def _detect_constructs(sql: str) -> list[str]:
    """Scan SQL for Databricks-specific construct patterns."""
    found: list[str] = []
    for name, pattern in _CONSTRUCT_PATTERNS:
        if pattern.search(sql):
            found.append(name)
    return found


def _extract_source_tables(sql: str, cte_names: set[str]) -> tuple[list[str], list[str]]:
    """Extract source tables from FROM/JOIN, preserving database prefix."""
    databases: list[str] = []
    tables: list[str] = []

    for pattern in [
        r"\bFROM\s+([a-zA-Z_][\w.]*)",
        r"\bJOIN\s+([a-zA-Z_][\w.]*)",
    ]:
        for match in re.finditer(pattern, sql, re.IGNORECASE):
            candidate = match.group(1)
            if not _is_valid_table(candidate, cte_names):
                continue
            db, tbl = _split_table_name(candidate)
            if db:
                databases.append(db)
            tables.append(tbl)

    return (list(dict.fromkeys(databases)), list(dict.fromkeys(tables)))


def _extract_target_tables(sql: str) -> tuple[list[str], list[str]]:
    """Extract target tables from DML, preserving database prefix."""
    databases: list[str] = []
    tables: list[str] = []

    for pattern in [
        r"\bINSERT\s+INTO\s+([a-zA-Z_][\w.]*)",
        r"\bCREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+([a-zA-Z_][\w.]*)\s+AS\b",
        r"\bMERGE\s+INTO\s+([a-zA-Z_][\w.]*)",
    ]:
        for match in re.finditer(pattern, sql, re.IGNORECASE):
            db, tbl = _split_table_name(match.group(1))
            if db:
                databases.append(db)
            tables.append(tbl)

    return (list(dict.fromkeys(databases)), list(dict.fromkeys(tables)))


def _determine_complexity(constructs_found: list[str]) -> str:
    """Determine migration complexity — highest severity wins."""
    if not constructs_found:
        return "direct"
    levels = {_COMPLEXITY_MAP.get(c, "direct") for c in constructs_found}
    if "manual" in levels:
        return "manual"
    if "rewrite" in levels:
        return "rewrite"
    return "direct"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_databricks_sql(sql_text: str) -> DatabricksSQLResult:
    """Parse SQL text and detect Databricks-specific constructs for migration.

    Steps:
      1. Strip comments
      2. Detect Databricks constructs via regex
      3. Extract CTE names to exclude from sources
      4. Extract source/target tables with database prefix preserved
      5. Determine complexity (highest severity wins)
      6. Set has_delta_ops flag

    Args:
        sql_text: Raw SQL string, potentially with comments.

    Returns:
        A DatabricksSQLResult TypedDict.
    """
    cleaned = _strip_comments(sql_text)
    constructs = _detect_constructs(cleaned)
    cte_names = _extract_cte_names(cleaned)
    source_db, source_tables = _extract_source_tables(cleaned, cte_names)
    target_db, target_tables = _extract_target_tables(cleaned)

    return DatabricksSQLResult(
        source_db=source_db,
        source_tables=source_tables,
        target_db=target_db,
        target_tables=target_tables,
        constructs_found=constructs,
        has_udf=False,
        has_delta_ops=any(c in _DELTA_CONSTRUCTS for c in constructs),
        complexity=_determine_complexity(constructs),
    )
