"""Regex-based SQL parser that extracts lineage metadata from SQL strings.

This module provides a single public function `parse_sql(sql_text)` that
returns a ParsedSQL TypedDict with source/target tables, key columns,
KPI metrics, job type, and the original transformation logic.

Design decisions:
  - Pure regex-based approach: zero external dependencies beyond stdlib.
    This is intentional so the parser can be used both directly on .sql
    pipeline files AND by the PySpark parser for spark.sql("...") calls.
  - CTE (Common Table Expression) names are detected and filtered out of
    source_tables, because CTEs are inline subqueries, not real tables.
  - Table names can be 1-part (table), 2-part (schema.table), or 3-part
    (catalog.schema.table) -- all are captured as a single string.
  - Deduplication uses `list(dict.fromkeys(...))` which preserves insertion
    order while removing duplicates (Python 3.7+ dict ordering guarantee).
"""

import re
from typing import TypedDict


class ParsedSQL(TypedDict):
    """Structured result returned by parse_sql().

    Fields:
        source_tables: Tables read from (FROM, JOIN clauses).
        target_tables: Tables written to (INSERT INTO, CREATE TABLE AS, MERGE INTO).
        key_columns: Column names used in JOIN ON and WHERE conditions.
        kpi_metrics: Aliases of aggregate expressions (SUM/AVG/COUNT/MAX/MIN ... AS alias).
        job_type: "aggregation" if GROUP BY or aggregate functions are present,
                  otherwise "curated".
        transformation_logic: The original SQL text (stored verbatim for downstream use).
    """

    source_tables: list[str]
    target_tables: list[str]
    key_columns: list[str]
    kpi_metrics: list[str]
    job_type: str
    transformation_logic: str


# SQL keywords that should never be treated as table names.
# These are common tokens that appear after FROM/JOIN in parsed output
# but represent SQL constructs, not actual tables.
_KEYWORDS_TO_EXCLUDE: set[str] = {
    "SELECT", "FROM", "WHERE", "JOIN", "ON", "AND", "OR", "NOT",
    "IN", "IS", "NULL", "AS", "GROUP", "BY", "ORDER", "HAVING",
    "LIMIT", "UNION", "ALL", "EXISTS", "CASE", "WHEN", "THEN",
    "ELSE", "END", "SET", "INTO", "VALUES", "INSERT", "UPDATE",
    "DELETE", "CREATE", "DROP", "ALTER", "TABLE", "INDEX", "VIEW",
    "DUAL", "TRUE", "FALSE",
}


def _strip_comments(sql: str) -> str:
    """Remove SQL comments from the input string.

    Handles two comment styles:
      - Single-line: -- comment text (until end of line)
      - Block: /* comment text */

    Block comments are removed first so that a -- inside a block comment
    doesn't cause premature stripping.  We use re.DOTALL so that the
    block-comment pattern can span multiple lines.

    Args:
        sql: Raw SQL text potentially containing comments.

    Returns:
        The SQL text with all comments removed.
    """
    # Remove /* ... */ block comments (may span multiple lines)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    # Remove -- single-line comments (from -- to end of line)
    sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    return sql


def _extract_cte_names(sql: str) -> set[str]:
    """Extract CTE (Common Table Expression) alias names from WITH clauses.

    A CTE looks like: WITH cte_name AS (SELECT ...), other_cte AS (...)
    We extract just the alias names so they can be excluded from source_tables.
    CTEs are subqueries defined inline -- they are NOT real database tables.

    The regex finds word tokens immediately before 'AS' that follow 'WITH'
    or a comma within a WITH block.

    Args:
        sql: SQL text (ideally comment-stripped).

    Returns:
        A set of CTE alias names in lowercase (for case-insensitive comparison).
    """
    # Match patterns like: WITH name AS (, or , name AS (
    # We look for identifiers (including dots, though CTE names rarely have dots)
    # that appear right before AS ( with optional whitespace.
    cte_pattern = r"\bWITH\s+(\w+)\s+AS\s*\("
    # Also match additional CTEs separated by commas: , name AS (
    additional_cte_pattern = r"\)\s*,\s*(\w+)\s+AS\s*\("

    names: set[str] = set()
    for match in re.finditer(cte_pattern, sql, re.IGNORECASE):
        names.add(match.group(1).lower())
    for match in re.finditer(additional_cte_pattern, sql, re.IGNORECASE):
        names.add(match.group(1).lower())
    return names


def _strip_database_prefix(name: str) -> str:
    """Strip database/schema prefix from a qualified table name.

    Converts catalog.schema.table or schema.table → table.
    A plain table name (no dots) is returned as-is.

    Args:
        name: A possibly-qualified table name (e.g., 'frontier_bronze.orders').

    Returns:
        The bare table name without any database/schema prefix.
    """
    return name.rsplit(".", maxsplit=1)[-1] if "." in name else name


def _extract_source_tables(sql: str, cte_names: set[str]) -> list[str]:
    """Extract table names from FROM and JOIN clauses.

    This function looks for table references after FROM and JOIN keywords.
    Table names can be:
      - Simple: orders
      - Schema-qualified: frontier_bronze.orders
      - Catalog-qualified: catalog.schema.table

    It excludes:
      - CTE alias names (they're subqueries, not real tables)
      - SQL keywords (SELECT, DUAL, etc.)
      - Subquery markers (the SELECT that appears in subqueries)

    The regex captures identifiers that can contain dots (for qualified names)
    but stops at whitespace, parentheses, commas, and aliases.

    Args:
        sql: Comment-stripped SQL text.
        cte_names: Set of CTE alias names to exclude (lowercase).

    Returns:
        Deduplicated list of source table names, in order of first appearance.
    """
    tables: list[str] = []

    # Pattern for FROM clause: FROM <table> (optional alias)
    # Matches: FROM table_name, FROM schema.table, FROM catalog.schema.table
    # The table name can contain dots and word chars. It stops at whitespace
    # that is followed by ON, WHERE, JOIN, or end-of-string (alias boundary).
    from_pattern = r"\bFROM\s+([a-zA-Z_][\w.]*)"
    for match in re.finditer(from_pattern, sql, re.IGNORECASE):
        table_name = match.group(1)
        if _is_valid_table(table_name, cte_names):
            tables.append(_strip_database_prefix(table_name))

    # Pattern for JOIN clauses: [LEFT|RIGHT|INNER|OUTER|CROSS|FULL] JOIN <table>
    # We match the JOIN keyword followed by the table name.
    join_pattern = r"\bJOIN\s+([a-zA-Z_][\w.]*)"
    for match in re.finditer(join_pattern, sql, re.IGNORECASE):
        table_name = match.group(1)
        if _is_valid_table(table_name, cte_names):
            tables.append(_strip_database_prefix(table_name))

    # Deduplicate while preserving order
    return list(dict.fromkeys(tables))


def _is_valid_table(name: str, cte_names: set[str]) -> bool:
    """Check whether a name is a valid table (not a keyword or CTE alias).

    Args:
        name: The extracted table name candidate.
        cte_names: Set of CTE alias names to exclude (lowercase).

    Returns:
        True if the name should be included as a source table.
    """
    # Strip any trailing dots (defensive -- shouldn't happen with our regex)
    name_clean = name.rstrip(".")
    # Reject SQL keywords (case-insensitive)
    if name_clean.upper() in _KEYWORDS_TO_EXCLUDE:
        return False
    # Reject CTE alias names (already lowercase in the set)
    if name_clean.lower() in cte_names:
        return False
    return True


def _extract_target_tables(sql: str) -> list[str]:
    """Extract table names from DML write statements.

    Detects three patterns:
      1. INSERT INTO <table> — standard insert
      2. CREATE [OR REPLACE] TABLE <table> AS — CTAS
      3. MERGE INTO <table> — merge/upsert

    Args:
        sql: Comment-stripped SQL text.

    Returns:
        Deduplicated list of target table names.
    """
    tables: list[str] = []

    # INSERT INTO <table>
    for match in re.finditer(r"\bINSERT\s+INTO\s+([a-zA-Z_][\w.]*)", sql, re.IGNORECASE):
        tables.append(_strip_database_prefix(match.group(1)))

    # CREATE [OR REPLACE] TABLE <table> AS
    for match in re.finditer(
        r"\bCREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+([a-zA-Z_][\w.]*)\s+AS\b",
        sql,
        re.IGNORECASE,
    ):
        tables.append(_strip_database_prefix(match.group(1)))

    # MERGE INTO <table>
    for match in re.finditer(r"\bMERGE\s+INTO\s+([a-zA-Z_][\w.]*)", sql, re.IGNORECASE):
        tables.append(_strip_database_prefix(match.group(1)))

    return list(dict.fromkeys(tables))


def _extract_key_columns(sql: str) -> list[str]:
    """Extract column names from JOIN ON and WHERE conditions.

    For JOIN ON: captures columns on both sides of the = sign.
    For WHERE: captures ALL columns used in comparison conditions throughout
    the entire WHERE clause (not just the first one).

    The approach is:
      1. Extract the full WHERE clause body (from WHERE up to GROUP BY,
         ORDER BY, HAVING, LIMIT, or end of string).
      2. Scan for aliased columns (table.col) and bare columns that appear
         before comparison operators (=, <, >, !=, <=, >=, <>).
      3. Filter out SQL keywords.

    Column names may have table alias prefixes (e.g., o.cust_id); we strip
    the alias prefix and keep only the bare column name.

    SQL keywords (AND, OR, NOT, NULL, etc.) are filtered out so that
    they don't pollute the key_columns list.

    Args:
        sql: Comment-stripped SQL text.

    Returns:
        Deduplicated list of bare column names.
    """
    columns: list[str] = []

    # --- JOIN ON columns ---
    # Match patterns like: ON t1.col1 = t2.col2
    # We capture both sides of the equality.
    on_pattern = r"\bON\s+([\w.]+)\s*=\s*([\w.]+)"
    for match in re.finditer(on_pattern, sql, re.IGNORECASE):
        for group in (match.group(1), match.group(2)):
            col = _bare_column(group)
            if col and col.upper() not in _KEYWORDS_TO_EXCLUDE:
                columns.append(col)

    # --- WHERE columns ---
    # Step 1: Extract the full WHERE clause body.
    # The WHERE clause runs from WHERE keyword up to GROUP BY, ORDER BY,
    # HAVING, LIMIT, or the end of the string — whichever comes first.
    where_body_match = re.search(
        r"\bWHERE\s+(.*?)(?:\bGROUP\s+BY\b|\bORDER\s+BY\b|\bHAVING\b|\bLIMIT\b|$)",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    if where_body_match:
        where_body = where_body_match.group(1)

        # Step 2: Scan for columns throughout the entire WHERE clause body.
        # Match aliased columns (table.col) that appear before comparison operators.
        # Pattern: word.word followed by optional spaces and a comparison operator.
        for m in re.finditer(
            r"([\w]+\.[\w]+)\s*(?:[=<>!]=?|<>)",
            where_body,
        ):
            col = _bare_column(m.group(1))
            if col and col.upper() not in _KEYWORDS_TO_EXCLUDE:
                columns.append(col)

        # Match bare columns (no dot prefix) that appear before comparison operators.
        # We use a negative lookbehind for word chars and dots to avoid grabbing
        # the tail end of an aliased column (e.g., the "col" in "t.col = ...").
        for m in re.finditer(
            r"(?<![.\w])([\w]+)\s*(?:[=<>!]=?|<>)",
            where_body,
        ):
            col = m.group(1)
            if col and col.upper() not in _KEYWORDS_TO_EXCLUDE:
                columns.append(col)

    return list(dict.fromkeys(columns))


def _bare_column(name: str) -> str:
    """Strip table alias prefix from a possibly-qualified column name.

    Examples:
        o.cust_id -> cust_id
        orders.cust_id -> cust_id
        cust_id -> cust_id

    If the name is a standalone identifier (no dot), it is returned as-is.

    Args:
        name: A possibly alias-qualified column name (e.g., 'o.cust_id').

    Returns:
        The bare column name without the alias prefix.
    """
    if "." in name:
        return name.rsplit(".", maxsplit=1)[-1]
    return name


def _detect_job_type(sql: str) -> str:
    """Detect whether the SQL represents an aggregation or a curated pipeline.

    Aggregation pipelines have at least one of:
      - GROUP BY clause
      - Aggregate functions: SUM, COUNT, AVG, MAX, MIN

    Curated pipelines are simple transformations (SELECT, JOIN, WHERE) without
    any aggregation logic.

    Args:
        sql: Comment-stripped SQL text.

    Returns:
        "aggregation" if aggregation keywords are found, else "curated".
    """
    # Check for GROUP BY clause
    if re.search(r"\bGROUP\s+BY\b", sql, re.IGNORECASE):
        return "aggregation"

    # Check for aggregate functions: SUM(...), COUNT(...), AVG(...), MAX(...), MIN(...)
    agg_pattern = r"\b(SUM|COUNT|AVG|MAX|MIN)\s*\("
    if re.search(agg_pattern, sql, re.IGNORECASE):
        return "aggregation"

    return "curated"


def _extract_kpi_aliases(sql: str) -> list[str]:
    """Extract aliases from aggregate function expressions.

    Looks for patterns like:
        SUM(amount) AS total_amount
        COUNT(*) AS order_count
        AVG(price) AS avg_price

    These aliases represent business KPI columns that the pipeline produces.

    Args:
        sql: Comment-stripped SQL text.

    Returns:
        Deduplicated list of KPI alias names.
    """
    kpis: list[str] = []

    # Match: AGG_FUNC(...) AS alias_name
    # The aggregate function body (between parens) can contain anything
    # except a closing paren that is not nested.
    kpi_pattern = r"\b(?:SUM|COUNT|AVG|MAX|MIN)\s*\([^)]*\)\s+AS\s+(\w+)"
    for match in re.finditer(kpi_pattern, sql, re.IGNORECASE):
        kpis.append(match.group(1))

    return list(dict.fromkeys(kpis))


def parse_sql(sql_text: str) -> ParsedSQL:
    """Parse a SQL string and extract lineage metadata.

    This is the single public entry point for the SQL parser module.
    It orchestrates all internal helpers in the correct order:

    1. Strip comments so they don't interfere with regex matching.
    2. Detect CTE names so they can be excluded from source tables.
    3. Extract source tables (FROM/JOIN), filtering out CTEs and keywords.
    4. Extract target tables (INSERT INTO/CREATE TABLE AS/MERGE INTO).
    5. Extract key columns from JOIN ON and WHERE conditions.
    6. Detect job type (aggregation vs curated).
    7. Extract KPI metric aliases from aggregate expressions.
    8. Store the original SQL as transformation logic.

    Args:
        sql_text: Raw SQL string, potentially with comments and whitespace.

    Returns:
        A ParsedSQL TypedDict with all extracted lineage metadata.
    """
    # Step 1: Remove comments first (they can interfere with all other parsing)
    cleaned = _strip_comments(sql_text)

    # Step 2: Identify CTE aliases so we can exclude them from source tables
    cte_names = _extract_cte_names(cleaned)

    # Step 3-7: Extract all metadata from the cleaned SQL
    return ParsedSQL(
        source_tables=_extract_source_tables(cleaned, cte_names),
        target_tables=_extract_target_tables(cleaned),
        key_columns=_extract_key_columns(cleaned),
        kpi_metrics=_extract_kpi_aliases(cleaned),
        job_type=_detect_job_type(cleaned),
        transformation_logic=sql_text.strip(),
    )
