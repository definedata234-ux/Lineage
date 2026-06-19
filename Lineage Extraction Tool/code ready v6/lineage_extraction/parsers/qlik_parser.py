"""Qlik Sense script (.qvs) parser for data lineage extraction.

This module extracts lineage metadata from Qlik Sense script files (.qvs).
It parses:
  - Named LOAD statements (target table name before the LOAD keyword)
  - FROM clauses referencing QVD files or table names (source tables)
  - RESIDENT clauses (in-memory table references as sources)
  - Embedded SQL SELECT blocks (dispatched to parse_sql())
  - STORE ... INTO statements (write targets)
  - Aggregation functions (SUM, COUNT, AVG, MIN, MAX) for KPI detection
  - JOIN ON fields for key column extraction
  - GROUP BY columns for key column candidates

Design decisions:
  - QVS files are plain text — no binary unpacking needed.
  - Each named LOAD block produces one target table entry.
  - STORE destinations are also added to target_tables (final writes).
  - FROM [lib://...] paths are normalised to the leaf filename without
    extension (e.g. customer_master.qvd → "customer_master").
  - SQL blocks after the SQL keyword are extracted and handed to parse_sql().
  - job_type is "aggregation" when any SUM/COUNT/AVG/MIN/MAX appears;
    otherwise "curated".
"""

import re
from pathlib import Path
from typing import TypedDict

try:
    from sql_parser import parse_sql
except ImportError:
    try:
        from column_sql_parser import extract_column_mappings as _extract_col_mappings

        def parse_sql(sql: str) -> dict:
            mappings = _extract_col_mappings(sql)
            tables = list(dict.fromkeys(m["source_table"] for m in mappings if m.get("source_table")))
            return {"source_tables": tables, "target_tables": [], "key_columns": [], "kpi_metrics": []}
    except ImportError:
        def parse_sql(sql: str) -> dict:
            return {"source_tables": [], "target_tables": [], "key_columns": [], "kpi_metrics": []}


class ParsedQlik(TypedDict):
    """Structured result returned by parse_qlik().

    Fields:
        source_tables: QVD file names and SQL source tables read by this script.
        target_tables: Named LOAD targets and STORE destinations.
        key_columns: Columns used in ON conditions and GROUP BY.
        kpi_metrics: Aggregated column aliases (e.g. "total_revenue (SUM)").
        job_type: "aggregation" if aggregations exist, else "curated".
        transformation_logic: Human-readable summary of the script structure.
    """

    source_tables: list[str]
    target_tables: list[str]
    key_columns: list[str]
    kpi_metrics: list[str]
    job_type: str
    transformation_logic: str


# QVS reserved words that should never be treated as table names
_QVS_KEYWORDS: set[str] = {
    "LOAD", "FROM", "WHERE", "GROUP", "BY", "HAVING", "ORDER",
    "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "ON", "AND", "OR",
    "NOT", "AS", "RESIDENT", "STORE", "INTO", "DROP", "TABLE",
    "SQL", "SELECT", "DISTINCT", "SET", "LET", "IF", "THEN", "ELSE",
    "END", "CALL", "SUBROUTINE", "NEXT", "FOR", "EACH", "IN",
    "WHILE", "DO", "EXIT", "WHEN", "SWITCH", "CASE", "DEFAULT",
    "CONCATENATE", "NOCONCATENATE", "MAPPING", "APPLY", "MAP",
    "USING", "PEEK", "FIRST", "LAST", "ONLY", "NULLASVALUE",
    "NULLASNULL", "QUALIFY", "UNQUALIFY", "ALIAS", "RENAME",
    "FIELDS", "TAG", "COMMENT", "SECTION", "ACCESS", "APPLICATION",
    "QVD", "QVW", "TXT", "CSV", "XLSX", "XLS",
    "UTF8", "ANSI", "OEM", "UNICODE",
    "EMBEDDED", "LABELS", "DELIMITER", "EXPLICIT", "KEEP", "ALL",
}


def _read_qvs_file(file_path: Path) -> str:
    """Read a QVS file and return its content.

    Args:
        file_path: Path to the .qvs file.

    Returns:
        File content as a string.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"QVS file not found: {file_path}")
    return file_path.read_text(encoding="utf-8")


def _strip_comments(content: str) -> str:
    """Remove // line comments from QVS content.

    Only strips // that are NOT inside bracket-delimited strings like
    [lib://path/file.qvd]. Inline // after code is stripped, but //
    within [...] (e.g. lib:// paths) is preserved.

    Block comments (/* ... */) are not common in QVS but are left as-is
    since they do not affect regex matching for our patterns.

    Args:
        content: Raw QVS script content.

    Returns:
        Content with // line comments removed.
    """
    lines: list[str] = []
    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("//"):
            # Full-line comment: replace with blank line to preserve line numbers
            lines.append("")
        else:
            # Only strip // that appear outside bracket-delimited strings.
            # We scan from left to right tracking bracket depth.
            result = _remove_comment_outside_brackets(line)
            lines.append(result)
    return "\n".join(lines)


def _remove_comment_outside_brackets(line: str) -> str:
    """Remove trailing // comment from a line, preserving // inside [...].

    Scans the line character by character. When bracket depth is 0,
    a // sequence starts a comment and the rest of the line is stripped.

    Args:
        line: A single line from a QVS file.

    Returns:
        The line with any out-of-bracket // comment removed.
    """
    depth = 0
    i = 0
    while i < len(line):
        ch = line[i]
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth = max(0, depth - 1)
        elif ch == "/" and i + 1 < len(line) and line[i + 1] == "/" and depth == 0:
            # Comment starts here — strip the rest
            return line[:i].rstrip()
        i += 1
    return line


def _extract_load_targets(content: str) -> list[str]:
    """Extract named LOAD targets from a QVS script.

    QVS pattern: a word followed by a colon (with optional surrounding
    whitespace), then LOAD on either the same line or the next non-empty
    line.  Both inline and multi-line forms are supported:

    Multi-line form:
        CustomerBase:
        LOAD ...

    Inline form (same line):
        CustomerBase: LOAD ...

    SQL-labeled blocks (e.g. ``OrdersFact:\\nSQL SELECT ...``) are NOT
    included — only ``LOAD`` introduces a target here.

    Args:
        content: QVS script content (may include comments).

    Returns:
        Deduplicated list of table names defined as LOAD targets.
    """
    clean = _strip_comments(content)
    # Match: word + colon, then any mix of spaces/tabs/newlines (including
    # zero whitespace for the inline form), then the LOAD keyword.
    # SQL labels are intentionally excluded — only LOAD is matched.
    pattern = re.compile(
        r"^[ \t]*([A-Za-z_]\w*)[ \t]*:[ \t]*(?:\r?\n[ \t]*)*LOAD\b",
        re.MULTILINE | re.IGNORECASE,
    )
    targets: list[str] = []
    for match in pattern.finditer(clean):
        name = match.group(1).strip()
        if name.upper() not in _QVS_KEYWORDS:
            targets.append(name)
    return list(dict.fromkeys(targets))


def _extract_from_sources(content: str) -> list[str]:
    """Extract source table names from FROM and RESIDENT clauses.

    Handles:
      - FROM [lib://path/filename.qvd] → leaf filename without extension
      - FROM schema.table → leaf table name
      - RESIDENT TableName → in-memory table name

    Args:
        content: QVS script content.

    Returns:
        Deduplicated list of source names.
    """
    clean = _strip_comments(content)
    sources: list[str] = []

    # Pattern 1: FROM [lib://any/path/filename.ext] — extract leaf filename
    lib_pattern = re.compile(
        r"\bFROM\s+\[lib://[^\]]+/([^\]/]+?)(?:\.\w+)?\s*\]",
        re.IGNORECASE,
    )
    for match in lib_pattern.finditer(clean):
        sources.append(match.group(1).strip())

    # Pattern 2: FROM plain name or schema.table (not bracket form)
    # Negative lookahead to avoid matching [lib://...] paths
    plain_pattern = re.compile(
        r"\bFROM\s+(?!\[)([A-Za-z_][\w.]*)",
        re.IGNORECASE,
    )
    for match in plain_pattern.finditer(clean):
        raw = match.group(1).rstrip(".")
        leaf = raw.split(".")[-1]
        if leaf.upper() not in _QVS_KEYWORDS:
            sources.append(leaf)

    # Pattern 3: RESIDENT <TableName>
    resident_pattern = re.compile(r"\bRESIDENT\s+([A-Za-z_]\w*)", re.IGNORECASE)
    for match in resident_pattern.finditer(clean):
        name = match.group(1)
        if name.upper() not in _QVS_KEYWORDS:
            sources.append(name)

    return list(dict.fromkeys(sources))


def _extract_store_targets(content: str) -> list[str]:
    """Extract STORE ... INTO destination names as write targets.

    Handles:
      - STORE TableName INTO [lib://path/filename.qvd] → "filename"
      - STORE TableName INTO plain_path → stem of path

    Args:
        content: QVS script content.

    Returns:
        Deduplicated list of output destination names.
    """
    clean = _strip_comments(content)
    targets: list[str] = []

    # STORE X INTO [lib://path/filename.ext]
    lib_store = re.compile(
        r"\bSTORE\s+\S+\s+INTO\s+\[lib://[^\]]+/([^\]/]+?)(?:\.\w+)?\s*\]",
        re.IGNORECASE,
    )
    for match in lib_store.finditer(clean):
        targets.append(match.group(1).strip())

    # STORE X INTO plain_path (no brackets)
    plain_store = re.compile(
        r"\bSTORE\s+\S+\s+INTO\s+([A-Za-z_][\w/\\.-]+)",
        re.IGNORECASE,
    )
    for match in plain_store.finditer(clean):
        path_str = match.group(1)
        leaf = Path(path_str).stem
        if leaf.upper() not in _QVS_KEYWORDS:
            targets.append(leaf)

    return list(dict.fromkeys(targets))


def _extract_sql_blocks(content: str) -> list[str]:
    """Extract embedded SQL SELECT statements from a QVS script.

    QVS allows embedding SQL via: TableName:\nSQL SELECT ...;

    Known limitation: the pattern terminates on the first bare semicolon, so
    SQL field expressions containing semicolons inside string literals or
    function arguments (e.g. Date#(f, 'YYYY;MM;DD')) will produce truncated
    blocks. This is acceptable for the Verizon-Frontier pipeline corpus where
    this pattern does not appear.

    Args:
        content: QVS script content.

    Returns:
        List of SQL statement strings (without trailing semicolons).
        Returns an empty list if no SQL blocks are found.
    """
    clean = _strip_comments(content)
    sql_blocks: list[str] = []

    # Match SQL keyword followed by SELECT ... up to the next semicolon
    sql_pattern = re.compile(
        r"\bSQL\s+(SELECT\b.+?);",
        re.IGNORECASE | re.DOTALL,
    )
    for match in sql_pattern.finditer(clean):
        sql_blocks.append(match.group(1).strip())

    return sql_blocks


def _extract_join_key_columns(content: str) -> list[str]:
    """Extract column names used in JOIN ON conditions.

    Args:
        content: QVS script content.

    Returns:
        Deduplicated list of column names from ON clauses.
    """
    clean = _strip_comments(content)
    columns: list[str] = []

    # ON alias.column = alias.column
    on_pattern = re.compile(
        r"\bON\s+(?:\w+\.)?(\w+)\s*=\s*(?:\w+\.)?(\w+)",
        re.IGNORECASE,
    )
    for match in on_pattern.finditer(clean):
        for col in (match.group(1), match.group(2)):
            if col.upper() not in _QVS_KEYWORDS:
                columns.append(col)

    return list(dict.fromkeys(columns))


def _extract_group_by_columns(content: str) -> list[str]:
    """Extract columns listed in GROUP BY clauses.

    Args:
        content: QVS script content.

    Returns:
        Deduplicated list of GROUP BY column names.
    """
    clean = _strip_comments(content)
    columns: list[str] = []

    group_by_pattern = re.compile(
        r"\bGROUP\s+BY\s+([\w,\s]+?)(?:;|\bHAVING\b|\bORDER\b|\bLIMIT\b|$)",
        re.IGNORECASE | re.DOTALL,
    )
    for match in group_by_pattern.finditer(clean):
        col_list = match.group(1)
        for col in re.split(r"[,\s]+", col_list):
            col = col.strip()
            if col and col.upper() not in _QVS_KEYWORDS:
                columns.append(col)

    return list(dict.fromkeys(columns))


def _extract_aggregation_columns(content: str) -> list[str]:
    """Extract aliased aggregation expressions as KPI metric names.

    Looks for: SUM(field) AS alias → "alias (SUM)"

    Args:
        content: QVS script content.

    Returns:
        Deduplicated list of KPI alias names with their aggregation function.
    """
    clean = _strip_comments(content)
    kpis: list[str] = []

    agg_pattern = re.compile(
        r"\b(SUM|COUNT|AVG|MIN|MAX|MEDIAN|STDEV)\s*\([^)]+\)\s+AS\s+(\w+)",
        re.IGNORECASE,
    )
    for match in agg_pattern.finditer(clean):
        func = match.group(1).upper()
        alias = match.group(2)
        kpis.append(f"{alias} ({func})")

    return list(dict.fromkeys(kpis))


def _detect_job_type_qlik(content: str) -> str:
    """Detect whether this QVS script is aggregation or curated.

    Returns "aggregation" if any SUM/COUNT/AVG/MIN/MAX call exists in
    non-commented code.  Comments are stripped first so that a commented-out
    aggregation call does not trigger a false "aggregation" classification.

    Args:
        content: QVS script content.

    Returns:
        "aggregation" or "curated".
    """
    clean = _strip_comments(content)
    agg_pattern = re.compile(
        r"\b(SUM|COUNT|AVG|MIN|MAX|MEDIAN|STDEV)\s*\(",
        re.IGNORECASE,
    )
    return "aggregation" if agg_pattern.search(clean) else "curated"


def _build_transformation_logic(
    load_targets: list[str],
    store_targets: list[str],
    sql_block_count: int,
    kpi_metrics: list[str],
) -> str:
    """Build a human-readable transformation logic summary for the CSV.

    Args:
        load_targets: Named LOAD target table names.
        store_targets: STORE destination names.
        sql_block_count: Number of embedded SQL blocks found.
        kpi_metrics: Aggregated KPI column names.

    Returns:
        Single-line transformation logic string.
    """
    parts: list[str] = []

    if load_targets:
        display = ", ".join(load_targets[:5])
        parts.append(f"LOAD targets: {display}")
        if len(load_targets) > 5:
            parts.append(f"... and {len(load_targets) - 5} more")

    if sql_block_count:
        parts.append(f"Embedded SQL blocks: {sql_block_count}")

    if store_targets:
        display = ", ".join(store_targets[:3])
        parts.append(f"STORE into: {display}")

    if kpi_metrics:
        kpi_names = [k.split(" (")[0] for k in kpi_metrics[:5]]
        parts.append(f"KPIs: {', '.join(kpi_names)}")

    return " | ".join(parts) if parts else "Qlik script with standard LOAD operations"


def parse_qlik(file_path: "str | Path") -> ParsedQlik:
    """Parse a Qlik Sense script file (.qvs) and extract lineage metadata.

    Extracts:
      1. Named LOAD target tables
      2. FROM / RESIDENT source tables
      3. STORE INTO write targets
      4. Embedded SQL blocks (delegated to parse_sql())
      5. JOIN ON key columns
      6. GROUP BY columns
      7. Aggregation KPI aliases
      8. Job type detection

    Args:
        file_path: Path to the .qvs file.

    Returns:
        A ParsedQlik TypedDict with all extracted lineage metadata.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    file_path = Path(file_path)
    raw_content = _read_qvs_file(file_path)

    load_targets = _extract_load_targets(raw_content)
    from_sources = _extract_from_sources(raw_content)
    store_targets = _extract_store_targets(raw_content)
    sql_blocks = _extract_sql_blocks(raw_content)
    join_keys = _extract_join_key_columns(raw_content)
    group_by_cols = _extract_group_by_columns(raw_content)
    kpi_metrics = _extract_aggregation_columns(raw_content)
    job_type = _detect_job_type_qlik(raw_content)

    # Parse embedded SQL blocks for additional source/target tables
    sql_source_tables: list[str] = []
    sql_target_tables: list[str] = []
    for sql in sql_blocks:
        try:
            parsed = parse_sql(sql)
            sql_source_tables.extend(parsed["source_tables"])
            sql_target_tables.extend(parsed["target_tables"])
        except Exception:
            pass

    source_tables = list(dict.fromkeys(from_sources + sql_source_tables))
    target_tables = list(dict.fromkeys(load_targets + store_targets + sql_target_tables))
    key_columns = list(dict.fromkeys(join_keys + group_by_cols))

    transformation_logic = _build_transformation_logic(
        load_targets, store_targets, len(sql_blocks), kpi_metrics
    )

    return ParsedQlik(
        source_tables=source_tables,
        target_tables=target_tables,
        key_columns=key_columns,
        kpi_metrics=kpi_metrics,
        job_type=job_type,
        transformation_logic=transformation_logic,
    )


# ---------------------------------------------------------------------------
# Reporting lineage extraction (separate from ETL lineage above)
# ---------------------------------------------------------------------------


def _extract_load_columns(content: str) -> list[str]:
    """Extract field names from the field list of each LOAD block.

    Handles:
      - Bare fields:     customer_id, customer_name, region
      - Aliased fields:  SUM(revenue) AS total_revenue -> "total_revenue"
      - Wildcard *:      skipped (not meaningful as column names)

    Stops consuming at FROM, RESIDENT, or semicolon so it does not
    accidentally pick up table names or other keywords.

    Args:
        content: QVS script content.

    Returns:
        Deduplicated list of column names.
    """
    clean = _strip_comments(content)
    columns: list[str] = []

    # Match: LOAD <optional qualifiers> <field-list> <terminator>
    # Terminators: FROM, RESIDENT, ; (any of these ends the field list)
    load_pattern = re.compile(
        r"\bLOAD\s+(?:DISTINCT\s+|ALL\s+)?(.*?)(?=\bFROM\b|\bRESIDENT\b|;)",
        re.IGNORECASE | re.DOTALL,
    )

    for match in load_pattern.finditer(clean):
        field_list = match.group(1).strip()
        if not field_list or field_list.strip() == "*":
            continue

        for item in field_list.split(","):
            item = item.strip()
            if not item:
                continue
            # Prefer AS alias (captures aliased aggregations like SUM(...) AS x)
            alias_match = re.search(r"\bAS\s+(\w+)\s*$", item, re.IGNORECASE)
            if alias_match:
                name = alias_match.group(1)
            else:
                # Skip unaliased function expressions — cannot reliably determine column name
                if "(" in item:
                    continue
                # Fall back to the last bare word (handles plain field names)
                bare = re.search(r"(\w+)\s*$", item)
                if not bare:
                    continue
                name = bare.group(1)

            if name.upper() not in _QVS_KEYWORDS and name:
                columns.append(name)

    return list(dict.fromkeys(columns))


def extract_reporting_lineage(file_path: "str | Path") -> dict:
    """Extract reporting lineage from a Qlik Sense .qvs script.

    Reuses the existing private helpers already in this module for extracting
    LOAD targets (sql_name), FROM/RESIDENT sources (tables), and aggregation
    detection (operation). Adds a new _extract_load_columns() helper for the
    LOAD field list (columns).

    Args:
        file_path: Path to the .qvs file.

    Returns:
        Dict with keys: sql_name, tables, columns, operation.
        Values are lists of strings (may be empty).
    """
    file_path = Path(file_path)
    content = _read_qvs_file(file_path)

    # sql_name: named LOAD targets (reuse existing helper)
    sql_names = _extract_load_targets(content)

    # tables: FROM sources + RESIDENT in-memory tables (reuse existing helpers)
    tables = _extract_from_sources(content)

    # columns: field names from LOAD field lists (new helper)
    columns = _extract_load_columns(content)

    # operation: SELECT always (LOAD is the Qlik read op);
    # JOIN if JOIN LOAD pattern found;
    # AGGREGATE if aggregation functions present.
    ops: set[str] = {"SELECT"}
    clean = _strip_comments(content)
    if re.search(r"\bJOIN\s+LOAD\b", clean, re.IGNORECASE):
        ops.add("JOIN")
    if re.search(r"\b(?:SUM|COUNT|AVG|MIN|MAX|MEDIAN|STDEV)\s*\(", clean, re.IGNORECASE):
        ops.add("AGGREGATE")

    return {
        "sql_name": list(dict.fromkeys(sql_names)),
        "tables": list(dict.fromkeys(tables)),
        "columns": list(dict.fromkeys(columns)),
        "operation": sorted(ops),
    }
