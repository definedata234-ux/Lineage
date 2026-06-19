"""Column-level SQL parser for lineage extraction.

Extends sql_parser.py to extract individual column-level mappings:
    source_table.source_column  →  target_table.target_column  [operation]

Per-column granularity is required so lineage consumers can answer:
  "Which source column feeds into this target column?"

Parsing strategy:
  - SELECT clause:  extract each selected expression with its alias
  - FROM/JOIN:      link selected columns back to their source table
  - WHERE/HAVING:   mark referenced columns as FILTER
  - GROUP BY / agg: mark aggregate expressions as AGGREGATE
  - Window funcs:   mark OVER-clause columns as WINDOW
  - JOIN ON:        mark join-key columns as JOIN

Limitations (accepted for regex-based approach):
  - Wildcard SELECT * produces one row with source_column="*"
  - Subquery-sourced columns cannot be traced deeper without a full parser
  - Column-to-table resolution uses best-effort alias matching

All public API returns list[ColumnMapping] — a TypedDict carrying
the raw extraction result before it's turned into ColumnLineageRecord.
"""

import re
from typing import TypedDict


class ColumnMapping(TypedDict):
    """One raw column-level mapping extracted from a SQL statement.

    Fields:
        source_table:    Qualified or bare table name read from (may be "").
        source_database: DB/schema prefix of source table (may be "").
        source_column:   Column name in the source (bare name or expression).
        target_table:    Qualified or bare table name written to (may be "").
        target_database: DB/schema prefix of target table (may be "").
        target_column:   Column alias in the target (or same as source).
        sql_operation:   SELECT | AGGREGATE | JOIN | FILTER | WINDOW | ALIAS | UNKNOWN
    """
    source_table: str
    source_database: str
    source_column: str
    target_table: str
    target_database: str
    target_column: str
    sql_operation: str


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_SQL_KEYWORDS: set[str] = {
    "SELECT", "FROM", "WHERE", "JOIN", "ON", "AND", "OR", "NOT",
    "IN", "IS", "NULL", "AS", "GROUP", "BY", "ORDER", "HAVING",
    "LIMIT", "UNION", "ALL", "EXISTS", "CASE", "WHEN", "THEN",
    "ELSE", "END", "SET", "INTO", "VALUES", "INSERT", "UPDATE",
    "DELETE", "CREATE", "DROP", "ALTER", "TABLE", "INDEX", "VIEW",
    "DUAL", "TRUE", "FALSE", "DISTINCT", "WITH", "OVER", "PARTITION",
    "ROWS", "RANGE", "BETWEEN", "UNBOUNDED", "PRECEDING", "FOLLOWING",
    "CURRENT", "ROW", "INNER", "LEFT", "RIGHT", "OUTER", "CROSS", "FULL",
}

def _is_junk_column(name: str) -> bool:
    """Return True if the column name is clearly not a real column.

    Filters out:
      - Pure numeric literals: 0, 1, 42, 3.14
      - Single characters that are not column names: '0', 'N'
      - Empty or whitespace-only strings
      - SQL boolean literals already in _SQL_KEYWORDS
    """
    stripped = name.strip()
    if not stripped:
        return True
    # Pure number (integer or float)
    try:
        float(stripped)
        return True
    except ValueError:
        pass
    # Single uppercase letter that is not a real column (e.g. 'N', 'X')
    # Allow single-letter table aliases like 'e', 't', 'c' — they're valid
    # but if they appear without a dot-prefix they're likely not column names
    # Only filter pure digit strings — already caught above
    return False


_AGG_FUNCS = re.compile(
    r"\b(SUM|COUNT|AVG|MAX|MIN|STDDEV|VARIANCE|COLLECT_LIST|COLLECT_SET|"
    r"APPROX_COUNT_DISTINCT|ANY_VALUE)\s*\(",
    re.IGNORECASE,
)

_WINDOW_FUNCS = re.compile(
    r"\b(ROW_NUMBER|RANK|DENSE_RANK|NTILE|PERCENT_RANK|LAG|LEAD|"
    r"FIRST_VALUE|LAST_VALUE|NTH_VALUE)\s*\(",
    re.IGNORECASE,
)


def _strip_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    return sql


def _split_qualified(name: str) -> tuple[str, str]:
    """Split 'db.table' → ('db', 'table'),  'table' → ('', 'table')."""
    name = name.strip().strip("`").strip('"')
    if "." in name:
        parts = name.rsplit(".", maxsplit=1)
        return parts[0].strip(), parts[1].strip()
    return "", name


def _extract_cte_names(sql: str) -> set[str]:
    names: set[str] = set()
    for m in re.finditer(r"\bWITH\s+(\w+)\s+AS\s*\(", sql, re.IGNORECASE):
        names.add(m.group(1).lower())
    for m in re.finditer(r"\)\s*,\s*(\w+)\s+AS\s*\(", sql, re.IGNORECASE):
        names.add(m.group(1).lower())
    return names


def _extract_target_info(sql: str) -> tuple[str, str]:
    """Return (target_db, target_table) from INSERT INTO / CREATE TABLE AS / MERGE INTO."""
    # Match either backtick-quoted names (`project.dataset.table`) or bare names
    _BT  = r"`[^`]+`"        # backtick-quoted
    _BARE = r"[a-zA-Z_][\w.]*"  # unquoted
    _TBL = rf"(?:{_BT}|{_BARE})"

    for pat in [
        # INSERT INTO must be complete — prevents "INTO" being captured as table
        rf"\bINSERT\s+INTO\s+({_TBL})",
        # CREATE [OR REPLACE] TABLE name AS SELECT
        rf"\bCREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?({_TBL})\s+(?:USING\s+\w+\s+)?AS\b",
        rf"\bCREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?({_TBL})\s*\n?\s*USING\b",
        rf"\bMERGE\s+INTO\s+({_TBL})",
    ]:
        m = re.search(pat, sql, re.IGNORECASE)
        if m:
            return _split_qualified(m.group(1).strip("`"))
    return "", ""


def _extract_from_tables(sql: str, cte_names: set[str]) -> list[tuple[str, str, str]]:
    """Return list of (alias_or_name, db, bare_table) for all FROM/JOIN tables."""
    results: list[tuple[str, str, str]] = []
    # Match: FROM/JOIN qualified_name [AS] alias
    # Accept both bare names and backtick-quoted BigQuery-style names
    pat = re.compile(
        r"\b(?:FROM|JOIN)\s+(`[^`]+`|[a-zA-Z_][\w.]*)\s*(?:AS\s+)?(\w+)?",
        re.IGNORECASE,
    )
    for m in pat.finditer(sql):
        qualified = m.group(1).strip("`")
        alias = m.group(2) or ""
        if qualified.upper() in _SQL_KEYWORDS:
            continue
        if qualified.lower() in cte_names:
            continue
        db, tbl = _split_qualified(qualified)
        # alias is what the query uses to refer to the table; fall back to bare name
        ref_name = alias if alias and alias.upper() not in _SQL_KEYWORDS else tbl
        results.append((ref_name.lower(), db, tbl))
    return results



def _bare_col(ref: str) -> str:
    """Strip table alias prefix and trailing SQL artifacts.

    'o.amount'                    → 'amount'
    'COUNT(t.col)'  (via rsplit)  → 'col'      (trailing ) stripped)
    'transaction_date DESC)'      → 'transaction_date'  (DESC + ) stripped)
    """
    col = ref.rsplit(".", 1)[-1].strip() if "." in ref else ref.strip()
    # Strip trailing non-identifier chars (parens, spaces, commas)
    col = re.sub(r"[)\s,]+$", "", col).strip()
    # Strip trailing SQL sort/window modifier keywords (DESC, ASC, NULLS FIRST, etc.)
    col = re.sub(
        r"\s+(?:DESC|ASC|NULLS\s+FIRST|NULLS\s+LAST)\s*$", "",
        col, flags=re.IGNORECASE
    ).strip()
    # Strip any remaining trailing non-word characters
    col = re.sub(r"\W+$", "", col).strip()
    return col



# ---------------------------------------------------------------------------
# CTE-aware helpers
# ---------------------------------------------------------------------------

def _extract_cte_bodies(sql: str) -> dict[str, str]:
    """Extract {cte_name_lower: body_sql} for every CTE in the statement.

    Uses paren-depth counting to correctly handle nested parentheses inside
    CTE bodies (e.g. CASE WHEN (...) THEN ... END, sub-SELECTs).
    """
    bodies: dict[str, str] = {}
    for m in re.finditer(r"\b(\w+)\s+AS\s*\(", sql, re.IGNORECASE):
        cte_name = m.group(1).lower()
        start = m.end() - 1          # position of the opening '('
        depth = 0
        for i in range(start, len(sql)):
            if sql[i] == "(":
                depth += 1
            elif sql[i] == ")":
                depth -= 1
                if depth == 0:
                    bodies[cte_name] = sql[start + 1 : i]
                    break
    return bodies


def _strip_cte_bodies(sql: str) -> str:
    """Return only the outer DML by removing all CTE body content.

    WITH cte1 AS (body1), cte2 AS (body2)
    INSERT INTO t SELECT a, b FROM cte1 JOIN cte2 ON ...

    Returns:  INSERT INTO t SELECT a, b FROM cte1 JOIN cte2 ON ...

    This lets _extract_select_columns find the OUTER SELECT (the one that
    actually writes to the target table) instead of the first CTE's SELECT.
    """
    with_m = re.search(r"\bWITH\b", sql, re.IGNORECASE)
    if not with_m:
        return sql

    in_str  = False
    str_ch  = None
    depth   = 0
    entered = False
    i       = with_m.start()

    while i < len(sql):
        c = sql[i]
        if not in_str and c in ("'", '"'):
            in_str, str_ch = True, c
        elif in_str and c == str_ch:
            in_str = False
        if not in_str:
            if c == "(":
                depth += 1
                entered = True
            elif c == ")":
                depth -= 1
                if depth == 0 and entered:
                    rest = sql[i + 1:].lstrip()
                    if not rest.startswith(","):
                        # No more CTEs — outer DML follows
                        return rest
        i += 1
    return sql


def _resolve_column_table(
    col_ref: str,
    from_tables: list[tuple[str, str, str]],
    cte_alias_map: "dict[str, tuple[str, str]] | None" = None,
) -> tuple[str, str]:
    """Resolve 'alias.col' or bare 'col' to (source_db, source_table).

    Resolution order:
      1. Dot-prefix matched against from_tables (real tables in scope)
      2. Dot-prefix matched against cte_alias_map (CTE aliases → real tables)
      3. Single from_tables entry (unambiguous bare column)
    """
    if "." in col_ref:
        prefix = col_ref.rsplit(".", 1)[0].lower().strip()
        prefix = re.sub(r"[^\w].*$", "", prefix).strip()
        for ref_name, db, tbl in from_tables:
            if ref_name == prefix or tbl.lower() == prefix:
                return db, tbl
        if cte_alias_map and prefix in cte_alias_map:
            return cte_alias_map[prefix]
        return "", ""
    if len(from_tables) == 1:
        return from_tables[0][1], from_tables[0][2]
    return "", ""


def _build_cte_alias_map(
    sql: str,
    cte_names: set[str],
) -> dict[str, tuple[str, str]]:
    """Map every CTE alias used in FROM/JOIN → (db, primary_real_table).

    For a chain like:
        daily_transactions  ← FROM frontier_bronze.transactions t
        account_summary     ← FROM daily_transactions dt JOIN frontier_bronze.accounts a
        customer_risk_score ← FROM account_summary asumm JOIN frontier_bronze.customers c
        (outer)             ← FROM customer_risk_score crs

    Returns:
        dt   → (frontier_bronze, transactions)
        a    → (frontier_bronze, accounts)
        asumm→ (frontier_bronze, accounts)     ← primary real table of account_summary
        c    → (frontier_bronze, customers)
        crs  → (frontier_bronze, customers)    ← primary real table of customer_risk_score
        ...
    """
    cte_bodies  = _extract_cte_bodies(sql)
    cte_lower   = {c.lower() for c in cte_names}

    # Step 1: find each CTE's direct real (non-CTE) source tables
    cte_direct: dict[str, list[tuple[str, str]]] = {}
    for cte_name, body in cte_bodies.items():
        real_tables = _extract_from_tables(body, cte_lower)
        cte_direct[cte_name] = [(db, tbl) for _, db, tbl in real_tables]

    # Step 2: resolve CTE→CTE chains to find the deepest real table
    cte_resolved: dict[str, tuple[str, str]] = {}
    for cte_name, direct in cte_direct.items():
        if direct:
            cte_resolved[cte_name] = direct[0]          # primary direct real table
        else:
            body = cte_bodies.get(cte_name, "")
            for used_cte in cte_lower:
                if re.search(r"\b" + re.escape(used_cte) + r"\b", body, re.IGNORECASE):
                    if used_cte in cte_resolved:
                        cte_resolved[cte_name] = cte_resolved[used_cte]
                        break
                    elif used_cte in cte_direct and cte_direct[used_cte]:
                        cte_resolved[cte_name] = cte_direct[used_cte][0]
                        break

    # Step 3: scan ALL FROM/JOIN for alias usage and build the final map
    alias_map: dict[str, tuple[str, str]] = {}
    for cte_name, src in cte_resolved.items():
        alias_map[cte_name] = src

    pat = re.compile(r"\b(?:FROM|JOIN)\s+(\w+)\s*(?:AS\s+)?(\w+)?", re.IGNORECASE)
    for m in pat.finditer(sql):
        table_ref = m.group(1).lower()
        alias     = (m.group(2) or "").lower()
        if table_ref in cte_lower:
            src = cte_resolved.get(table_ref)
            if src:
                if alias and alias.upper() not in _SQL_KEYWORDS:
                    alias_map[alias] = src
                alias_map[table_ref] = src

    return alias_map


def _build_cte_column_maps(
    sql: str,
    cte_names: set[str],
) -> dict[str, dict[str, tuple[str, str, str, str]]]:
    """Build column-level lineage maps for each CTE.

    Returns {cte_name: {output_col: (src_db, src_tbl, src_col, operation)}}.
    Processes CTEs in definition order so downstream CTEs can reference upstream.

    Example: customer_risk_score.total_amount traces back to
             account_summary.total_amount → SUM(transactions.amount)
             → (frontier_bronze, transactions, amount, AGGREGATE)
    """
    cte_bodies = _extract_cte_bodies(sql)
    cte_lower  = {c.lower() for c in cte_names}
    col_maps: dict[str, dict[str, tuple[str, str, str, str]]] = {}

    for cte_name, body in cte_bodies.items():
        real_tbls = _extract_from_tables(body, cte_lower)

        # alias→cte_name within this CTE body
        a2c: dict[str, str] = {}
        for m in re.finditer(r"\b(?:FROM|JOIN)\s+(\w+)\s*(?:AS\s+)?(\w+)?", body, re.IGNORECASE):
            ref = m.group(1).lower()
            al  = (m.group(2) or "").lower()
            if ref in cte_lower:
                a2c[ref] = ref
                if al and al.upper() not in _SQL_KEYWORDS:
                    a2c[al] = ref

        cmap: dict[str, tuple[str, str, str, str]] = {}

        for expr, alias, op in _extract_select_columns(body):
            if not alias or alias == "*":
                continue

            if op == "WINDOW":
                pb = re.search(r"\bPARTITION\s+BY\s+([\w.]+)", expr, re.IGNORECASE)
                ob = re.search(r"\bORDER\s+BY\s+([\w.]+)",    expr, re.IGNORECASE)
                ref_m = pb or ob
                if ref_m:
                    ref_s = ref_m.group(1)
                    db, tbl = _resolve_column_table(ref_s, real_tbls)
                    bare = _bare_col(ref_s)
                    if not db and "." in ref_s:
                        pre = ref_s.rsplit(".", 1)[0].lower()
                        cn  = _bare_col(ref_s)
                        if pre in a2c:
                            up = col_maps.get(a2c[pre], {}).get(cn)
                            if up:
                                db, tbl, bare = up[0], up[1], up[2]
                    cmap[alias] = (db, tbl, bare, "WINDOW")
                else:
                    cmap[alias] = ("", "", alias, "WINDOW")

            elif op == "AGGREGATE":
                m2 = re.match(r"\w+\s*\(\s*(?:DISTINCT\s+)?(.*?)\s*\)\s*$",
                               expr.strip(), re.IGNORECASE | re.DOTALL)
                cr = m2.group(1).strip() if m2 else expr
                if cr == "*":
                    cmap[alias] = ("", "", "*", "AGGREGATE")
                    continue
                bare = _bare_col(cr)
                db, tbl = _resolve_column_table(cr, real_tbls)
                if not db and "." in cr:
                    pre = cr.rsplit(".", 1)[0].lower()
                    cn  = _bare_col(cr)
                    if pre in a2c:
                        up = col_maps.get(a2c[pre], {}).get(cn)
                        if up:
                            db, tbl, bare = up[0], up[1], up[2]
                cmap[alias] = (db, tbl, bare, "AGGREGATE")

            else:  # SELECT or ALIAS
                bare = _bare_col(expr)
                db, tbl = _resolve_column_table(expr, real_tbls)
                src_op = op

                if not db and "." in expr:
                    pre = expr.rsplit(".", 1)[0].lower()
                    cn  = _bare_col(expr)
                    if pre in a2c:
                        up = col_maps.get(a2c[pre], {}).get(cn)
                        if up:
                            db, tbl, bare, src_op = up

                # CASE expression: trace the WHEN condition operand
                if re.match(r"\s*CASE\b", expr, re.IGNORECASE):
                    wm = re.search(r"\bWHEN\s+([\w.]+)\s*[><=!]", expr, re.IGNORECASE)
                    if wm:
                        cr2   = wm.group(1)
                        c_db, c_tbl = _resolve_column_table(cr2, real_tbls)
                        c_bare = _bare_col(cr2)
                        if not c_db and "." in cr2:
                            c_pre = cr2.rsplit(".", 1)[0].lower()
                            c_cn  = _bare_col(cr2)
                            if c_pre in a2c:
                                up = col_maps.get(a2c[c_pre], {}).get(c_cn)
                                if up:
                                    c_db, c_tbl, c_bare = up[0], up[1], up[2]
                        if c_db or c_tbl:
                            db, tbl, bare, src_op = c_db, c_tbl, c_bare, "ALIAS"

                cmap[alias] = (db, tbl, bare, src_op)

        col_maps[cte_name] = cmap

    return col_maps


# NOTE: _resolve_column_table is defined above (replaces the earlier version).
# The function below is the ORIGINAL signature kept for backward-compat callers.
# All internal callers now pass cte_alias_map.

def _extract_select_columns(sql: str) -> list[tuple[str, str, str]]:
    """Extract (raw_expression, alias, operation) tuples from the SELECT clause.

    Returns list of:
      (raw_expr, alias, operation)
    where operation is SELECT | AGGREGATE | WINDOW | ALIAS
    """
    # Find the SELECT clause body (between SELECT and FROM)
    m = re.search(r"\bSELECT\s+(.*?)\s+FROM\b", sql, re.IGNORECASE | re.DOTALL)
    if not m:
        # No FROM → maybe it's SELECT without FROM (rare but handle)
        m = re.search(r"\bSELECT\s+(.*?)$", sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return []

    select_body = m.group(1).strip()

    # Remove DISTINCT
    select_body = re.sub(r"^\s*DISTINCT\s+", "", select_body, flags=re.IGNORECASE)

    # Tokenise at commas that are not inside parentheses
    items: list[str] = []
    depth = 0
    current: list[str] = []
    for ch in select_body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            items.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    if current:
        items.append("".join(current).strip())

    results: list[tuple[str, str, str]] = []
    for item in items:
        item = item.strip()
        if not item:
            continue

        # Detect alias: expr AS alias  or  expr alias (bare word at end)
        alias_match = re.search(r"\bAS\s+(\w+)\s*$", item, re.IGNORECASE)
        if alias_match:
            alias = alias_match.group(1)
            expr = item[: alias_match.start()].strip()
        else:
            # Try bare alias: last token if it's a plain identifier
            tokens = item.split()
            if (
                len(tokens) > 1
                and re.match(r"^\w+$", tokens[-1])
                and tokens[-1].upper() not in _SQL_KEYWORDS
                and not tokens[-1].endswith(")")
            ):
                alias = tokens[-1]
                expr = " ".join(tokens[:-1]).strip()
            else:
                alias = ""
                expr = item

        # Detect operation type
        if expr.strip() == "*":
            operation = "SELECT"
            alias = alias or "*"
        elif _WINDOW_FUNCS.search(expr):
            operation = "WINDOW"
            alias = alias or _bare_col(expr)
        elif _AGG_FUNCS.search(expr):
            operation = "AGGREGATE"
            alias = alias or _bare_col(expr)
        elif alias and alias != _bare_col(expr):
            operation = "ALIAS"
        else:
            operation = "SELECT"
            alias = alias or _bare_col(expr)

        results.append((expr, alias, operation))

    return results


def _extract_filter_columns(sql: str) -> list[str]:
    """Extract bare column references from WHERE and HAVING clauses."""
    cols: list[str] = []
    for clause_kw in (r"\bWHERE\b", r"\bHAVING\b"):
        m = re.search(
            clause_kw
            + r"\s+(.*?)(?:\bGROUP\s+BY\b|\bORDER\s+BY\b|\bHAVING\b|\bLIMIT\b|$)",
            sql,
            re.IGNORECASE | re.DOTALL,
        )
        if not m:
            continue
        body = m.group(1)
        # Capture aliased columns (tbl.col) before comparison operators
        for cm in re.finditer(r"([\w]+\.[\w]+)\s*(?:[=<>!]=?|<>|\bIN\b|\bLIKE\b)", body, re.IGNORECASE):
            cols.append(cm.group(1))
        for cm in re.finditer(r"(?<![.\w])([\w]+)\s*(?:[=<>!]=?|<>)", body):
            c = cm.group(1)
            if c.upper() not in _SQL_KEYWORDS and not _is_junk_column(c):
                cols.append(c)
    return list(dict.fromkeys(cols))


def _extract_join_columns(sql: str) -> list[str]:
    """Extract columns from JOIN ON conditions."""
    cols: list[str] = []
    for m in re.finditer(r"\bON\s+([\w.]+)\s*=\s*([\w.]+)", sql, re.IGNORECASE):
        for g in (m.group(1), m.group(2)):
            cols.append(g)
    return cols


# ---------------------------------------------------------------------------
# Dependency extraction (filter/join columns from CTE bodies)
# ---------------------------------------------------------------------------

def _extract_cte_dependencies(
    sql: str,
    cte_names: set[str],
    cte_bodies: dict[str, str],
    target_db: str,
    target_table: str,
    cte_alias_map: "dict[str, tuple[str, str]]",
    cte_col_maps: "dict[str, dict[str, tuple[str,str,str,str]]] | None" = None,
) -> list[ColumnMapping]:
    """Extract FILTER_DEPENDENCY and JOIN_KEY rows from all CTE bodies.

    These rows document HOW data was filtered and joined during the
    transformation, even though the columns don't appear as named output
    columns in the target table.  target_column is set to "\u2014" (em dash)
    to clearly signal "no direct output column mapping."

    FILTER_DEPENDENCY: column used in WHERE/HAVING inside a CTE body.
      e.g.  transactions.transaction_date  was filtered by WHERE >= 365 days.
      This controlled which rows entered the pipeline.

    JOIN_KEY: column used in JOIN ON inside a CTE body.
      e.g.  transactions.account_id = accounts.account_id.
      This connected two tables; neither side is a named output column.
    """
    deps: list[ColumnMapping] = []
    cte_lower = {c.lower() for c in cte_names}
    seen: set[tuple] = set()   # dedup (src_db, src_tbl, src_col, op)

    for cte_name, body in cte_bodies.items():
        real_tbls = _extract_from_tables(body, cte_lower)

        # ── FILTER_DEPENDENCY ─────────────────────────────────────────────
        # Build a local alias→cte_name map for this CTE body's FROM/JOIN clauses
        # e.g. in account_summary: "dt" → "daily_transactions"
        local_alias_to_cte: dict[str, str] = {}
        for _m in re.finditer(r"\b(?:FROM|JOIN)\s+(\w+)\s*(?:AS\s+)?(\w+)?", body, re.IGNORECASE):
            _ref = _m.group(1).lower()
            _al  = (_m.group(2) or "").lower()
            if _ref in cte_lower:
                local_alias_to_cte[_ref] = _ref
                if _al and _al.upper() not in _SQL_KEYWORDS:
                    local_alias_to_cte[_al] = _ref

        for col_ref in _extract_filter_columns(body):
            bare = _bare_col(col_ref)
            if _is_junk_column(bare):
                continue
            src_db, src_tbl = _resolve_column_table(col_ref, real_tbls, cte_alias_map)

            # If col_ref is a CTE-derived column (e.g. dt.rn where rn is computed),
            # look it up in cte_col_maps to get the real source table/column.
            if "." in col_ref and cte_col_maps:
                pre = col_ref.rsplit(".", 1)[0].lower()
                cn  = _bare_col(col_ref)
                if pre in local_alias_to_cte:
                    cte_nm  = local_alias_to_cte[pre]
                    cte_map = cte_col_maps.get(cte_nm, {})
                    if cn in cte_map:
                        up = cte_map[cn]
                        if up[1]:   # has a resolved real table
                            src_db, src_tbl, bare = up[0], up[1], up[2]

            if not src_tbl:
                continue   # can't resolve source — skip
            key = (src_db, src_tbl, bare, "FILTER_DEPENDENCY")
            if key in seen:
                continue
            seen.add(key)
            deps.append(ColumnMapping(
                source_database=src_db,
                source_table=src_tbl,
                source_column=bare,
                target_database=target_db,
                target_table=target_table,
                target_column="\u2014",
                sql_operation="FILTER_DEPENDENCY",
            ))

        # ── JOIN_KEY ──────────────────────────────────────────────────────
        for col_ref in _extract_join_columns(body):
            bare = _bare_col(col_ref)
            if _is_junk_column(bare):
                continue
            src_db, src_tbl = _resolve_column_table(col_ref, real_tbls, cte_alias_map)
            if not src_tbl:
                continue
            key = (src_db, src_tbl, bare, "JOIN_KEY")
            if key in seen:
                continue
            seen.add(key)
            deps.append(ColumnMapping(
                source_database=src_db,
                source_table=src_tbl,
                source_column=bare,
                target_database=target_db,
                target_table=target_table,
                target_column="\u2014",
                sql_operation="JOIN_KEY",
            ))

    return deps


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _split_statements(sql: str) -> list[str]:
    """Split a multi-statement SQL file into individual statements at semicolons.

    Ignores semicolons inside parentheses, strings, and comments.
    Returns a list of non-empty statement strings.
    """
    statements: list[str] = []
    depth = 0
    in_single = False
    in_double = False
    buf: list[str] = []
    i = 0
    while i < len(sql):
        ch = sql[i]
        # String tracking
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        # Paren depth (only outside strings)
        if not in_single and not in_double:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth = max(0, depth - 1)
            elif ch == ";" and depth == 0:
                stmt = "".join(buf).strip()
                if stmt:
                    statements.append(stmt)
                buf = []
                i += 1
                continue
        buf.append(ch)
        i += 1
    # Remaining content after last semicolon
    stmt = "".join(buf).strip()
    if stmt:
        statements.append(stmt)
    return statements


def extract_column_mappings(
    sql_text: str,
    include_dependencies: bool = False,
) -> list[ColumnMapping]:
    """Parse one or more SQL statements and return column-level lineage mappings.

    Handles:
      - Multi-statement files (splits on ';')
      - CREATE TABLE IF NOT EXISTS (target extraction)
      - Numeric/junk column filtering (e.g. '1' from WHERE 1=0)
      - CTE names excluded from source tables in all statements
      - MERGE INTO, INSERT INTO, CREATE TABLE AS as targets
      - All SELECT / AGGREGATE / WINDOW / JOIN / FILTER operations

    Args:
        sql_text:             One or more SQL statements, optionally separated by ';'.
        include_dependencies: When True, also emits FILTER_DEPENDENCY and JOIN_KEY rows
                              for CTE-internal filter and join columns.
                              These rows have target_column="\u2014" (no direct output mapping).

    Returns:
        Deduplicated list of ColumnMapping dicts.
    """
    cleaned = _strip_comments(sql_text)
    # Extract ALL CTE names across the entire file (shared across statements)
    cte_names = _extract_cte_names(cleaned)

    statements = _split_statements(cleaned)
    if not statements:
        statements = [cleaned]

    all_mappings: list[ColumnMapping] = []

    for stmt in statements:
        stmt = stmt.strip()
        if not stmt:
            continue

        # Skip pure DDL statements that have no data flow
        # (VACUUM, OPTIMIZE, RESTORE, CONVERT TO DELTA)
        _DDL_ONLY = re.compile(
            r"^\s*(VACUUM|OPTIMIZE|RESTORE\s+TABLE|CONVERT\s+TO\s+DELTA)\b",
            re.IGNORECASE
        )
        if _DDL_ONLY.match(stmt):
            continue

        stmt_cte_names = _extract_cte_names(stmt) | cte_names
        target_db, target_table = _extract_target_info(stmt)

        # ── CTE-aware helpers ─────────────────────────────────────────────
        # cte_alias_map : alias → (db, primary_real_table) for every CTE alias
        # cte_col_maps  : CTE-name → {output_col: (db, tbl, col, op)}
        # outer_stmt    : full statement with CTE bodies removed so that
        #                 _extract_select_columns finds the OUTER SELECT
        cte_alias_map = _build_cte_alias_map(stmt, stmt_cte_names) if stmt_cte_names else {}
        cte_col_maps  = _build_cte_column_maps(stmt, stmt_cte_names) if stmt_cte_names else {}
        outer_stmt    = _strip_cte_bodies(stmt)       if stmt_cte_names else stmt

        # from_tables: real (non-CTE) tables in scope of the full statement
        from_tables  = _extract_from_tables(stmt, stmt_cte_names)

        # outer_a2c: outer SELECT alias → CTE name (e.g. "crs"→"customer_risk_score")
        outer_a2c: dict[str, str] = {}
        for _m in re.finditer(r"\b(?:FROM|JOIN)\s+(\w+)\s*(?:AS\s+)?(\w+)?", outer_stmt, re.IGNORECASE):
            _ref = _m.group(1).lower()
            _al  = (_m.group(2) or "").lower()
            if _ref in {c.lower() for c in stmt_cte_names}:
                outer_a2c[_ref] = _ref
                if _al and _al.upper() not in _SQL_KEYWORDS:
                    outer_a2c[_al] = _ref

        # SELECT/JOIN/FILTER all from the OUTER statement only.
        # CTE-internal joins and filters (e.g. ON dt.account_id = a.account_id,
        # WHERE dt.rn <= 100) are intermediate transformations — they don't
        # directly produce output columns in the target table.
        # Only outer-query conditions (WHERE crs.risk_rank <= 10000) are relevant.
        select_cols = _extract_select_columns(outer_stmt)
        filter_cols = _extract_filter_columns(outer_stmt)
        join_cols   = _extract_join_columns(outer_stmt)

        # Build set of output column aliases from the outer SELECT so we can
        # filter JOIN/FILTER refs to only those that appear in the actual output.
        output_cols: set[str] = {alias.lower() for _, alias, _ in select_cols if alias and alias != "*"}

        mappings: list[ColumnMapping] = []

        # ── SELECT / AGGREGATE / WINDOW / ALIAS ──────────────────────────
        for expr, alias, operation in select_cols:
            # Skip pure zero-arg system functions: CURRENT_TIMESTAMP(), NOW(), etc.
            if re.match(r"^[A-Za-z_][A-Za-z0-9_]*\s*\(\s*\)\s*$", expr.strip()):
                continue

            src_op = operation

            if operation == "WINDOW":
                # Extract source col from PARTITION BY or ORDER BY
                pb = re.search(r"\bPARTITION\s+BY\s+([\w.]+)", expr, re.IGNORECASE)
                ob = re.search(r"\bORDER\s+BY\s+([\w.]+)",    expr, re.IGNORECASE)
                ref_m = pb or ob
                if ref_m:
                    ref_s = ref_m.group(1)
                    bare  = _bare_col(ref_s)
                    src_db, src_tbl = _resolve_column_table(ref_s, from_tables, cte_alias_map)
                    if not src_db and "." in ref_s:
                        pre = ref_s.rsplit(".", 1)[0].lower()
                        cn  = _bare_col(ref_s)
                        if pre in outer_a2c:
                            up = cte_col_maps.get(outer_a2c[pre], {}).get(cn)
                            if up:
                                src_db, src_tbl, bare = up[0], up[1], up[2]
                else:
                    bare = alias
                    src_db, src_tbl = "", ""

            elif operation == "AGGREGATE":
                inner_m = re.match(
                    r"\w+\s*\(\s*(?:DISTINCT\s+)?(.*?)\s*\)\s*$",
                    expr.strip(), re.IGNORECASE | re.DOTALL
                )
                col_ref = inner_m.group(1).strip() if inner_m else expr
                bare    = _bare_col(col_ref) if col_ref != "*" else "*"
                src_db, src_tbl = _resolve_column_table(col_ref, from_tables, cte_alias_map)
                if not src_db and "." in col_ref:
                    pre = col_ref.rsplit(".", 1)[0].lower()
                    cn  = _bare_col(col_ref)
                    if pre in outer_a2c:
                        up = cte_col_maps.get(outer_a2c[pre], {}).get(cn)
                        if up:
                            src_db, src_tbl, bare = up[0], up[1], up[2]

            else:  # SELECT or ALIAS
                if re.search(r"[-+*/]", expr) and expr.count(".") > 1:
                    bare = "expr"
                    src_db, src_tbl = "", ""
                else:
                    bare = _bare_col(expr)
                    src_db, src_tbl = _resolve_column_table(expr, from_tables, cte_alias_map)

                # For outer-SELECT columns that reference a CTE alias (e.g. crs.total_amount),
                # look them up in the CTE column map for precise source attribution.
                # This traces crs.total_amount → account_summary.total_amount → transactions.amount
                if "." in expr:
                    pre = expr.rsplit(".", 1)[0].lower()
                    cn  = _bare_col(expr)
                    if pre in outer_a2c:
                        up = cte_col_maps.get(outer_a2c[pre], {}).get(cn)
                        if up:
                            src_db, src_tbl, bare, src_op = up

                # CASE expression: trace the WHEN condition operand through CTE maps
                if re.match(r"\s*CASE\b", expr, re.IGNORECASE):
                    wm = re.search(r"\bWHEN\s+([\w.]+)\s*[><=!]", expr, re.IGNORECASE)
                    if wm:
                        cr2 = wm.group(1)
                        if "." in cr2:
                            c_pre = cr2.rsplit(".", 1)[0].lower()
                            c_cn  = _bare_col(cr2)
                            if c_pre in outer_a2c:
                                up = cte_col_maps.get(outer_a2c[c_pre], {}).get(c_cn)
                                if up:
                                    src_db, src_tbl, bare, src_op = up

            # Skip junk column names
            if _is_junk_column(bare) or _is_junk_column(alias):
                continue
            mappings.append(ColumnMapping(
                source_database=src_db,
                source_table=src_tbl,
                source_column=bare if bare != "*" else "*",
                target_database=target_db,
                target_table=target_table,
                target_column=alias,
                sql_operation=src_op,
            ))

        # ── JOIN key columns ──────────────────────────────────────────────
        # Only emit JOIN rows for columns that actually appear in the output SELECT.
        for col_ref in join_cols:
            bare = _bare_col(col_ref)
            if _is_junk_column(bare):
                continue
            # Skip if this column is not in the final output
            if output_cols and bare.lower() not in output_cols:
                continue
            src_db, src_tbl = _resolve_column_table(col_ref, from_tables, cte_alias_map)
            # Trace through CTE maps to get real source
            if not src_db and "." in col_ref:
                pre = col_ref.rsplit(".", 1)[0].lower()
                cn  = _bare_col(col_ref)
                if pre in outer_a2c:
                    up = cte_col_maps.get(outer_a2c[pre], {}).get(cn)
                    if up:
                        src_db, src_tbl, bare = up[0], up[1], up[2]
            mappings.append(ColumnMapping(
                source_database=src_db,
                source_table=src_tbl,
                source_column=bare,
                target_database=target_db,
                target_table=target_table,
                target_column=bare,
                sql_operation="JOIN",
            ))

        # ── FILTER columns ────────────────────────────────────────────────
        # Only emit FILTER rows for columns that actually appear in the output SELECT.
        for col_ref in filter_cols:
            bare = _bare_col(col_ref)
            if _is_junk_column(bare):
                continue
            # Skip if this column is not in the final output
            if output_cols and bare.lower() not in output_cols:
                continue
            src_db, src_tbl = "", ""
            # target_col is the OUTPUT alias (e.g. "risk_rank") — fixed before CTE trace.
            # After CTE trace bare may become the true source col ("amount").
            target_col_filter = bare  # preserve output alias
            # For filter refs like crs.risk_rank, first check the CTE column map
            # (gives the true source column e.g. transactions.amount) before
            # falling back to the alias map (which only gives the primary table).
            if "." in col_ref:
                pre = col_ref.rsplit(".", 1)[0].lower()
                cn  = _bare_col(col_ref)
                if pre in outer_a2c:
                    up = cte_col_maps.get(outer_a2c[pre], {}).get(cn)
                    if up:
                        src_db, src_tbl, bare = up[0], up[1], up[2]
                if not src_db:
                    src_db, src_tbl = _resolve_column_table(col_ref, from_tables, cte_alias_map)
            else:
                src_db, src_tbl = _resolve_column_table(col_ref, from_tables, cte_alias_map)
            mappings.append(ColumnMapping(
                source_database=src_db,
                source_table=src_tbl,
                source_column=bare,
                target_database=target_db,
                target_table=target_table,
                target_column=target_col_filter,   # output alias, not traced source col
                sql_operation="FILTER",
            ))

        # ── Dependency rows (optional) ────────────────────────────────────
        if include_dependencies and stmt_cte_names:
            cte_bodies_dep = _extract_cte_bodies(stmt)
            deps = _extract_cte_dependencies(
                stmt, stmt_cte_names, cte_bodies_dep,
                target_db, target_table, cte_alias_map,
                cte_col_maps=cte_col_maps,
            )
            all_mappings.extend(deps)

        all_mappings.extend(mappings)

    from collections import defaultdict, OrderedDict

    # ── Pass 1: prefer rows with a real source_table over blank ones ──────────
    # Groups by (src_col, tgt_tbl, tgt_col, op); within each group keeps only
    # rows with a non-blank source_table (drops CTE-alias phantoms).
    phase1: dict[tuple, list] = defaultdict(list)
    for m in all_mappings:
        gkey = (
            m["source_column"],
            m["target_table"],
            m["target_column"],
            m["sql_operation"],
        )
        phase1[gkey].append(m)

    resolved: list[ColumnMapping] = []
    seen_p1: set[tuple] = set()
    for gkey, grp in phase1.items():
        with_src = [m for m in grp if m["source_table"]]
        candidates = with_src if with_src else grp
        for m in candidates:
            k = (m["source_database"], m["source_table"],
                 m["source_column"], m["target_database"],
                 m["target_table"], m["target_column"],
                 m["sql_operation"])
            if k not in seen_p1:
                seen_p1.add(k)
                resolved.append(m)

    # ── Pass 2: merge operations for same source→target column pair ───────────
    # Leadership feedback: "if 2+ operations on same column, comma-separate them
    # and don't repeat the row."
    # Group by (src_db, src_tbl, src_col, tgt_db, tgt_tbl, tgt_col)
    # Collect all operations per group; emit ONE row with "OP1, OP2" etc.
    # Operation priority order for display
    OP_ORDER = ["SELECT", "AGGREGATE", "WINDOW", "ALIAS", "JOIN", "FILTER", "UNKNOWN"]

    merge_groups: OrderedDict = OrderedDict()
    for m in resolved:
        mkey = (
            m["source_database"], m["source_table"], m["source_column"],
            m["target_database"], m["target_table"], m["target_column"],
        )
        if mkey not in merge_groups:
            merge_groups[mkey] = {"mapping": m, "ops": []}
        op = m["sql_operation"]
        if op not in merge_groups[mkey]["ops"]:
            merge_groups[mkey]["ops"].append(op)

    final: list[ColumnMapping] = []
    for mkey, entry in merge_groups.items():
        m = entry["mapping"]
        ops = entry["ops"]
        # Sort by priority order for consistent display
        ops_sorted = sorted(ops, key=lambda o: OP_ORDER.index(o) if o in OP_ORDER else 99)
        merged_op = ", ".join(ops_sorted) if len(ops_sorted) > 1 else ops_sorted[0]
        final.append(ColumnMapping(
            source_database=m["source_database"],
            source_table=m["source_table"],
            source_column=m["source_column"],
            target_database=m["target_database"],
            target_table=m["target_table"],
            target_column=m["target_column"],
            sql_operation=merged_op,
        ))

    return final
