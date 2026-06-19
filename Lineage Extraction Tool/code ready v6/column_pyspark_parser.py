"""Column-level PySpark parser for lineage extraction.

Uses Python's ast module to extract column-level lineage from PySpark
DataFrame API code. Each detected column-to-column flow becomes one
ColumnMapping row.

Detection patterns:
  - .select("col", F.col("col"), F.col("src").alias("tgt"))
    → SELECT and ALIAS mappings
  - .withColumn("new_col", expr)
    → ALIAS mapping (source_col = expression, target_col = new_col)
  - .join(other, df1.col == df2.col) or .join(other, ["col"])
    → JOIN mappings
  - .filter(df.col ...) or .where(...)
    → FILTER mappings
  - .groupBy("col").agg(F.sum("amount").alias("total"))
    → AGGREGATE mappings
  - spark.sql("...") embedded SQL → delegated to column_sql_parser
  - spark.table("db.table") / spark.read.table("db.table")
    → source table detection for context resolution
  - .saveAsTable("db.table") / .insertInto("db.table")
    → target table for all mappings accumulated so far

The parser preserves qualified names (db.table) and splits them into
(source_database, source_table) so the output schema matches SQL output.
"""

import ast
import re
from typing import Optional

from column_sql_parser import ColumnMapping, extract_column_mappings


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _split_qualified(name: str) -> tuple[str, str]:
    """Split 'db.table' → ('db', 'table'), 'table' → ('', 'table')."""
    if "." in name:
        parts = name.rsplit(".", maxsplit=1)
        return parts[0].strip(), parts[1].strip()
    return "", name.strip()


def _get_string_const(node: ast.expr) -> Optional[str]:
    """Return the string value if node is a string constant, else None."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _is_spark_table_call(node: ast.Call) -> bool:
    f = node.func
    return (
        isinstance(f, ast.Attribute)
        and f.attr == "table"
        and isinstance(f.value, ast.Name)
        and f.value.id == "spark"
    )


def _is_spark_read_table_call(node: ast.Call) -> bool:
    f = node.func
    return (
        isinstance(f, ast.Attribute)
        and f.attr == "table"
        and isinstance(f.value, ast.Attribute)
        and f.value.attr == "read"
        and isinstance(f.value.value, ast.Name)
        and f.value.value.id == "spark"
    )


def _is_spark_sql_call(node: ast.Call) -> bool:
    f = node.func
    return (
        isinstance(f, ast.Attribute)
        and f.attr == "sql"
        and isinstance(f.value, ast.Name)
        and f.value.id == "spark"
    )


def _method_name(node: ast.Call) -> Optional[str]:
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _extract_col_name(node: ast.expr) -> Optional[str]:
    """Extract a column name from F.col("name"), col("name"), or "name" string."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Call):
        # F.col("name") or col("name")
        if (
            isinstance(node.func, ast.Attribute) and node.func.attr == "col"
            or isinstance(node.func, ast.Name) and node.func.id == "col"
        ):
            if node.args and isinstance(node.args[0], ast.Constant):
                return node.args[0].value
    return None


def _extract_alias(node: ast.expr) -> tuple[Optional[str], Optional[str]]:
    """If node is .alias("name"), return (inner_expr_str, alias), else (None, None)."""
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "alias"
        and node.args
    ):
        alias = _get_string_const(node.args[0])
        return "expr", alias
    return None, None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_pyspark_column_mappings(
    code: str,
    file_path: str = "",
    include_dependencies: bool = False,
) -> list[ColumnMapping]:
    """Parse PySpark source code and extract column-level lineage.

    Returns a flat list of ColumnMapping dicts — same schema as
    extract_column_mappings() in column_sql_parser.py so both can be
    processed by the same downstream writer.

    Args:
        code:      Python source code string.
        file_path: Used only for error messages.

    Returns:
        List of ColumnMapping dicts.
    """
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return []

    mappings: list[ColumnMapping] = []

    # Track the most recently seen source and target tables as context
    # for column resolution within a statement block.
    current_source_db: str = ""
    current_source_table: str = ""
    current_target_db: str = ""
    current_target_table: str = ""

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        # ── Source tables ─────────────────────────────────────────────────
        if _is_spark_table_call(node) or _is_spark_read_table_call(node):
            name = _get_string_const(node.args[0]) if node.args else None
            if name:
                db, tbl = _split_qualified(name)
                current_source_db = db
                current_source_table = tbl
            continue

        # ── Embedded spark.sql() → delegate to SQL column parser ──────────
        if _is_spark_sql_call(node):
            sql_text = _get_string_const(node.args[0]) if node.args else None
            if sql_text:
                for stmt in re.split(r";+", sql_text):
                    stmt = stmt.strip()
                    if stmt:
                        mappings.extend(extract_column_mappings(stmt))
            continue

        meth = _method_name(node)
        if meth is None:
            continue

        # ── Target tables ─────────────────────────────────────────────────
        if meth in ("saveAsTable", "insertInto", "writeTo"):
            name = _get_string_const(node.args[0]) if node.args else None
            if name:
                db, tbl = _split_qualified(name)
                current_target_db = db
                current_target_table = tbl
            continue

        # ── .select(*cols) ────────────────────────────────────────────────
        if meth == "select":
            for arg in node.args:
                col_name = _extract_col_name(arg)
                if col_name:
                    _, alias = _extract_alias(arg)
                    mappings.append(ColumnMapping(
                        source_database=current_source_db,
                        source_table=current_source_table,
                        source_column=col_name,
                        target_database=current_target_db,
                        target_table=current_target_table,
                        target_column=alias or col_name,
                        sql_operation="SELECT",
                    ))
                    continue
                # F.col("src").alias("tgt") pattern
                if (
                    isinstance(arg, ast.Call)
                    and isinstance(arg.func, ast.Attribute)
                    and arg.func.attr == "alias"
                    and arg.args
                ):
                    tgt_alias = _get_string_const(arg.args[0])
                    inner = arg.func.value
                    src_col = _extract_col_name(inner)
                    if tgt_alias and src_col:
                        mappings.append(ColumnMapping(
                            source_database=current_source_db,
                            source_table=current_source_table,
                            source_column=src_col,
                            target_database=current_target_db,
                            target_table=current_target_table,
                            target_column=tgt_alias,
                            sql_operation="ALIAS",
                        ))

        # ── .withColumn("new_col", expr) ──────────────────────────────────
        elif meth == "withColumn" and len(node.args) >= 2:
            new_col = _get_string_const(node.args[0])
            src_col = _extract_col_name(node.args[1]) or "expr"
            if new_col:
                mappings.append(ColumnMapping(
                    source_database=current_source_db,
                    source_table=current_source_table,
                    source_column=src_col,
                    target_database=current_target_db,
                    target_table=current_target_table,
                    target_column=new_col,
                    sql_operation="ALIAS" if src_col != "expr" else "UNKNOWN",
                ))

        # ── .agg(F.sum("col").alias("total"), ...) ────────────────────────
        elif meth == "agg":
            for arg in node.args:
                # F.sum("col").alias("alias") pattern
                if (
                    isinstance(arg, ast.Call)
                    and isinstance(arg.func, ast.Attribute)
                    and arg.func.attr == "alias"
                    and arg.args
                ):
                    tgt = _get_string_const(arg.args[0])
                    inner = arg.func.value
                    if isinstance(inner, ast.Call):
                        src = _extract_col_name(inner.args[0]) if inner.args else "expr"
                    else:
                        src = "expr"
                    if tgt:
                        mappings.append(ColumnMapping(
                            source_database=current_source_db,
                            source_table=current_source_table,
                            source_column=src or "expr",
                            target_database=current_target_db,
                            target_table=current_target_table,
                            target_column=tgt,
                            sql_operation="AGGREGATE",
                        ))

        # ── .join(other, condition | ["col"] | "col") ─────────────────────
        elif meth == "join" and len(node.args) >= 2 and include_dependencies:
            cond = node.args[1]
            # String column name: .join(other, "col")
            col = _get_string_const(cond)
            if col:
                mappings.append(ColumnMapping(
                    source_database=current_source_db,
                    source_table=current_source_table,
                    source_column=col,
                    target_database=current_target_db,
                    target_table=current_target_table,
                    target_column=col,
                    sql_operation="JOIN",
                ))
            # List of cols: .join(other, ["a", "b"])
            elif isinstance(cond, ast.List):
                for elt in cond.elts:
                    c = _get_string_const(elt)
                    if c:
                        mappings.append(ColumnMapping(
                            source_database=current_source_db,
                            source_table=current_source_table,
                            source_column=c,
                            target_database=current_target_db,
                            target_table=current_target_table,
                            target_column=c,
                            sql_operation="JOIN",
                        ))
            # df1.col == df2.col comparison
            elif isinstance(cond, ast.Compare):
                for side in [cond.left] + list(cond.comparators):
                    if isinstance(side, ast.Attribute):
                        mappings.append(ColumnMapping(
                            source_database=current_source_db,
                            source_table=current_source_table,
                            source_column=side.attr,
                            target_database=current_target_db,
                            target_table=current_target_table,
                            target_column=side.attr,
                            sql_operation="JOIN",
                        ))

        # ── .filter(.where) ───────────────────────────────────────────────
        elif meth in ("filter", "where") and node.args and include_dependencies:
            # Try to extract column from a string expression
            expr_str = _get_string_const(node.args[0])
            if expr_str:
                for m in re.finditer(r"\b(\w+)\s*(?:[=<>!]|IS|IN|LIKE)", expr_str, re.IGNORECASE):
                    col = m.group(1)
                    if col.upper() not in {"IS", "IN", "NOT", "NULL", "LIKE", "TRUE", "FALSE"}:
                        mappings.append(ColumnMapping(
                            source_database=current_source_db,
                            source_table=current_source_table,
                            source_column=col,
                            target_database=current_target_db,
                            target_table=current_target_table,
                            target_column=col,
                            sql_operation="FILTER",
                        ))

    # Deduplicate
    seen: set[tuple] = set()
    unique: list[ColumnMapping] = []
    for m in mappings:
        key = (
            m["source_table"], m["source_column"],
            m["target_table"], m["target_column"],
            m["sql_operation"],
        )
        if key not in seen:
            seen.add(key)
            unique.append(m)

    return unique
