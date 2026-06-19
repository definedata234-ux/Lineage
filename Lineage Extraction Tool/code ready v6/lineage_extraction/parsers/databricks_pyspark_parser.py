# scripts/lineage_extraction/python/databricks_pyspark_parser.py
"""AST-based parser for Databricks-specific PySpark patterns.

Detects PySpark patterns that need migration attention when moving
from Databricks (AWS) to BigQuery (GCP). Uses Python's ast module to walk
the source code and identify:
  - UDF patterns (pandas_udf, udf, udf.register)
  - Window operations (Window.partitionBy, .over)
  - Delta Lake operations (format("delta"), DeltaTable, merge)
  - Format-specific I/O (parquet, csv, json, orc)
  - Partitioning (repartition, coalesce, partitionBy, bucketBy, sortBy)
  - Broadcast joins

Source/target extraction preserves the database prefix (e.g.
"frontier_bronze.orders" -> database="frontier_bronze", table="orders").

Design decisions:
  - ast.walk() for flat iteration (same approach as pyspark_parser.py).
  - Source/target extraction reuses the same spark.table / saveAsTable /
    insertInto detection patterns, but preserves the database prefix instead
    of stripping it.
  - Complexity is determined by the highest-severity construct found:
    direct < rewrite < manual.
"""

import ast
from typing import TypedDict


class DatabricksPySparkResult(TypedDict):
    """Structured result from parse_databricks_pyspark().

    Fields:
        source_db: Database/schema names from source tables.
        source_tables: Bare table names from spark.table / spark.read.table.
        target_db: Database/schema names from target tables.
        target_tables: Bare table names from saveAsTable / insertInto.
        constructs_found: All Databricks-specific patterns detected.
        has_udf: True if any UDF patterns detected.
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
# Complexity mapping for PySpark constructs
# ---------------------------------------------------------------------------
# "direct" = direct BigQuery equivalent exists
# "rewrite" = needs structural rewrite
# "manual" = no clean BigQuery equivalent
_COMPLEXITY_MAP: dict[str, str] = {
    # direct
    "format(parquet)": "direct",
    "broadcast": "direct",
    "coalesce": "direct",
    "repartition": "direct",
    # rewrite
    "Window": "rewrite",
    "over": "rewrite",
    "partitionBy": "rewrite",
    "format(csv)": "rewrite",
    "format(json)": "rewrite",
    "format(orc)": "rewrite",
    "sortBy": "rewrite",
    # manual
    "pandas_udf": "manual",
    "udf": "manual",
    "udf.register": "manual",
    "format(delta)": "manual",
    "DeltaTable": "manual",
    "merge": "manual",
    "bucketBy": "manual",
}

_UDF_CONSTRUCTS: set[str] = {"pandas_udf", "udf", "udf.register"}
_DELTA_CONSTRUCTS: set[str] = {"format(delta)", "DeltaTable", "merge"}

# Method names that write to a target table (same set as pyspark_parser.py)
_WRITE_METHODS = frozenset({"saveAsTable", "insertInto"})


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _empty_result() -> DatabricksPySparkResult:
    """Return an empty result with all defaults."""
    return DatabricksPySparkResult(
        source_db=[],
        source_tables=[],
        target_db=[],
        target_tables=[],
        constructs_found=[],
        has_udf=False,
        has_delta_ops=False,
        complexity="direct",
    )


def _get_func_name(node: ast.Call) -> str | None:
    """Extract the method name from an ast.Call's func attribute."""
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _get_first_string_arg(node: ast.Call) -> str | None:
    """Extract the first positional argument if it's a string literal."""
    if node.args and isinstance(node.args[0], ast.Constant):
        if isinstance(node.args[0].value, str):
            return node.args[0].value
    return None


def _split_table_name(qualified: str) -> tuple[str, str]:
    """Split 'db.table' -> ('db', 'table'), 'table' -> ('', 'table')."""
    if "." in qualified:
        parts = qualified.rsplit(".", maxsplit=1)
        return (parts[0], parts[1])
    return ("", qualified)


def _is_spark_table_call(node: ast.Call) -> bool:
    """True if node is spark.table("...")."""
    func = node.func
    return (
        isinstance(func, ast.Attribute)
        and func.attr == "table"
        and isinstance(func.value, ast.Name)
        and func.value.id == "spark"
    )


def _is_spark_read_table_call(node: ast.Call) -> bool:
    """True if node is spark.read.table("...")."""
    func = node.func
    return (
        isinstance(func, ast.Attribute)
        and func.attr == "table"
        and isinstance(func.value, ast.Attribute)
        and func.value.attr == "read"
        and isinstance(func.value.value, ast.Name)
        and func.value.value.id == "spark"
    )


def _determine_complexity(constructs_found: list[str]) -> str:
    """Determine migration complexity -- highest severity wins."""
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

def parse_databricks_pyspark(code: str) -> DatabricksPySparkResult:
    """Parse Python source and detect Databricks-specific PySpark patterns.

    Uses ast.walk() to iterate over every node. For each ast.Call and
    ast.FunctionDef, pattern-matching detects migration-relevant constructs.

    Source/target extraction preserves the database prefix.

    Args:
        code: Python source code as a string.

    Returns:
        A DatabricksPySparkResult TypedDict.
    """
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return _empty_result()

    constructs: list[str] = []
    source_db: list[str] = []
    source_tables: list[str] = []
    target_db: list[str] = []
    target_tables: list[str] = []

    for node in ast.walk(tree):
        # --- Check decorators for pandas_udf ---
        if isinstance(node, ast.FunctionDef):
            for decorator in node.decorator_list:
                if isinstance(decorator, ast.Name) and decorator.id == "pandas_udf":
                    constructs.append("pandas_udf")
                elif isinstance(decorator, ast.Call):
                    if isinstance(decorator.func, ast.Name) and decorator.func.id == "pandas_udf":
                        constructs.append("pandas_udf")

        # --- All other patterns are on ast.Call nodes ---
        if not isinstance(node, ast.Call):
            continue

        # --- pandas_udf as function call (not decorator) ---
        if isinstance(node.func, ast.Name) and node.func.id == "pandas_udf":
            constructs.append("pandas_udf")
            continue

        # --- udf() function call ---
        if isinstance(node.func, ast.Name) and node.func.id == "udf":
            constructs.append("udf")
            continue

        # --- broadcast() function call ---
        if isinstance(node.func, ast.Name) and node.func.id == "broadcast":
            constructs.append("broadcast")
            continue

        # --- All remaining patterns need an Attribute func ---
        method_name = _get_func_name(node)
        if method_name is None:
            continue

        # --- spark.udf.register() ---
        if method_name == "register" and isinstance(node.func, ast.Attribute):
            value = node.func.value
            if isinstance(value, ast.Attribute) and value.attr == "udf":
                constructs.append("udf.register")
                continue

        # --- Source: spark.table("db.table") ---
        if _is_spark_table_call(node):
            name = _get_first_string_arg(node)
            if name:
                db, tbl = _split_table_name(name)
                if db:
                    source_db.append(db)
                source_tables.append(tbl)
            continue

        # --- Source: spark.read.table("db.table") ---
        if _is_spark_read_table_call(node):
            name = _get_first_string_arg(node)
            if name:
                db, tbl = _split_table_name(name)
                if db:
                    source_db.append(db)
                source_tables.append(tbl)
            continue

        # --- Target: saveAsTable / insertInto ---
        if method_name in _WRITE_METHODS:
            name = _get_first_string_arg(node)
            if name:
                db, tbl = _split_table_name(name)
                if db:
                    target_db.append(db)
                target_tables.append(tbl)
            # Fall through -- saveAsTable/insertInto are targets but NOT constructs

        # --- .format("xxx") detection ---
        if method_name == "format":
            arg = _get_first_string_arg(node)
            if arg:
                constructs.append(f"format({arg})")
            continue

        # --- Window operations: Window.partitionBy / orderBy / rowsBetween / rangeBetween ---
        if (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "Window"
            and method_name in ("partitionBy", "orderBy", "rowsBetween", "rangeBetween")
        ):
            constructs.append("Window")
            continue

        # --- .over(Window) pattern ---
        if method_name == "over":
            constructs.append("over")
            continue

        # --- DeltaTable.xxx() (forPath, forName) ---
        if (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "DeltaTable"
        ):
            constructs.append("DeltaTable")
            continue

        # --- .merge() on DeltaTable ---
        if method_name == "merge":
            constructs.append("merge")
            continue

        # --- Partitioning constructs ---
        if method_name in ("repartition", "coalesce", "partitionBy", "bucketBy", "sortBy"):
            constructs.append(method_name)
            continue

    # Deduplicate all lists
    constructs = list(dict.fromkeys(constructs))
    source_db = list(dict.fromkeys(source_db))
    source_tables = list(dict.fromkeys(source_tables))
    target_db = list(dict.fromkeys(target_db))
    target_tables = list(dict.fromkeys(target_tables))

    return DatabricksPySparkResult(
        source_db=source_db,
        source_tables=source_tables,
        target_db=target_db,
        target_tables=target_tables,
        constructs_found=constructs,
        has_udf=any(c in _UDF_CONSTRUCTS for c in constructs),
        has_delta_ops=any(c in _DELTA_CONSTRUCTS for c in constructs),
        complexity=_determine_complexity(constructs),
    )
