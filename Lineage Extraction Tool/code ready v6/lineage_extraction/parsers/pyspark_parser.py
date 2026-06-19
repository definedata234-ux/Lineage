"""AST-based PySpark DataFrame API parser that extracts lineage metadata.

This module uses Python's built-in `ast` module to walk PySpark source code
and extract data lineage information: which tables are read from, which tables
are written to, what columns are used as join keys, and whether the pipeline
is an aggregation or a curated transformation.

Design decisions:
  - ast.walk() is used (NOT ast.NodeVisitor) per the spec -- it provides a
    simple flat iteration over every node in the tree, which is sufficient for
    our pattern-matching needs and easier to reason about.
  - spark.sql("SELECT ...") calls are delegated to the regex-based SQL parser
    (parse_sql from sql_parser.py) so we don't duplicate SQL parsing logic.
  - Transformation methods (join, filter, groupBy, agg, select, etc.) are
    tracked in order and joined with " -> " to produce a human-readable
    transformation logic string.
  - Deduplication uses list(dict.fromkeys(...)) which preserves insertion
    order while removing duplicates (Python 3.7+ dict ordering guarantee).
  - SyntaxError is handled gracefully -- invalid Python code returns an empty
    ParsedPySpark result rather than crashing the caller.
"""

import ast
import json
from typing import TypedDict

# Import the SQL parser so we can delegate spark.sql("...") calls to it.
# This avoids duplicating SQL parsing logic -- the regex-based parser already
# handles FROM, JOIN, INSERT, CTE filtering, etc.
from lineage_extraction.parsers.sql_parser import parse_sql


class ParsedPySpark(TypedDict):
    """Structured result returned by parse_pyspark() and parse_notebook().

    Fields:
        source_tables: Tables read from (spark.table, spark.read.table, spark.sql).
        target_tables: Tables written to (saveAsTable, insertInto, writeTo).
        key_columns: Column names used in join conditions (df.col == df.col).
        kpi_metrics: Aggregate aliases from spark.sql() calls (delegated to SQL parser).
        job_type: "aggregation" if groupBy/agg detected, else "curated".
        transformation_logic: Human-readable summary of transformation methods.
    """

    source_tables: list[str]
    target_tables: list[str]
    key_columns: list[str]
    kpi_metrics: list[str]
    job_type: str
    transformation_logic: str


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


def _empty_result() -> ParsedPySpark:
    """Return an empty ParsedPySpark with default values.

    Used when the input code cannot be parsed (SyntaxError) or when
    no lineage-relevant constructs are found.

    Returns:
        A ParsedPySpark with empty lists, job_type "curated", and empty logic.
    """
    return ParsedPySpark(
        source_tables=[],
        target_tables=[],
        key_columns=[],
        kpi_metrics=[],
        job_type="curated",
        transformation_logic="",
    )


def _get_func_name(node: ast.Call) -> str | None:
    """Extract the method name from an ast.Call node's func attribute.

    Handles two common patterns:
      1. Simple method call: func=Attribute(attr="table")
         -> returns "table"
      2. Chained method call: func=Attribute(value=Attribute(attr="read"), attr="table")
         -> returns "table" (we only care about the final method name)

    Returns None if the func is not an Attribute node (e.g., a bare function call
    like foo() with no object receiver).
    """
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _get_first_string_arg(node: ast.Call) -> str | None:
    """Extract the first positional argument if it's a string literal.

    PySpark DataFrame API calls like spark.table("name") or
    .write.saveAsTable("name") always pass the table name as the first
    positional argument. This helper extracts that string.

    Returns None if the first argument is not a string constant (e.g.,
    it could be a variable reference or an expression).
    """
    if node.args and isinstance(node.args[0], ast.Constant):
        # ast.Constant in Python 3.8+ replaces the old ast.Str
        if isinstance(node.args[0].value, str):
            return node.args[0].value
    return None


def _extract_join_keys_from_compare(node: ast.Compare) -> list[str]:
    """Extract column names from a join condition like df1.cust_id == df2.id.

    In PySpark, join conditions are expressed as Python comparisons:
        df1.join(df2, df1.cust_id == df2.id)

    The AST represents this as a Compare node where both sides are
    Attribute nodes (object.attribute). We extract the attribute name
    (the column name) from each side.

    Args:
        node: An ast.Compare node (e.g., from a join() call's second argument).

    Returns:
        A list of column names found in the comparison (typically 2 for ==).
    """
    keys: list[str] = []

    # The left operand of the comparison (e.g., df1.cust_id)
    if isinstance(node.left, ast.Attribute):
        keys.append(node.left.attr)

    # Each comparator on the right side (e.g., df2.id)
    # Usually there's exactly one for ==, but ast.Compare supports
    # multiple chained comparisons (a < b < c).
    for comparator in node.comparators:
        if isinstance(comparator, ast.Attribute):
            keys.append(comparator.attr)

    return keys


def _is_spark_table_call(node: ast.Call) -> bool:
    """Check if an ast.Call node represents spark.table("...").

    Pattern in AST: Call(func=Attribute(value=Name(id="spark"), attr="table"))

    This distinguishes spark.table() from other .table() calls
    (e.g., some_object.table()) by checking that the receiver is
    named "spark".
    """
    func = node.func
    if not isinstance(func, ast.Attribute):
        return False
    if func.attr != "table":
        return False
    # Check that the receiver (value) is the Name "spark"
    if isinstance(func.value, ast.Name) and func.value.id == "spark":
        return True
    return False


def _is_spark_read_table_call(node: ast.Call) -> bool:
    """Check if an ast.Call node represents spark.read.table("...").

    Pattern in AST:
        Call(func=Attribute(
            value=Attribute(value=Name(id="spark"), attr="read"),
            attr="table"
        ))

    We walk the attribute chain: the outer attr must be "table",
    and the inner chain must be spark.read.
    """
    func = node.func
    if not isinstance(func, ast.Attribute):
        return False
    if func.attr != "table":
        return False
    # Check that the receiver is spark.read
    if isinstance(func.value, ast.Attribute):
        inner = func.value
        if inner.attr == "read" and isinstance(inner.value, ast.Name):
            if inner.value.id == "spark":
                return True
    return False


def _is_spark_sql_call(node: ast.Call) -> bool:
    """Check if an ast.Call node represents spark.sql("...").

    Pattern in AST: Call(func=Attribute(value=Name(id="spark"), attr="sql"))

    When detected, we delegate the SQL string to parse_sql() for full
    SQL-based lineage extraction.
    """
    func = node.func
    if not isinstance(func, ast.Attribute):
        return False
    if func.attr != "sql":
        return False
    if isinstance(func.value, ast.Name) and func.value.id == "spark":
        return True
    return False


# Method names that represent transformation steps we want to track.
# These are the common PySpark DataFrame methods that transform data.
TRANSFORM_METHODS = frozenset({
    "join", "filter", "where", "groupBy", "agg",
    "select", "withColumn", "drop", "distinct",
    "orderBy", "sort", "limit", "union", "intersect",
})

# Method names that indicate a write operation (target table).
WRITE_METHODS = frozenset({
    "saveAsTable", "insertInto", "writeTo",
})


def parse_pyspark(code: str) -> ParsedPySpark:
    """Parse Python source code and extract PySpark lineage metadata.

    Uses ast.walk() to iterate over every node in the parsed AST tree.
    For each ast.Call node, we check what method is being called and
    extract the relevant lineage information:

      1. spark.table("name") / spark.read.table("name") -> source table
      2. spark.sql("SELECT ...") -> delegate to parse_sql for full extraction
      3. .write.saveAsTable("name") / .insertInto("name") / .writeTo("name") -> target
      4. .join(df2, condition) -> extract key columns from comparison
      5. .groupBy() / .agg() -> mark as aggregation
      6. Transformation method tracking (join, filter, groupBy, agg, select, etc.)

    Args:
        code: Python source code as a string. May be a single statement or
              a full script.

    Returns:
        A ParsedPySpark TypedDict with all extracted lineage metadata.
        Returns an empty result if the code has syntax errors.
    """
    # Try to parse the Python source code into an AST.
    # If it's invalid Python (syntax error), return empty gracefully.
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return _empty_result()

    # Accumulators for lineage metadata
    source_tables: list[str] = []
    target_tables: list[str] = []
    key_columns: list[str] = []
    kpi_metrics: list[str] = []
    transform_methods: list[str] = []
    has_aggregation = False

    # SQL strings found in spark.sql() calls -- these are appended to
    # the transformation logic separately from the method chain.
    sql_strings: list[str] = []

    # Iterate over every node in the AST using ast.walk().
    # This gives us a flat view of all nodes regardless of nesting depth,
    # which is simple and sufficient for our pattern-matching needs.
    for node in ast.walk(tree):
        # We only care about function/method calls (ast.Call nodes)
        if not isinstance(node, ast.Call):
            continue

        # --- Pattern 1: spark.table("name") -> source ---
        if _is_spark_table_call(node):
            table_name = _get_first_string_arg(node)
            if table_name:
                source_tables.append(_strip_database_prefix(table_name))
            continue

        # --- Pattern 2: spark.read.table("name") -> source ---
        if _is_spark_read_table_call(node):
            table_name = _get_first_string_arg(node)
            if table_name:
                source_tables.append(_strip_database_prefix(table_name))
            continue

        # --- Pattern 3: spark.sql("SELECT ...") -> delegate to SQL parser ---
        if _is_spark_sql_call(node):
            sql_text = _get_first_string_arg(node)
            if sql_text:
                # Delegate to the regex-based SQL parser for full extraction.
                # The SQL parser returns source/target tables, key columns,
                # KPI metrics, and job type from the SQL string.
                sql_result = parse_sql(sql_text)

                # Merge the SQL parser results into our accumulators.
                source_tables.extend(sql_result["source_tables"])
                target_tables.extend(sql_result["target_tables"])
                key_columns.extend(sql_result["key_columns"])
                kpi_metrics.extend(sql_result["kpi_metrics"])

                # If the SQL contains aggregation, mark this pipeline as such.
                if sql_result["job_type"] == "aggregation":
                    has_aggregation = True

                # Store the SQL string for the transformation logic.
                sql_strings.append(sql_text)
            continue

        # --- For all other calls, check the method name ---
        method_name = _get_func_name(node)
        if method_name is None:
            continue

        # --- Pattern 4: Write methods -> target table ---
        if method_name in WRITE_METHODS:
            table_name = _get_first_string_arg(node)
            if table_name:
                target_tables.append(_strip_database_prefix(table_name))

        # --- Pattern 5: join -> extract key columns from comparison ---
        if method_name == "join" and len(node.args) >= 2:
            # The second argument to .join() is the join condition.
            # In PySpark, this is typically a Column comparison like
            # df1.col == df2.col, which AST represents as ast.Compare.
            condition_arg = node.args[1]
            if isinstance(condition_arg, ast.Compare):
                join_keys = _extract_join_keys_from_compare(condition_arg)
                key_columns.extend(join_keys)

        # --- Pattern 6: groupBy / agg -> aggregation detection ---
        if method_name in ("groupBy", "agg"):
            has_aggregation = True

        # --- Pattern 7: Track transformation methods for logic summary ---
        if method_name in TRANSFORM_METHODS or method_name in WRITE_METHODS:
            transform_methods.append(method_name)

    # Deduplicate all lists while preserving insertion order.
    # dict.fromkeys() preserves order and removes duplicates in Python 3.7+.
    source_tables = list(dict.fromkeys(source_tables))
    target_tables = list(dict.fromkeys(target_tables))
    key_columns = list(dict.fromkeys(key_columns))
    kpi_metrics = list(dict.fromkeys(kpi_metrics))

    # Build the transformation logic string.
    # Format: "method1 -> method2 -> ..." or "sql_query" or combined with " | "
    logic_parts: list[str] = []

    if transform_methods:
        # Join tracked methods with arrow notation to show the flow
        logic_parts.append(" -> ".join(transform_methods))

    if sql_strings:
        # Append SQL strings as-is (they already contain the full query)
        logic_parts.extend(sql_strings)

    transformation_logic = " | ".join(logic_parts)

    return ParsedPySpark(
        source_tables=source_tables,
        target_tables=target_tables,
        key_columns=key_columns,
        kpi_metrics=kpi_metrics,
        job_type="aggregation" if has_aggregation else "curated",
        transformation_logic=transformation_logic,
    )


def parse_pyspark_per_write(code: str) -> list[ParsedPySpark]:
    """Parse PySpark code and return one result per write target.

    Unlike parse_pyspark() which merges all operations into a single result,
    this function walks top-level statements in order and emits a separate
    ParsedPySpark each time it encounters a write operation (saveAsTable,
    insertInto, writeTo). Sources and transformations accumulate across
    statements, so each record captures the full pipeline that feeds into
    that particular write.

    Why per-write instead of one merged result?
      - A .py file often contains multiple logical operations: a curated
        write (bronze→silver) followed by an aggregation write (groupBy→silver).
        Merging them hides which target is curated vs aggregated and makes
        every source appear to feed into every target.
      - Per-write records let the CSV consumer see exactly which sources
        feed into each target and whether that specific flow is curated or
        aggregated.

    spark.sql() calls are NOT processed here — they are handled separately
    by the orchestrator to avoid double-counting.

    Args:
        code: Python source code as a string.

    Returns:
        A list of ParsedPySpark dicts, one per write target found.
        Returns an empty list if the code has syntax errors or no writes.
    """
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return []

    results: list[ParsedPySpark] = []

    # Accumulators that grow as we visit statements in order.
    # When a write target is found, a snapshot of these becomes the record.
    sources_so_far: list[str] = []
    keys_so_far: list[str] = []
    methods_so_far: list[str] = []
    has_aggregation = False

    # Iterate over top-level statements in source order.
    # tree.body is a list of statements in the exact order they appear in
    # the file, so sources and writes are encountered in the correct sequence.
    for stmt in tree.body:
        for node in ast.walk(stmt):
            if not isinstance(node, ast.Call):
                continue

            # Source: spark.table("name") or spark.read.table("name")
            if _is_spark_table_call(node) or _is_spark_read_table_call(node):
                table_name = _get_first_string_arg(node)
                if table_name:
                    sources_so_far.append(_strip_database_prefix(table_name))
                continue

            # Skip spark.sql() — handled separately by the orchestrator
            if _is_spark_sql_call(node):
                continue

            method_name = _get_func_name(node)
            if method_name is None:
                continue

            # Write target: emit a record with everything accumulated so far
            if method_name in WRITE_METHODS:
                table_name = _get_first_string_arg(node)
                if table_name:
                    methods_so_far.append(method_name)
                    results.append(ParsedPySpark(
                        source_tables=list(dict.fromkeys(sources_so_far)),
                        target_tables=[_strip_database_prefix(table_name)],
                        key_columns=list(dict.fromkeys(keys_so_far)),
                        kpi_metrics=[],
                        job_type="aggregation" if has_aggregation else "curated",
                        transformation_logic=" -> ".join(methods_so_far),
                    ))
                continue

            # Join keys: extract column names from join conditions
            if method_name == "join" and len(node.args) >= 2:
                condition_arg = node.args[1]
                if isinstance(condition_arg, ast.Compare):
                    join_keys = _extract_join_keys_from_compare(condition_arg)
                    keys_so_far.extend(join_keys)

            # Aggregation detection
            if method_name in ("groupBy", "agg"):
                has_aggregation = True

            # Track transformation methods
            if method_name in TRANSFORM_METHODS:
                methods_so_far.append(method_name)

    return results


def parse_notebook(notebook_json: str) -> ParsedPySpark:
    """Parse a Jupyter notebook (.ipynb) JSON and extract PySpark lineage.

    Jupyter notebooks store code in cells with a "cell_type": "code" field.
    This function:
      1. Parses the notebook JSON structure.
      2. Extracts only the code cells (ignoring markdown, raw, etc.).
      3. Concatenates all code cell source into a single string.
      4. Passes the combined code to parse_pyspark().

    Args:
        notebook_json: The raw JSON string of a .ipynb notebook file.

    Returns:
        A ParsedPySpark with lineage metadata extracted from all code cells.
        Returns an empty result if the JSON is invalid or has no code cells.
    """
    try:
        notebook = json.loads(notebook_json)
    except (json.JSONDecodeError, TypeError):
        return _empty_result()

    # Extract source lines from code cells and concatenate them.
    # Each cell's "source" field is a list of strings (lines of code).
    code_parts: list[str] = []
    for cell in notebook.get("cells", []):
        if cell.get("cell_type") == "code":
            source = cell.get("source", [])
            # source can be a list of strings or a single string
            if isinstance(source, list):
                code_parts.append("".join(source))
            elif isinstance(source, str):
                code_parts.append(source)

    # Join all code cells with newlines so they form a valid Python script,
    # then delegate to the main PySpark parser.
    combined_code = "\n".join(code_parts)

    # If there were no code cells, return empty result
    if not combined_code.strip():
        return _empty_result()

    return parse_pyspark(combined_code)
