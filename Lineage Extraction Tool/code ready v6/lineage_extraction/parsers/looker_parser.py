"""Looker (LookML) parser for data lineage extraction.

This module extracts lineage metadata from LookML files (.view.lkml, .explore.lkml,
.model.lkml, .dashboard.lkml). It parses:
  - Views with sql_table_name references
  - Derived tables with native SQL or sql_query
  - Dimensions and measures with SQL expressions
  - Joins between views in explores
  - Dashboards with lookml and query references

Design decisions:
  - LookML uses a YAML-like syntax but is not pure YAML. This parser uses
    a combination of YAML parsing for some sections and regex for others.
  - sql_table_name can be a schema.table reference or a derived_table name.
  - Native derived tables contain full SQL queries that need SQL parsing.
  - Joins in explores create relationships between views (similar to foreign keys).
  - Measures with type: count, sum, avg, etc., represent business KPIs.
"""

import re
from pathlib import Path
from typing import Any, TypedDict

import yaml

try:
    # Prefer the flat-directory sql_parser shim if available.
    from sql_parser import parse_sql
except ImportError:
    # Fallback: extract source tables from SQL using column_sql_parser.
    try:
        from column_sql_parser import extract_column_mappings as _extract_col_mappings

        def parse_sql(sql: str) -> dict:
            """Shim: return source_tables extracted from SQL."""
            mappings = _extract_col_mappings(sql)
            tables = list(dict.fromkeys(m["source_table"] for m in mappings if m.get("source_table")))
            return {"source_tables": tables, "target_tables": [], "key_columns": [], "kpi_metrics": []}
    except ImportError:
        def parse_sql(sql: str) -> dict:
            """Ultimate fallback — no SQL parser available."""
            return {"source_tables": [], "target_tables": [], "key_columns": [], "kpi_metrics": []}


class ParsedLooker(TypedDict):
    """Structured result returned by parse_looker().

    Fields:
        source_tables: All tables referenced in sql_table_name and derived tables.
        target_tables: Views and explores created in this LookML file.
        key_columns: Dimensions used in joins and primary keys.
        kpi_metrics: Measure names (business KPIs).
        job_type: "aggregation" if measures exist, else "curated".
        transformation_logic: Combined description of LookML objects.
        views: List of view names and their base tables.
        explores: List of explore names and their base views.
        joins: List of join relationships between views.
        dashboards: List of dashboard names and their visualizations.
    """

    source_tables: list[str]
    target_tables: list[str]
    key_columns: list[str]
    kpi_metrics: list[str]
    job_type: str
    transformation_logic: str
    views: list[dict[str, Any]]
    explores: list[dict[str, Any]]
    joins: list[dict[str, Any]]
    dashboards: list[dict[str, Any]]


# LookML keywords that should not be treated as table names
_LOOKML_KEYWORDS: set[str] = {
    "view", "explore", "dimension", "measure", "set", "filter",
    "access_filter", "parameter", "bind_filters", "derived_table",
    "join", "relationship", "sql_on", "sql_table_name", "sql_query",
    "persist_for", "datagroup", "action", "link", "when",
    "required_access_grants", "user_access_filters", "always_filter",
    "conditionally_filter", "fields", "hidden", "sorts", "row",
    "column", "header", "html", "plot", "series", "x_axis", "y_axis",
    "type", "label", "description", "primary_key", "hidden", "group",
    "group_label", "view_label", "tags", "alias",
}


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


def _read_lookml_file(file_path: Path) -> str:
    """Read a LookML file and return its content.

    Args:
        file_path: Path to the LookML file.

    Returns:
        The file content as a string.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"LookML file not found: {file_path}")

    return file_path.read_text(encoding="utf-8")


def _extract_lookml_blocks(content: str, block_type: str) -> list[str]:
    """Extract top-level blocks (view, explore, dashboard, etc.) from LookML.

    LookML uses indentation-based block structure. This function extracts
    blocks like:
      - view: view_name { ... }
      - explore: explore_name { ... }
      - dashboard: dashboard_name { ... }

    Args:
        content: LookML file content.
        block_type: Type of block to extract (e.g., "view", "explore").

    Returns:
        List of block contents (including the block declaration line).
    """
    # Pattern to match blocks like: view: name { ... }
    # LookML uses "type: name {" syntax with a colon
    # We handle nested braces by counting opening/closing braces
    blocks: list[str] = []
    pattern = rf"\b{block_type}:\s*\w+\s*\{{"

    for match in re.finditer(pattern, content, re.IGNORECASE):
        start = match.start()
        brace_count = 0
        in_block = False
        end = start

        # Count braces to find the end of the block
        for i in range(start, len(content)):
            if content[i] == '{':
                brace_count += 1
                in_block = True
            elif content[i] == '}':
                brace_count -= 1
                if in_block and brace_count == 0:
                    end = i + 1
                    break

        if end > start:
            blocks.append(content[start:end])

    return blocks


def _parse_view_block(view_content: str) -> dict[str, Any]:
    """Parse a LookML view block and extract lineage information.

    Args:
        view_content: Content of a single view block.

    Returns:
        Dictionary with view name, sql_table_name, dimensions, measures, etc.
    """
    view_info: dict[str, Any] = {
        "name": "",
        "sql_table_name": "",
        "derived_table": None,
        "dimensions": [],
        "measures": [],
        "primary_key": "",
    }

    # Extract view name - handle "view: name {" syntax
    view_match = re.search(r"view:\s*(\w+)", view_content, re.IGNORECASE)
    if view_match:
        view_info["name"] = view_match.group(1)

    # Extract sql_table_name - handle "sql_table_name: schema.table ;;" syntax
    sql_table_match = re.search(
        r"sql_table_name\s*:\s*([\w.\"'`\.]+)\s*;;", view_content, re.IGNORECASE
    )
    if sql_table_match:
        view_info["sql_table_name"] = sql_table_match.group(1).strip('"\'`')

    # Check for derived_table
    if "derived_table:" in view_content.lower() or "derived_table {" in view_content.lower():
        derived_content = _extract_derived_table_content(view_content)
        view_info["derived_table"] = derived_content

        # Extract sql_query from derived_table
        sql_query_match = re.search(
            r"sql_query\s*:\s*\|\s*-?\s*(.+?)(?=\n\s*\w+\s*:|$)",
            derived_content,
            re.DOTALL
        )
        if sql_query_match:
            view_info["derived_sql"] = sql_query_match.group(1).strip()

    # Extract dimensions - handle "dimension: name {" syntax
    for dim_match in re.finditer(
        r"dimension:\s*(\w+)\s*\{([^}]+)\}", view_content, re.DOTALL
    ):
        dim_name = dim_match.group(1)
        dim_body = dim_match.group(2)

        dim_info = {"name": dim_name}

        # Extract sql expression - handle "sql: ${TABLE}.column ;;" syntax
        sql_match = re.search(
            r"sql\s*:\s*([^;]+?)(?=\s*;;|\n\s*\w+\s*:)", dim_body, re.DOTALL
        )
        if sql_match:
            dim_info["sql"] = sql_match.group(1).strip()

        # Check for primary_key
        if "primary_key" in dim_body.lower() or "primary key" in dim_body.lower():
            view_info["primary_key"] = dim_name
            dim_info["is_primary_key"] = True

        # Extract type
        type_match = re.search(r"type\s*:\s*(\w+)", dim_body)
        if type_match:
            dim_info["type"] = type_match.group(1)

        view_info["dimensions"].append(dim_info)

    # Extract measures (aggregations = KPIs) - handle "measure: name {" syntax
    for measure_match in re.finditer(
        r"measure:\s*(\w+)\s*\{([^}]+)\}", view_content, re.DOTALL
    ):
        measure_name = measure_match.group(1)
        measure_body = measure_match.group(2)

        measure_info = {"name": measure_name}

        # Extract sql expression - handle "sql: ${column} ;;" syntax
        sql_match = re.search(
            r"sql\s*:\s*([^;]+?)(?=\s*;;|\n\s*\w+\s*:)", measure_body, re.DOTALL
        )
        if sql_match:
            measure_info["sql"] = sql_match.group(1).strip()

        # Extract type (count, sum, avg, etc.)
        type_match = re.search(r"type\s*:\s*(\w+)", measure_body)
        if type_match:
            measure_info["type"] = type_match.group(1)

        view_info["measures"].append(measure_info)

    return view_info


def _extract_derived_table_content(view_content: str) -> str:
    """Extract the derived_table block content from a view.

    Args:
        view_content: Content of a view block.

    Returns:
        The derived_table block content.
    """
    # Find derived_table: or derived_table { and capture until closing brace
    pattern = r"derived_table\s*\{?(.+?)(?=\n\s*\w+\s*:|\n\s*\})"
    match = re.search(pattern, view_content, re.DOTALL)
    if match:
        return match.group(1)
    return ""


def _parse_explore_block(explore_content: str) -> dict[str, Any]:
    """Parse a LookML explore block and extract join relationships.

    Args:
        explore_content: Content of a single explore block.

    Returns:
        Dictionary with explore name, base view, and joins.
    """
    explore_info: dict[str, Any] = {
        "name": "",
        "base_view": "",
        "joins": [],
        "always_filter": None,
    }

    # Extract explore name - handle "explore: name {" syntax
    explore_match = re.search(r"explore:\s*(\w+)", explore_content, re.IGNORECASE)
    if explore_match:
        explore_info["name"] = explore_match.group(1)
        # Default base view is same as explore name unless from: is specified
        explore_info["base_view"] = explore_match.group(1)

    # Check for from: clause
    from_match = re.search(r"from\s*:\s*(\w+)", explore_content, re.IGNORECASE)
    if from_match:
        explore_info["base_view"] = from_match.group(1)

    # Extract joins - handle "join: name {" syntax
    for join_match in re.finditer(
        r"join:\s*(\w+)\s*\{([^}]+)\}", explore_content, re.DOTALL
    ):
        join_name = join_match.group(1)
        join_body = join_match.group(2)

        join_info: dict[str, Any] = {"name": join_name}

        # Extract sql_on (join condition)
        sql_on_match = re.search(
            r"sql_on\s*:\s*([^;]+?)(?=\n\s*\w+\s*:|;\s*$)", join_body, re.DOTALL
        )
        if sql_on_match:
            join_info["sql_on"] = sql_on_match.group(1).strip()

        # Extract relationship type
        rel_match = re.search(r"relationship\s*:\s*(\w+)", join_body)
        if rel_match:
            join_info["relationship"] = rel_match.group(1)

        # Extract from: if specified
        from_join_match = re.search(r"from\s*:\s*(\w+)", join_body)
        if from_join_match:
            join_info["from"] = from_join_match.group(1)

        explore_info["joins"].append(join_info)

    # Extract always_filter
    always_filter_match = re.search(
        r"always_filter\s*\{([^}]+)\}", explore_content, re.DOTALL
    )
    if always_filter_match:
        explore_info["always_filter"] = always_filter_match.group(1).strip()

    return explore_info


def _parse_dashboard_block(dashboard_content: str) -> dict[str, Any]:
    """Parse a LookML dashboard block and extract visualization info.

    Args:
        dashboard_content: Content of a single dashboard block.

    Returns:
        Dictionary with dashboard name, tiles, and queries.
    """
    dashboard_info: dict[str, Any] = {
        "name": "",
        "title": "",
        "tiles": [],
    }

    # Extract dashboard name - handle "dashboard: name {" syntax
    dash_match = re.search(r"dashboard:\s*([\w_]+)", dashboard_content, re.IGNORECASE)
    if dash_match:
        dashboard_info["name"] = dash_match.group(1)

    # Extract title
    title_match = re.search(r"title\s*:\s*\"([^\"]+)\"", dashboard_content)
    if title_match:
        dashboard_info["title"] = title_match.group(1)

    # Extract elements/tiles
    for element_match in re.finditer(
        r"(?:element|tile)\s*:\s*\{([^}]+)\}", dashboard_content, re.DOTALL
    ):
        element_info: dict[str, Any] = {}

        element_body = element_match.group(1)

        # Extract name or title
        name_match = re.search(r"name\s*:\s*\"([^\"]+)\"", element_body)
        if name_match:
            element_info["name"] = name_match.group(1)

        # Extract type (chart type, table, etc.)
        type_match = re.search(r"type\s*:\s*\"([^\"]+)\"", element_body)
        if type_match:
            element_info["type"] = type_match.group(1)

        # Extract model, explore, view (for query lookml)
        model_match = re.search(r"model\s*:\s*\"([^\"]+)\"", element_body)
        if model_match:
            element_info["model"] = model_match.group(1)

        explore_match = re.search(r"explore\s*:\s*\"([^\"]+)\"", element_body)
        if explore_match:
            element_info["explore"] = explore_match.group(1)

        view_match = re.search(r"view\s*:\s*\"([^\"]+)\"", element_body)
        if view_match:
            element_info["view"] = view_match.group(1)

        if element_info:
            dashboard_info["tiles"].append(element_info)

    return dashboard_info


def _extract_key_columns_from_joins(joins: list[dict[str, Any]]) -> list[str]:
    """Extract column names used in join conditions.

    Args:
        joins: List of join information dictionaries.

    Returns:
        List of column names used in join sql_on clauses.
    """
    columns: list[str] = []

    for join in joins:
        sql_on = join.get("sql_on", "")
        if sql_on:
            # Extract column references from sql_on
            # Pattern: ${view_name.column_name} or view_name.column_name
            col_pattern = r"\$\{?\w+\.(\w+)\}?"
            for match in re.finditer(col_pattern, sql_on):
                columns.append(match.group(1))

            # Also extract bare columns after operators
            bare_col_pattern = r"[\s=<>!]+([A-Za-z_]\w*)\s*[\s=<>!]?"
            for match in re.finditer(bare_col_pattern, sql_on):
                col = match.group(1)
                if col not in _LOOKML_KEYWORDS:
                    columns.append(col)

    return list(dict.fromkeys(columns))


def _extract_source_tables_from_views(
    views: list[dict[str, Any]], sql_queries: list[str]
) -> list[str]:
    """Extract source table references from views.

    Args:
        views: List of view information dictionaries.
        sql_queries: Output list that will contain SQL to parse.

    Returns:
        List of source table names.
    """
    source_tables: list[str] = []

    for view in views:
        # Add sql_table_name if present
        sql_table = view.get("sql_table_name", "")
        if sql_table and sql_table not in _LOOKML_KEYWORDS:
            source_tables.append(_strip_database_prefix(sql_table))

        # Add derived table SQL for parsing
        derived_sql = view.get("derived_sql", "")
        if derived_sql:
            sql_queries.append(derived_sql)

    return source_tables


def _extract_key_columns_from_dimensions(
    views: list[dict[str, Any]],
) -> list[str]:
    """Extract primary key and important dimension columns.

    Args:
        views: List of view information dictionaries.

    Returns:
        List of key column names.
    """
    key_columns: list[str] = []

    for view in views:
        # Add primary key if present
        pk = view.get("primary_key", "")
        if pk:
            key_columns.append(f"{view['name']}.{pk}")

        # Add dimensions with type: string that might be keys
        for dim in view.get("dimensions", []):
            dim_name = dim.get("name", "")
            dim_type = dim.get("type", "")

            # String dimensions in primary position are potential keys
            if dim_type == "string" and not dim_name.startswith("is_"):
                # Check if it's referenced in sql (more likely to be a key)
                sql = dim.get("sql", "")
                if "." in sql or dim_name.lower() in ("id", "key", "code", "name"):
                    key_columns.append(f"{view['name']}.{dim_name}")

    return key_columns


def _build_transformation_logic_lookml(
    views: list[dict[str, Any]],
    explores: list[dict[str, Any]],
    dashboards: list[dict[str, Any]],
) -> str:
    """Build transformation logic description from LookML objects.

    Args:
        views: List of view information.
        explores: List of explore information.
        dashboards: List of dashboard information.

    Returns:
        Formatted transformation logic description (single line for CSV).
    """
    parts: list[str] = []

    if views:
        view_info = []
        for view in views[:5]:  # Limit to first 5
            view_name = view["name"]
            sql_table = view.get("sql_table_name", "")
            measure_count = len(view.get("measures", []))
            if sql_table:
                view_info.append(f"{view_name}({sql_table},{measure_count}measures)")
            else:
                view_info.append(f"{view_name}(derived_table,{measure_count}measures)")

        if view_info:
            parts.append(f"Views: {', '.join(view_info)}")
        if len(views) > 5:
            parts.append(f"... and {len(views) - 5} more views")

    if explores:
        explore_info = []
        for explore in explores[:3]:  # Limit to first 3
            explore_name = explore["name"]
            base_view = explore.get("base_view", "")
            join_count = len(explore.get("joins", []))
            explore_info.append(f"{explore_name}(base:{base_view},{join_count}joins)")

        if explore_info:
            parts.append(f"Explores: {', '.join(explore_info)}")
        if len(explores) > 3:
            parts.append(f"... and {len(explores) - 3} more explores")

    if dashboards:
        dash_info = []
        for dash in dashboards[:3]:
            dash_name = dash.get("name", dash.get("title", "unnamed"))
            tile_count = len(dash.get("tiles", []))
            dash_info.append(f"{dash_name}({tile_count}tiles)")

        if dash_info:
            parts.append(f"Dashboards: {', '.join(dash_info)}")
        if len(dashboards) > 3:
            parts.append(f"... and {len(dashboards) - 3} more dashboards")

    return " | ".join(parts) if parts else "LookML model with standard definitions"


def _detect_job_type_looker(views: list[dict[str, Any]]) -> str:
    """Detect whether this LookML file is an aggregation or curated.

    Aggregation if any views have measures (which are aggregations).

    Args:
        views: List of view information dictionaries.

    Returns:
        "aggregation" if measures exist, else "curated".
    """
    for view in views:
        if view.get("measures"):
            return "aggregation"
    return "curated"


def parse_looker(file_path: str | Path) -> ParsedLooker:
    """Parse a LookML file and extract lineage metadata.

    This is the main entry point for LookML parsing. It handles:
      - .view.lkml files (view definitions)
      - .explore.lkml files (explore definitions)
      - .model.lkml files (model with multiple explores)
      - .dashboard.lkml files (dashboard definitions)

    Args:
        file_path: Path to the LookML file.

    Returns:
        A ParsedLooker TypedDict with all extracted lineage metadata.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file content cannot be parsed.
    """
    file_path = Path(file_path)
    content = _read_lookml_file(file_path)

    # Determine file type from extension
    file_type = ""
    if ".view." in file_path.name:
        file_type = "view"
    elif ".explore." in file_path.name:
        file_type = "explore"
    elif ".model." in file_path.name:
        file_type = "model"
    elif ".dashboard." in file_path.name or ".dash." in file_path.name:
        file_type = "dashboard"

    # Parse based on file type
    views: list[dict[str, Any]] = []
    explores: list[dict[str, Any]] = []
    dashboards: list[dict[str, Any]] = []
    joins: list[dict[str, Any]] = []
    sql_queries: list[str] = []

    if file_type == "view":
        # Parse view blocks
        view_blocks = _extract_lookml_blocks(content, "view")
        for block in view_blocks:
            views.append(_parse_view_block(block))

    elif file_type == "explore" or file_type == "model":
        # Parse explore blocks
        explore_blocks = _extract_lookml_blocks(content, "explore")
        for block in explore_blocks:
            explore_info = _parse_explore_block(block)
            explores.append(explore_info)
            joins.extend(explore_info.get("joins", []))

        # Also parse any view blocks within the file
        view_blocks = _extract_lookml_blocks(content, "view")
        for block in view_blocks:
            views.append(_parse_view_block(block))

    elif file_type == "dashboard":
        # Parse dashboard blocks
        dashboard_blocks = _extract_lookml_blocks(content, "dashboard")
        for block in dashboard_blocks:
            dashboards.append(_parse_dashboard_block(block))

    else:
        # Unknown file type - try to parse all block types
        view_blocks = _extract_lookml_blocks(content, "view")
        for block in view_blocks:
            views.append(_parse_view_block(block))

        explore_blocks = _extract_lookml_blocks(content, "explore")
        for block in explore_blocks:
            explore_info = _parse_explore_block(block)
            explores.append(explore_info)
            joins.extend(explore_info.get("joins", []))

        dashboard_blocks = _extract_lookml_blocks(content, "dashboard")
        for block in dashboard_blocks:
            dashboards.append(_parse_dashboard_block(block))

    # Extract source tables from views
    source_tables = _extract_source_tables_from_views(views, sql_queries)

    # Parse SQL queries from derived tables
    for query in sql_queries:
        try:
            parsed = parse_sql(query)
            source_tables.extend(parsed["source_tables"])
        except Exception:
            pass

    # Extract target tables (view names)
    target_tables = [view.get("name", "") for view in views if view.get("name")]

    # Extract explore names as targets too
    target_tables.extend(
        [exp.get("name", "") for exp in explores if exp.get("name")]
    )

    # Extract key columns
    key_columns = _extract_key_columns_from_joins(joins)
    key_columns.extend(_extract_key_columns_from_dimensions(views))

    # Extract KPI metrics (measures)
    kpi_metrics: list[str] = []
    for view in views:
        for measure in view.get("measures", []):
            view_name = view.get("name", "")
            measure_name = measure.get("name", "")
            measure_type = measure.get("type", "")
            if measure_name:
                kpi_metrics.append(f"{view_name}.{measure_name} ({measure_type})")

    # Detect job type
    job_type = _detect_job_type_looker(views)

    # Build transformation logic
    transformation_logic = _build_transformation_logic_lookml(
        views, explores, dashboards
    )

    return ParsedLooker(
        source_tables=list(dict.fromkeys(source_tables)),
        target_tables=list(dict.fromkeys(target_tables)),
        key_columns=list(dict.fromkeys(key_columns)),
        kpi_metrics=kpi_metrics,
        job_type=job_type,
        transformation_logic=transformation_logic,
        views=views,
        explores=explores,
        joins=joins,
        dashboards=dashboards,
    )


# ---------------------------------------------------------------------------
# Reporting lineage extraction (separate from ETL lineage above)
# ---------------------------------------------------------------------------


def extract_reporting_lineage(file_path: "str | Path") -> dict:
    """Extract reporting lineage from a Looker .lkml file.

    Reuses the existing private helpers (_extract_lookml_blocks,
    _parse_view_block, _parse_explore_block, _extract_source_tables_from_views)
    already in this module to avoid duplication.

    Handles all LookML file types:
      - .view.lkml: sql_name=view names, tables=sql_table_name, columns=dimensions+measures
      - .explore.lkml: sql_name=explore names, tables=view names from joins/from,
        columns=column names from sql_on join conditions
      - .model.lkml: combination of explore and view extraction
      - .dashboard.lkml: sql_name=explore references from elements,
        columns=dimension and measure references from elements

    operation = SELECT always; JOIN if explores have join: blocks;
                AGGREGATE if any measure has an aggregating type
                (sum, count, average, count_distinct, sum_distinct,
                percent_of_total, running_total)

    Args:
        file_path: Path to the .lkml file.

    Returns:
        Dict with keys: sql_name, tables, columns, operation.
        Values are lists of strings (may be empty).
    """
    file_path = Path(file_path)
    content = _read_lookml_file(file_path)

    # Determine file type from name convention
    file_type = ""
    if ".view." in file_path.name:
        file_type = "view"
    elif ".explore." in file_path.name:
        file_type = "explore"
    elif ".model." in file_path.name:
        file_type = "model"
    elif ".dashboard." in file_path.name or ".dash." in file_path.name:
        file_type = "dashboard"

    # --- Parse all block types present in the file ---
    views: list[dict] = []
    explores: list[dict] = []

    for block in _extract_lookml_blocks(content, "view"):
        views.append(_parse_view_block(block))

    for block in _extract_lookml_blocks(content, "explore"):
        explores.append(_parse_explore_block(block))

    # --- sql_name: view names + explore names ---
    sql_names: list[str] = []
    for v in views:
        name = v.get("name", "")
        if name:
            sql_names.append(name)
    for e in explores:
        name = e.get("name", "")
        if name:
            sql_names.append(name)

    # --- tables: from views (sql_table_name + derived_table SQL) ---
    sql_queries: list[str] = []
    tables: list[str] = _extract_source_tables_from_views(views, sql_queries)
    for query in sql_queries:
        try:
            parsed = parse_sql(query)
            tables.extend(parsed["source_tables"])
        except Exception:
            pass

    # For explore/model files: also extract view names from join and from clauses.
    # Explore files reference views via joins but don't define sql_table_name
    # directly — the view names ARE the table references for lineage purposes.
    if file_type in ("explore", "model", ""):
        for explore in explores:
            # Base view (from explore name or explicit from: clause)
            base_view = explore.get("base_view", "") or explore.get("name", "")
            if base_view:
                tables.append(base_view)
            # Joined views
            for join in explore.get("joins", []):
                join_name = join.get("name", "")
                if join_name:
                    tables.append(join_name)

    # --- columns: dimension and measure field names from views ---
    columns: list[str] = []
    for view in views:
        for dim in view.get("dimensions", []):
            name = dim.get("name", "")
            if name:
                columns.append(name)
        for measure in view.get("measures", []):
            name = measure.get("name", "")
            if name:
                columns.append(name)

    # For explore/model files: extract column names from sql_on join conditions.
    # sql_on like "${orders.customer_id} = ${customers.customer_id}" yields
    # column names customer_id, order_id, etc.
    # NOTE: We extract directly from the raw explore block content rather than
    # relying on _parse_explore_block's join parsing, which has a known bug
    # where [^}]+ truncates at the } inside ${view.column} references.
    if file_type in ("explore", "model", ""):
        for block in _extract_lookml_blocks(content, "explore"):
            for match in re.finditer(r"\$\{(\w+)\.(\w+)\}", block):
                col_name = match.group(2)
                columns.append(col_name)

    # For dashboard files: extract explore references and dimension/measure
    # names from element definitions. Dashboards don't have view/explore
    # blocks — they have elements: { explore: "name", dimensions: [...], ... }
    if file_type == "dashboard":
        # Extract explore references from elements
        for explore_match in re.finditer(
            r"explore\s*:\s*\"(\w+)\"", content
        ):
            name = explore_match.group(1)
            if name not in sql_names:
                sql_names.append(name)

        # Extract column names from dimensions: [...] and measures: [...] arrays
        # These contain references like "orders.order_date" → "order_date"
        for dim_match in re.finditer(r"dimensions\s*:\s*\[([^\]]+)\]", content):
            for ref in re.finditer(r"\w+\.(\w+)", dim_match.group(1)):
                col = ref.group(1)
                columns.append(col)

        for meas_match in re.finditer(r"measures\s*:\s*\[([^\]]+)\]", content):
            for ref in re.finditer(r"\w+\.(\w+)", meas_match.group(1)):
                col = ref.group(1)
                columns.append(col)

    # --- operation: SELECT always; JOIN if any explore has join blocks;
    #     AGGREGATE if any measure has an aggregating type ---
    ops: set[str] = {"SELECT"}

    has_joins = any(e.get("joins") for e in explores)
    if has_joins:
        ops.add("JOIN")

    _AGGREGATE_TYPES = {"sum", "count", "average", "count_distinct",
                        "sum_distinct", "percent_of_total", "running_total"}
    for view in views:
        for measure in view.get("measures", []):
            if measure.get("type", "").lower() in _AGGREGATE_TYPES:
                ops.add("AGGREGATE")
                break
        if "AGGREGATE" in ops:
            break

    return {
        "sql_name": list(dict.fromkeys(sql_names)),
        "tables": list(dict.fromkeys(tables)),
        "columns": list(dict.fromkeys(columns)),
        "operation": sorted(ops),
    }
