"""Power BI (.pbit) parser for data lineage extraction.

This module extracts lineage metadata from Power BI template files (.pbit),
which are JSON-formatted representations of Power BI Desktop files. It parses:
  - Data sources (SQL, BigQuery, Excel, Web API)
  - Power Query/M transformations
  - Data model tables and relationships
  - DAX measures and calculated columns
  - Report visualizations

Design decisions:
  - .pbit files are JSON, unlike .pbix which are ZIP archives containing
    DataModel, DiagramLayout, and other components. This parser focuses on
    .pbit for simplicity but can be extended to .pbix.
  - SQL queries are extracted from dataSources and parsed using the existing
    SQL parser for consistent table/column extraction.
  - Power Query/M expressions are parsed for table references using regex
    patterns similar to SQL parsing.
  - DAX measures and calculated columns are extracted as transformation logic.
  - Multiple LineageRecord objects are created: one per data source query,
    one per Power Query transformation, and one for the model-level relationships.
"""

import json
import re
from pathlib import Path
from typing import Any, TypedDict

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


class ParsedPowerBI(TypedDict):
    """Structured result returned by parse_powerbi().

    Fields:
        source_tables: All tables read from any data source.
        target_tables: Tables/views created in the data model.
        key_columns: Columns used in relationships and joins.
        kpi_metrics: DAX measure names (business KPIs).
        job_type: "aggregation" if any aggregations exist, else "curated".
        transformation_logic: Combined description of all transformations.
        data_sources: List of data source names and types.
        power_query_transformations: List of Power Query transformation names.
        dax_measures: List of DAX measure definitions.
        relationships: List of table relationships in the data model.
    """

    source_tables: list[str]
    target_tables: list[str]
    key_columns: list[str]
    kpi_metrics: list[str]
    job_type: str
    transformation_logic: str
    data_sources: list[dict[str, Any]]
    power_query_transformations: list[str]
    dax_measures: list[str]
    relationships: list[dict[str, Any]]


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


# Keywords to exclude from Power Query table references
_POWER_QUERY_KEYWORDS: set[str] = {
    "let", "in", "each", "if", "then", "else", "and", "or", "not",
    "true", "false", "null", "type", "table", "record", "list",
    "Source", "Table", "Text", "Date", "DateTime", "Duration",
    "Number", "Logical", "Binary", "Function", "Any", "None",
}


def _parse_pbit_json(file_path: Path) -> dict[str, Any]:
    """Read and parse a .pbit file as JSON.

    .pbit files can be either:
      - A ZIP archive (real Power BI template) containing a 'DataModel' entry
      - A plain JSON file (test/sample files)

    Args:
        file_path: Path to the .pbit file.

    Returns:
        Parsed JSON content as a dictionary.

    Raises:
        ValueError: If the file cannot be parsed.
        FileNotFoundError: If the file does not exist.
    """
    import zipfile as _zf

    # Try ZIP first (real .pbit format)
    try:
        with _zf.ZipFile(file_path, "r") as zf:
            names = zf.namelist()
            # Priority order: DataModel, Report/Layout, first .json entry
            candidates = (
                [n for n in names if n == "DataModel"] +
                [n for n in names if n == "Report/Layout"] +
                [n for n in names if n.endswith(".json")]
            )
            if candidates:
                raw = zf.read(candidates[0]).decode("utf-8", errors="replace")
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    pass
            # No JSON entry found — return empty dict so parser gracefully returns empty
            return {}
    except _zf.BadZipFile:
        pass

    # Fallback: try plain JSON (sample/test files)
    try:
        content = file_path.read_text(encoding="utf-8")
        return json.loads(content)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"Cannot parse {file_path} as ZIP or JSON: {exc}") from exc


def _extract_metadata_from_pbit(pbit_data: dict[str, Any]) -> dict[str, Any]:
    """Extract metadata from the PBIT file structure.

    Looks for DOMAIN, SUBJECT_AREA, SCHEDULE, and OWNER in the metadata
    section of the PBIT file.

    Args:
        pbit_data: Parsed PBIT JSON content.

    Returns:
        Dictionary with domain, subject_area, schedule, and owner.
    """
    metadata = pbit_data.get("metadata", {})

    return {
        "domain": metadata.get("DOMAIN", "unknown"),
        "subject_area": metadata.get("SUBJECT_AREA", "unknown"),
        "schedule": metadata.get("SCHEDULE"),
        "owner": metadata.get("OWNER"),
    }


def _extract_data_source_tables(
    data_sources: list[dict[str, Any]], queries_to_parse: list[str]
) -> tuple[list[str], list[str], list[str]]:
    """Extract source tables and SQL from data source definitions.

    Args:
        data_sources: List of data source objects from the PBIT file.
        queries_to_parse: Output list that will contain SQL queries to parse.

    Returns:
        Tuple of (source_tables, sql_tables, transformation_descriptions).
    """
    source_tables: list[str] = []
    sql_tables: list[str] = []
    transformations: list[str] = []

    for ds in data_sources:
        ds_type = ds.get("type", "unknown")
        ds_name = ds.get("name", "unnamed_source")

        # Extract queries from SQL data sources
        if ds_type in ("sql", "bigquery", "snowflake", "postgres"):
            for query_obj in ds.get("queries", []):
                query_name = query_obj.get("name", "unnamed_query")
                query_sql = query_obj.get("query", "")

                if query_sql:
                    queries_to_parse.append(query_sql)
                    sql_tables.append(query_name)
                    transformations.append(
                        f"SQL Query '{query_name}' from {ds_name} ({ds_type})"
                    )

                # Add connection info as a source reference
                conn_str = ds.get("connectionString", "")
                if conn_str:
                    # Extract database name from connection string
                    if "Database=" in conn_str or "database=" in conn_str:
                        db_match = re.search(
                            r"[Dd]atabase=([^;]+)", conn_str
                        )
                        if db_match:
                            source_tables.append(f"{ds_type}://{db_match.group(1)}")

        # For BigQuery, extract project and dataset info
        elif ds_type == "bigquery":
            conn_str = ds.get("connectionString", "")
            if "project=" in conn_str:
                project_match = re.search(r"project=([^;]+)", conn_str)
                if project_match:
                    source_tables.append(f"bigquery://{project_match.group(1)}")

        # For Excel files, track the file path
        elif ds_type == "excel":
            file_path = ds.get("path", "")
            if file_path:
                source_tables.append(f"excel:{file_path}")

        # For Web/API sources, track the endpoint
        elif ds_type == "web":
            base_url = ds.get("baseUrl", "")
            if base_url:
                source_tables.append(f"api:{base_url}")

    return source_tables, sql_tables, transformations


def _extract_power_query_tables(
    transformations: list[dict[str, Any]],
) -> tuple[list[str], list[str], list[str]]:
    """Extract table references from Power Query/M expressions.

    Args:
        transformations: List of Power Query transformation objects.

    Returns:
        Tuple of (source_tables, target_tables, transformation_names).
    """
    source_tables: list[str] = []
    target_tables: list[str] = []
    transformation_names: list[str] = []

    for transform in transformations:
        transform_name = transform.get("name", "unnamed_transform")
        transformation_names.append(transform_name)
        target_tables.append(transform_name)

        m_expression = transform.get("mExpression", "")

        # Extract Source references (typically the first step)
        source_pattern = r'\bSource\s*=\s*([A-Za-z_][\w.]*(?:\([^)]*\))?)'
        for match in re.finditer(source_pattern, m_expression):
            source_ref = match.group(1)

            # Clean up the reference (remove function calls, etc.)
            if "(" in source_ref:
                source_ref = source_ref.split("(")[0]

            if source_ref and source_ref not in _POWER_QUERY_KEYWORDS:
                source_tables.append(source_ref)

        # Extract table references from SqL.Database, Table.Buffer, etc.
        sql_db_pattern = r"Sql\.Database\s*\(\s*['\"]([^'\"]+)['\"]"
        for match in re.finditer(sql_db_pattern, m_expression):
            source_tables.append(f"sql://{match.group(1)}")

        # Extract nested table references (e.g., Table.NestedJoin)
        nested_table_pattern = r"Table\.NestedJoin\s*\([^,]+,\s*\{[^\}]+\},\s*([A-Za-z_][\w]*)"
        for match in re.finditer(nested_table_pattern, m_expression):
            ref_table = match.group(1)
            if ref_table not in _POWER_QUERY_KEYWORDS:
                source_tables.append(ref_table)

        # Extract table references from previous query steps
        # Patterns like: PreviousStep = ..., CurrentStep = PreviousStep
        step_ref_pattern = r'=\s*([A-Za-z_][\w]*)\s*(?:,|\)|$)'
        for match in re.finditer(step_ref_pattern, m_expression):
            step_ref = match.group(1)
            if step_ref and step_ref not in _POWER_QUERY_KEYWORDS:
                source_tables.append(step_ref)

    return source_tables, target_tables, transformation_names


def _extract_data_model_tables(
    data_model: dict[str, Any],
) -> tuple[list[str], list[str]]:
    """Extract tables and relationships from the data model.

    Args:
        data_model: Data model section from the PBIT file.

    Returns:
        Tuple of (model_tables, relationships_info).
    """
    model_tables: list[str] = []
    relationships_info: list[dict[str, Any]] = []

    tables = data_model.get("tables", [])
    for table in tables:
        table_name = table.get("name", "")
        if table_name:
            model_tables.append(table_name)

    relationships = data_model.get("relationships", [])
    for rel in relationships:
        relationships_info.append({
            "name": rel.get("name", ""),
            "from_table": rel.get("from", ""),
            "to_table": rel.get("to", ""),
            "from_column": rel.get("fromColumn", ""),
            "to_column": rel.get("toColumn", ""),
        })

    return model_tables, relationships_info


def _extract_dax_measures(
    measures: list[dict[str, Any]],
    calculated_columns: list[dict[str, Any]],
) -> list[str]:
    """Extract DAX measure and calculated column definitions.

    Args:
        measures: List of DAX measure objects.
        calculated_columns: List of calculated column objects.

    Returns:
        List of formatted DAX measure definitions.
    """
    dax_definitions: list[str] = []

    for measure in measures:
        table = measure.get("table", "")
        name = measure.get("name", "")
        expression = measure.get("expression", "")
        if table and name and expression:
            dax_definitions.append(f"MEASURE {table}[{name}] = {expression}")

    for col in calculated_columns:
        table = col.get("table", "")
        name = col.get("name", "")
        expression = col.get("expression", "")
        if table and name and expression:
            dax_definitions.append(f"COLUMN {table}[{name}] = {expression}")

    return dax_definitions


def _extract_report_visuals(
    report_pages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Extract visualization information from report pages.

    Args:
        report_pages: List of report page objects.

    Returns:
        List of visualization metadata.
    """
    visuals: list[dict[str, Any]] = []

    for page in report_pages:
        page_name = page.get("name", "")
        for visual in page.get("visuals", []):
            visuals.append({
                "page": page_name,
                "type": visual.get("type", ""),
                "axes": visual.get("axis", []),
                "values": visual.get("values", []),
                "groups": visual.get("group", []),
            })

    return visuals


def _build_transformation_logic(
    data_source_desc: list[str],
    power_query_names: list[str],
    dax_measures: list[str],
    relationships: list[dict[str, Any]],
    visuals: list[dict[str, Any]],
) -> str:
    """Build a comprehensive transformation logic description.

    Args:
        data_source_desc: Descriptions of data source queries.
        power_query_names: Names of Power Query transformations.
        dax_measures: DAX measure definitions.
        relationships: Data model relationships.
        visuals: Report visualization metadata.

    Returns:
        Formatted transformation logic description (single line for CSV).
    """
    parts: list[str] = []

    if data_source_desc:
        parts.append("Data Sources: " + "; ".join(data_source_desc[:3]))
        if len(data_source_desc) > 3:
            parts.append(f"... and {len(data_source_desc) - 3} more data sources")

    if power_query_names:
        parts.append(f"Power Query: {', '.join(power_query_names[:5])}")
        if len(power_query_names) > 5:
            parts.append(f"... and {len(power_query_names) - 5} more transformations")

    if dax_measures:
        # Simplified DAX measure names only (not full expressions)
        measure_names = [m.split("[")[1].split("]")[0] if "[" in m else m[:30] for m in dax_measures[:5]]
        parts.append(f"DAX Measures: {', '.join(measure_names)}")
        if len(dax_measures) > 5:
            parts.append(f"... and {len(dax_measures) - 5} more measures")

    if relationships:
        rel_info = [f"{r['from_table']}.{r['from_column']}->{r['to_table']}.{r['to_column']}" for r in relationships[:3]]
        parts.append(f"Relationships: {', '.join(rel_info)}")
        if len(relationships) > 3:
            parts.append(f"... and {len(relationships) - 3} more")

    if visuals:
        parts.append(f"Report: {len(visuals)} visuals")

    return " | ".join(parts) if parts else "Power BI data model with standard transformations"


def _detect_job_type_powerbi(
    power_query_transforms: list[str],
    dax_measures: list[str],
    relationships: list[dict[str, Any]],
) -> str:
    """Detect whether this Power BI model is an aggregation or curated.

    Aggregation if:
      - Power Query contains Group, Aggregate, Sum, Count operations
      - DAX measures use aggregation functions (SUM, COUNT, AVG, etc.)
      - Many-to-many relationships exist (typical in aggregated models)

    Args:
        power_query_transforms: List of Power Query transformation names.
        dax_measures: List of DAX measure definitions.
        relationships: List of relationship definitions.

    Returns:
        "aggregation" if aggregation detected, else "curated".
    """
    # Check Power Query for aggregation operations
    agg_keywords = {"Group", "Aggregate", "Sum", "Count", "Average", "Max", "Min"}
    for transform in power_query_transforms:
        if any(keyword.lower() in transform.lower() for keyword in agg_keywords):
            return "aggregation"

    # Check DAX measures for aggregation functions
    dax_agg_pattern = r"\b(SUM|COUNT|AVERAGE|AVG|MAX|MIN|DISTINCTCOUNT)\s*\("
    for measure in dax_measures:
        if re.search(dax_agg_pattern, measure):
            return "aggregation"

    return "curated"


def parse_powerbi(file_path: str | Path) -> ParsedPowerBI:
    """Parse a Power BI .pbit file and extract lineage metadata.

    This is the main entry point for Power BI parsing. It extracts:
      1. Metadata from the PBIT header
      2. SQL queries from data sources
      3. Table references from Power Query transformations
      4. Data model structure and relationships
      5. DAX measures and calculated columns
      6. Report visualization metadata

    Args:
        file_path: Path to the .pbit file.

    Returns:
        A ParsedPowerBI TypedDict with all extracted lineage metadata.

    Raises:
        ValueError: If the file is not valid JSON or missing required fields.
        FileNotFoundError: If the file does not exist.
    """
    file_path = Path(file_path)

    # Parse the PBIT JSON structure
    pbit_data = _parse_pbit_json(file_path)

    # Extract all components
    data_sources = pbit_data.get("dataSources", [])
    transformations = pbit_data.get("transformations", [])
    data_model = pbit_data.get("dataModel", {})
    measures = pbit_data.get("measures", [])
    calc_columns = pbit_data.get("calculatedColumns", [])
    report_pages = pbit_data.get("reportPages", [])

    # Collect SQL queries to parse
    sql_queries_to_parse: list[str] = []

    # Extract data source information
    ds_source_tables, ds_sql_tables, ds_transforms = _extract_data_source_tables(
        data_sources, sql_queries_to_parse
    )

    # Extract Power Query transformation information
    pq_source_tables, pq_target_tables, pq_names = _extract_power_query_tables(
        transformations
    )

    # Parse SQL queries to get additional tables
    sql_source_tables: list[str] = []
    sql_target_tables: list[str] = []
    for query in sql_queries_to_parse:
        try:
            parsed = parse_sql(query)
            sql_source_tables.extend(parsed["source_tables"])
            sql_target_tables.extend(parsed["target_tables"])
        except Exception:
            # If SQL parsing fails, continue with other queries
            pass

    # Extract data model information
    model_tables, relationships = _extract_data_model_tables(data_model)

    # Extract DAX definitions
    dax_definitions = _extract_dax_measures(measures, calc_columns)

    # Extract report visuals
    visuals = _extract_report_visuals(report_pages)

    # Combine all source tables
    all_source_tables = list(dict.fromkeys(
        ds_source_tables + pq_source_tables + sql_source_tables
    ))

    # Combine all target tables (Power Query outputs + model tables)
    all_target_tables = list(dict.fromkeys(
        pq_target_tables + model_tables + sql_target_tables
    ))

    # Extract key columns from relationships
    key_columns: list[str] = []
    for rel in relationships:
        key_columns.append(rel["from_column"])
        key_columns.append(rel["to_column"])

    # Extract KPI names from DAX measures
    kpi_metrics: list[str] = []
    for measure in measures:
        kpi_metrics.append(f"{measure.get('table', '')}[{measure.get('name', '')}]")

    # Build transformation logic
    transformation_logic = _build_transformation_logic(
        ds_transforms,
        pq_names,
        dax_definitions,
        relationships,
        visuals,
    )

    # Detect job type
    job_type = _detect_job_type_powerbi(pq_names, dax_definitions, relationships)

    # Build data source info for output
    ds_info = []
    for ds in data_sources:
        ds_info.append({
            "name": ds.get("name", ""),
            "type": ds.get("type", ""),
        })

    return ParsedPowerBI(
        source_tables=all_source_tables,
        target_tables=all_target_tables,
        key_columns=list(dict.fromkeys(key_columns)),
        kpi_metrics=kpi_metrics,
        job_type=job_type,
        transformation_logic=transformation_logic,
        data_sources=ds_info,
        power_query_transformations=pq_names,
        dax_measures=dax_definitions,
        relationships=relationships,
    )


# ---------------------------------------------------------------------------
# Reporting lineage extraction (separate from ETL lineage above)
# ---------------------------------------------------------------------------


def _extract_tables_from_sql(sql: str) -> list[str]:
    """Extract table names from FROM and JOIN clauses in a SQL string.

    Keeps qualified names (schema.table) as-is so the caller can decide
    whether to normalise them.

    Args:
        sql: A SQL query string.

    Returns:
        Deduplicated list of table references.
    """
    tables: list[str] = []
    # Match FROM/JOIN followed by an identifier (optionally schema-qualified)
    # Stop before subquery keywords, whitespace-only tokens, or parentheses.
    pattern = re.compile(
        r"\b(?:FROM|JOIN)\s+([`\"\[]?[\w]+(?:\.[\w]+)*[`\"\]]?)",
        re.IGNORECASE,
    )
    for match in pattern.finditer(sql):
        raw = match.group(1).strip("`\"[]")
        if raw.upper() not in {"SELECT", "WITH", "WHERE", "AS", "ON"}:
            tables.append(_strip_database_prefix(raw))
    return list(dict.fromkeys(tables))


def _extract_select_columns(sql: str) -> list[str]:
    """Extract column names / aliases from the SELECT clause of a SQL query.

    Handles:
      - Simple alias:  revenue_amount AS total_amount -> "total_amount"
      - Qualified ref: r.region -> "region"
      - No alias:      transaction_id -> "transaction_id"

    Args:
        sql: A SQL query string (single statement).

    Returns:
        Deduplicated list of column names or aliases.
    """
    # Find the SELECT ... FROM block (non-greedy so nested subqueries
    # don't consume the outer FROM).
    match = re.search(
        r"\bSELECT\b\s+(.*?)\s+\bFROM\b",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return []

    select_body = match.group(1)
    columns: list[str] = []

    _SQL_RESERVED = {
        "SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING",
        "ORDER", "DISTINCT", "AS", "ON", "AND", "OR", "NOT",
    }

    for item in select_body.split(","):
        item = item.strip()
        if not item:
            continue
        # Prefer AS alias (last word after AS)
        alias_match = re.search(r"\bAS\s+(\w+)\s*$", item, re.IGNORECASE)
        if alias_match:
            columns.append(alias_match.group(1))
            continue
        # Fall back to the last word (handles table.column or bare column)
        bare = re.search(r"(\w+)\s*$", item)
        if bare:
            col = bare.group(1)
            if col.upper() not in _SQL_RESERVED:
                columns.append(col)

    return list(dict.fromkeys(columns))


def extract_reporting_lineage(file_path: "str | Path") -> dict:
    """Extract reporting lineage from a PowerBI .pbit file.

    Reads the JSON structure of the .pbit file and extracts:
      - sql_name:  data source names (dataSources[i].name)
      - tables:    FROM/JOIN tables in embedded SQL + dataModel table names
      - columns:   SELECT clause aliases + DAX measure names
      - operation: SELECT (always), JOIN, AGGREGATE (detected from SQL)

    All returned lists are deduplicated.

    Args:
        file_path: Path to the .pbit file.

    Returns:
        Dict with keys: sql_name, tables, columns, operation.
        Values are lists of strings (may be empty).
    """
    file_path = Path(file_path)
    data = _parse_pbit_json(file_path)

    # --- sql_name: data source connection names ---
    sql_names: list[str] = []
    for ds in data.get("dataSources", []):
        name = ds.get("name", "")
        if name:
            sql_names.append(name)

    # --- Collect all embedded SQL query strings ---
    all_sql: list[str] = []
    for ds in data.get("dataSources", []):
        for query in ds.get("queries", []):
            q = query.get("query", "")
            if q:
                all_sql.append(q)

    # --- tables: FROM/JOIN tables across all SQL queries ---
    tables: list[str] = []
    for sql in all_sql:
        tables.extend(_extract_tables_from_sql(sql))

    # --- Also extract tables from dataModel.tables[].name ---
    # This handles real .pbit files where tables are in the data model
    for tbl in data.get("dataModel", {}).get("tables", []):
        name = tbl.get("name", "").strip()
        if name:
            tables.append(_strip_database_prefix(name))
        # Also check source field (e.g. "gold.retail_sales_summary")
        src = tbl.get("source", "").strip()
        if src:
            tables.append(_strip_database_prefix(src))
        # Check M-query partition expressions for table references
        for part in tbl.get("partitions", []):
            expr = part.get("source", {}).get("expression", [])
            if isinstance(expr, list):
                expr = "\n".join(expr)
            if isinstance(expr, str):
                # Extract table names from M expressions like:
                # Source{[Schema="gold",Item="retail_sales_summary"]}[Data]
                for m in re.finditer(r'Item\s*=\s*["\']([^"\']+)["\']', expr):
                    tables.append(m.group(1))
                # Also extract from Sql.Database calls
                for m in re.finditer(r'\[Schema\s*=\s*["\']([^"\']+)["\']\s*,\s*Item\s*=\s*["\']([^"\']+)["\']\]', expr):
                    tables.append(m.group(2))

    # --- Also check top-level "tables" key (our sample format) ---
    top_tables = data.get("tables", [])
    if isinstance(top_tables, list):
        for t in top_tables:
            if isinstance(t, str) and t.strip():
                tables.append(_strip_database_prefix(t.strip()))

    # --- Also check model.tables (alternative schema) ---
    for tbl in data.get("model", {}).get("tables", []):
        name = tbl.get("name", "").strip()
        if name:
            tables.append(_strip_database_prefix(name))

    # --- columns: SELECT aliases from SQL + DAX measure names ---
    columns: list[str] = []
    for sql in all_sql:
        columns.extend(_extract_select_columns(sql))
    # DAX measures from the semantic model
    for table in data.get("model", {}).get("tables", []):
        for measure in table.get("measures", []):
            measure_name = measure.get("name", "")
            if measure_name:
                columns.append(measure_name)
    # Also extract columns from dataModel tables
    for tbl in data.get("dataModel", {}).get("tables", []):
        for col in tbl.get("columns", []):
            col_name = col.get("name", "").strip()
            if col_name:
                columns.append(col_name)
    # Top-level columns key (our sample format)
    top_cols = data.get("columns", [])
    if isinstance(top_cols, list):
        columns.extend([c for c in top_cols if isinstance(c, str) and c.strip()])

    # --- operation: always SELECT; add JOIN / AGGREGATE if detected ---
    ops: set[str] = {"SELECT"}
    for sql in all_sql:
        if re.search(r"\bJOIN\b", sql, re.IGNORECASE):
            ops.add("JOIN")
        if re.search(r"\b(?:GROUP\s+BY|SUM|COUNT|AVG|MIN|MAX)\b", sql, re.IGNORECASE):
            ops.add("AGGREGATE")
    # Check top-level operation key (our sample format)
    top_ops = data.get("operation", [])
    if isinstance(top_ops, list):
        for op in top_ops:
            if isinstance(op, str):
                ops.add(op)

    return {
        "sql_name": list(dict.fromkeys(sql_names)),
        "tables":   list(dict.fromkeys(t for t in tables if t)),
        "columns":  list(dict.fromkeys(c for c in columns if c)),
        "operation": sorted(ops),
    }
