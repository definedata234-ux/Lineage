"""universal_format_detector.py
Universal file format detector and content extractor.

Every file type that arrives at the server — regardless of extension or even
lack of one — is identified here and its SQL/code content is extracted into
a normalised list of (content_string, dialect) tuples for the column parsers.

Supported format families
──────────────────────────
ETL / Pipeline code
  .sql .ddl .dml .hql .bql .tsql .pgsql .mysql .presto .trino   → SQL dialect
  .py .pyw .pyx                                                    → PySpark / Python
  .ipynb                                                           → Jupyter notebook
  .scala .sc                                                       → Spark Scala (SQL extracted)
  .java                                                            → Spark Java (SQL extracted)
  .r .R                                                            → R + SparkR / sparklyr
  .sh .bash .zsh                                                   → Shell (SQL extracted)
  .ksh .csh

BI / Reporting tools
  .pbit .pbix .rdl                                                 → Power BI / SSRS
  .lkml .lookml                                                    → Looker
  .qvs .qvd .qvw .qvf                                             → Qlik
  .twb .twbx .tds .tdsx                                            → Tableau
  .xml (Tableau, SSRS, ADF, etc.)                                  → XML-based tools

Data transformation frameworks
  .yaml .yml                                                       → dbt, Airflow, Prefect,
                                                                     Azure Data Factory,
                                                                     GitHub Actions
  .json                                                            → ADF pipelines, Glue jobs,
                                                                     Databricks JSON
  .toml .ini .cfg                                                  → config (metadata only)

Notebooks and mixed formats
  .ipynb                                                           → Jupyter (Python/SQL cells)
  .rmd .Rmd                                                        → R Markdown
  .qmd                                                             → Quarto

Stored procedures and DB objects
  .proc .sp .fnc .trg .vw .pkg .pkb .pks                          → PL/SQL / T-SQL objects
  .hql                                                             → HiveQL
  .ql                                                              → GraphQL / generic query

Configuration and manifest
  .tf .tfvars                                                      → Terraform (resource names)
  .properties .env                                                 → property files

Plain text / unknown
  .txt  and any unknown extension                                  → attempt SQL extraction

Content extraction returns:
    list[ExtractedContent]  where each item has:
        content : str         — the raw SQL/code string
        dialect : str         — "sql" | "python" | "json" | "yaml" | "xml" | "unknown"
        label   : str         — human-readable source label (e.g. "cell_3", "query_customers")
"""

import json
import re
import zipfile
import io
from pathlib import Path
from typing import TypedDict


class ExtractedContent(TypedDict):
    content: str
    dialect: str   # sql | python | yaml | json | xml | scala | r | unknown
    label:   str   # human label for this chunk


# ---------------------------------------------------------------------------
# Extension → (file_family, dialect)
# ---------------------------------------------------------------------------
_EXT_MAP: dict[str, tuple[str, str]] = {
    # ── SQL variants ──────────────────────────────────────────────────────
    ".sql":    ("sql",    "sql"),
    ".ddl":    ("sql",    "sql"),
    ".dml":    ("sql",    "sql"),
    ".hql":    ("sql",    "sql"),      # HiveQL
    ".bql":    ("sql",    "sql"),      # BigQuery legacy
    ".tsql":   ("sql",    "sql"),      # T-SQL
    ".pgsql":  ("sql",    "sql"),      # PostgreSQL
    ".mysql":  ("sql",    "sql"),
    ".presto": ("sql",    "sql"),
    ".trino":  ("sql",    "sql"),
    ".ql":     ("sql",    "sql"),
    ".proc":   ("sql",    "sql"),
    ".sp":     ("sql",    "sql"),
    ".fnc":    ("sql",    "sql"),
    ".trg":    ("sql",    "sql"),
    ".vw":     ("sql",    "sql"),
    ".pkg":    ("sql",    "sql"),
    ".pkb":    ("sql",    "sql"),
    ".pks":    ("sql",    "sql"),
    # ── Python ───────────────────────────────────────────────────────────
    ".py":     ("python", "python"),
    ".pyw":    ("python", "python"),
    ".pyx":    ("python", "python"),
    # ── Notebooks ────────────────────────────────────────────────────────
    ".ipynb":  ("notebook", "python"),
    ".rmd":    ("rnotebook", "r"),
    ".Rmd":    ("rnotebook", "r"),
    ".qmd":    ("qmd", "python"),      # Quarto (Python or R)
    # ── Scala / Java (Spark) ─────────────────────────────────────────────
    ".scala":  ("scala",  "scala"),
    ".sc":     ("scala",  "scala"),
    ".java":   ("java",   "java"),
    # ── R ────────────────────────────────────────────────────────────────
    ".r":      ("r",      "r"),
    ".R":      ("r",      "r"),
    # ── Shell ─────────────────────────────────────────────────────────────
    ".sh":     ("shell",  "shell"),
    ".bash":   ("shell",  "shell"),
    ".zsh":    ("shell",  "shell"),
    ".ksh":    ("shell",  "shell"),
    ".csh":    ("shell",  "shell"),
    # ── BI tools ─────────────────────────────────────────────────────────
    ".pbit":   ("powerbi", "json"),
    ".pbix":   ("powerbi_binary", "binary"),
    ".rdl":    ("ssrs",   "xml"),
    ".lkml":   ("looker", "lookml"),
    ".lookml": ("looker", "lookml"),
    ".qvs":    ("qlik",   "qlik"),
    ".qvd":    ("qlik",   "qlik"),
    ".qvw":    ("qlik",   "qlik"),
    ".qvf":    ("qlik",   "qlik"),
    ".twb":    ("tableau", "xml"),
    ".twbx":   ("tableau_zip", "binary"),
    ".tds":    ("tableau", "xml"),
    ".tdsx":   ("tableau_zip", "binary"),
    # ── Data frameworks / config ──────────────────────────────────────────
    ".yaml":   ("yaml",   "yaml"),
    ".yml":    ("yaml",   "yaml"),
    ".json":   ("json",   "json"),
    ".toml":   ("toml",   "toml"),
    ".ini":    ("ini",    "ini"),
    ".cfg":    ("ini",    "ini"),
    ".properties": ("properties", "ini"),
    ".env":    ("properties", "ini"),
    # ── Terraform ─────────────────────────────────────────────────────────
    ".tf":     ("terraform", "hcl"),
    ".tfvars": ("terraform", "hcl"),
    # ── XML ───────────────────────────────────────────────────────────────
    ".xml":    ("xml",    "xml"),
    # ── Plain text ────────────────────────────────────────────────────────
    ".txt":    ("text",   "unknown"),
    ".csv":    ("data",   "data"),
    ".tsv":    ("data",   "data"),
}


def detect_format(file_path: Path, content_bytes: bytes) -> tuple[str, str]:
    """Return (file_family, dialect) for a file.

    Falls back to content sniffing when the extension is missing or unknown.
    """
    ext = file_path.suffix.lower()
    if ext in _EXT_MAP:
        return _EXT_MAP[ext]

    # ── Content sniffing for unknown / no extension ───────────────────────
    try:
        sample = content_bytes[:512].decode("utf-8", errors="replace").strip()
    except Exception:
        return ("binary", "binary")

    if sample.startswith("{") or sample.startswith("["):
        return ("json", "json")
    if sample.startswith("<?xml") or sample.startswith("<"):
        return ("xml", "xml")
    if re.search(r"^(SELECT|INSERT|UPDATE|DELETE|CREATE|WITH|MERGE)\b", sample, re.IGNORECASE | re.MULTILINE):
        return ("sql", "sql")
    if "import pyspark" in sample or "from pyspark" in sample or "spark.table(" in sample:
        return ("python", "python")
    if sample.startswith("---") or re.match(r"^\w[\w_]*\s*:", sample):
        return ("yaml", "yaml")

    return ("text", "unknown")


# ---------------------------------------------------------------------------
# Per-family content extractors
# Returns list[ExtractedContent]
# ---------------------------------------------------------------------------

def _extract_sql(content: str, label: str) -> list[ExtractedContent]:
    """Split on semicolons, return one item per non-empty statement."""
    stmts = [s.strip() for s in content.split(";") if s.strip()]
    if not stmts:
        return [ExtractedContent(content=content, dialect="sql", label=label)]
    return [ExtractedContent(content=s, dialect="sql", label=f"{label}_stmt{i+1}") for i, s in enumerate(stmts)]


def _extract_python(content: str, label: str) -> list[ExtractedContent]:
    return [ExtractedContent(content=content, dialect="python", label=label)]


def _extract_notebook(content: str, label: str) -> list[ExtractedContent]:
    """Extract code cells from a .ipynb JSON notebook."""
    results: list[ExtractedContent] = []
    try:
        nb = json.loads(content)
    except json.JSONDecodeError:
        return [ExtractedContent(content=content, dialect="python", label=label)]

    for i, cell in enumerate(nb.get("cells", [])):
        cell_type = cell.get("cell_type", "")
        src = cell.get("source", [])
        code = "".join(src) if isinstance(src, list) else src
        if not code.strip():
            continue

        if cell_type == "code":
            # Detect SQL magic cells: %%sql or -- sql comment start
            if code.strip().startswith("%%sql") or code.strip().lower().startswith("-- sql"):
                sql_body = re.sub(r"^%%sql\s*", "", code.strip(), flags=re.IGNORECASE)
                results.extend(_extract_sql(sql_body, f"{label}_cell{i+1}"))
            else:
                results.append(ExtractedContent(content=code, dialect="python", label=f"{label}_cell{i+1}"))
        elif cell_type == "raw":
            # Raw cells sometimes contain SQL
            if re.search(r"\b(SELECT|INSERT|CREATE)\b", code, re.IGNORECASE):
                results.extend(_extract_sql(code, f"{label}_raw{i+1}"))

    return results or [ExtractedContent(content=content, dialect="python", label=label)]


def _extract_rmarkdown(content: str, label: str) -> list[ExtractedContent]:
    """Extract code chunks from R Markdown / Quarto (.rmd, .qmd)."""
    results: list[ExtractedContent] = []
    chunk_pat = re.compile(r"```\{(\w+).*?\}(.*?)```", re.DOTALL)
    for i, m in enumerate(chunk_pat.finditer(content)):
        lang = m.group(1).lower()
        code = m.group(2).strip()
        if not code:
            continue
        if lang in ("sql",):
            results.extend(_extract_sql(code, f"{label}_chunk{i+1}"))
        elif lang in ("r", "python"):
            results.append(ExtractedContent(content=code, dialect=lang, label=f"{label}_chunk{i+1}"))
    return results or [ExtractedContent(content=content, dialect="r", label=label)]


def _extract_scala_java(content: str, label: str, dialect: str) -> list[ExtractedContent]:
    """Extract SQL strings from Scala/Java Spark code."""
    results: list[ExtractedContent] = [
        ExtractedContent(content=content, dialect=dialect, label=label)
    ]
    # Find spark.sql("...") or spark.sql("""...""") literals
    for m in re.finditer(r'spark\.sql\s*\(\s*"""(.*?)"""\s*\)', content, re.DOTALL):
        results.extend(_extract_sql(m.group(1), f"{label}_sparksql"))
    for m in re.finditer(r'spark\.sql\s*\(\s*"((?:[^"\\]|\\.)*)"\s*\)', content):
        results.extend(_extract_sql(m.group(1), f"{label}_sparksql"))
    return results


def _extract_r(content: str, label: str) -> list[ExtractedContent]:
    """Extract SQL from R dbGetQuery / dbExecute / tbl calls."""
    results: list[ExtractedContent] = [
        ExtractedContent(content=content, dialect="r", label=label)
    ]
    for m in re.finditer(r'(?:dbGetQuery|dbExecute|sqlQuery)\s*\([^,]+,\s*["\']([^"\']+)["\']', content):
        results.extend(_extract_sql(m.group(1), f"{label}_rsql"))
    return results


def _extract_shell(content: str, label: str) -> list[ExtractedContent]:
    """Extract SQL from shell heredocs and bq/psql/mysql command arguments."""
    results: list[ExtractedContent] = []
    # Heredoc: bq query << 'EOF' ... EOF
    for m in re.finditer(r"<<\s*['\"]?(\w+)['\"]?\s*\n(.*?)\n\1", content, re.DOTALL):
        sql_candidate = m.group(2).strip()
        if re.search(r"\b(SELECT|INSERT|CREATE|MERGE)\b", sql_candidate, re.IGNORECASE):
            results.extend(_extract_sql(sql_candidate, f"{label}_heredoc"))
    # Inline: bq query --use_legacy_sql=false 'SELECT ...'
    for m in re.finditer(r"(?:bq\s+query|psql|mysql|sqlcmd)[^\n]*?['\"]([^'\"]{20,})['\"]", content):
        sql_candidate = m.group(1).strip()
        if re.search(r"\b(SELECT|INSERT|CREATE)\b", sql_candidate, re.IGNORECASE):
            results.extend(_extract_sql(sql_candidate, f"{label}_inline"))
    return results or [ExtractedContent(content=content, dialect="shell", label=label)]


def _extract_yaml(content: str, label: str) -> list[ExtractedContent]:
    """Extract SQL from dbt models, Airflow operators, ADF pipeline YAML."""
    results: list[ExtractedContent] = []

    # dbt: model SQL inline
    for m in re.finditer(r"sql\s*:\s*[|>]?\s*\n((?:[ \t]+.+\n)+)", content):
        sql = re.sub(r"^[ \t]+", "", m.group(1), flags=re.MULTILINE).strip()
        if sql:
            results.extend(_extract_sql(sql, f"{label}_dbt_sql"))

    # Airflow / generic: sql: "SELECT ..." or query: "SELECT ..."
    for m in re.finditer(r"(?:sql|query|statement)\s*:\s*[\"']([^\"']{10,})[\"']", content, re.IGNORECASE):
        results.extend(_extract_sql(m.group(1), f"{label}_yaml_sql"))

    # Airflow multi-line sql: |
    for m in re.finditer(r"(?:sql|query)\s*:\s*\|\n((?:[ \t]+.+\n)+)", content, re.IGNORECASE):
        sql = re.sub(r"^[ \t]+", "", m.group(1), flags=re.MULTILINE).strip()
        if sql:
            results.extend(_extract_sql(sql, f"{label}_yaml_block_sql"))

    # Table/dataset references in dbt sources.yml
    for m in re.finditer(r"identifier\s*:\s*[\"']?(\w[\w.]*)[\"']?", content):
        pass  # captured for metadata only; no column extraction possible

    return results or [ExtractedContent(content=content, dialect="yaml", label=label)]


def _extract_json(content: str, label: str) -> list[ExtractedContent]:
    """Extract SQL from JSON — ADF pipelines, Glue jobs, Databricks JSON."""
    results: list[ExtractedContent] = []
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        return [ExtractedContent(content=content, dialect="json", label=label)]

    def _walk(obj: object, path: str) -> None:
        if isinstance(obj, str):
            if re.search(r"\b(SELECT|INSERT|CREATE|MERGE|UPDATE|DELETE)\b", obj, re.IGNORECASE):
                results.extend(_extract_sql(obj, f"{label}_{path}"))
        elif isinstance(obj, dict):
            for k, v in obj.items():
                _walk(v, f"{path}.{k}")
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                _walk(item, f"{path}[{i}]")

    _walk(data, "root")
    return results or [ExtractedContent(content=content, dialect="json", label=label)]


def _extract_xml(content: str, label: str) -> list[ExtractedContent]:
    """Extract SQL from XML — Tableau .twb, SSRS .rdl, Azure Data Factory."""
    results: list[ExtractedContent] = []

    # SSRS / Tableau embedded SQL in CommandText, CustomSQL, Query tags
    for tag in ("CommandText", "CustomSQL", "Query", "SelectCommand", "SqlQuery",
                "command", "sql", "Statement"):
        pat = re.compile(rf"<{tag}[^>]*>(.*?)</{tag}>", re.DOTALL | re.IGNORECASE)
        for m in pat.finditer(content):
            sql_candidate = m.group(1).strip()
            # Unescape XML entities
            sql_candidate = (sql_candidate
                             .replace("&amp;", "&").replace("&lt;", "<")
                             .replace("&gt;", ">").replace("&quot;", '"')
                             .replace("&#10;", "\n").replace("&#13;", "\r"))
            if re.search(r"\b(SELECT|INSERT|CREATE)\b", sql_candidate, re.IGNORECASE):
                results.extend(_extract_sql(sql_candidate, f"{label}_{tag.lower()}"))

    # Tableau .twb: <relation type='text'> ... </relation>
    for m in re.finditer(r"<relation[^>]+type=['\"]text['\"][^>]*>(.*?)</relation>",
                         content, re.DOTALL | re.IGNORECASE):
        sql_candidate = m.group(1).strip()
        if re.search(r"\b(SELECT|FROM)\b", sql_candidate, re.IGNORECASE):
            results.extend(_extract_sql(sql_candidate, f"{label}_tableau_relation"))

    return results or [ExtractedContent(content=content, dialect="xml", label=label)]


def _extract_tableau_zip(content_bytes: bytes, label: str) -> list[ExtractedContent]:
    """Unzip .twbx / .tdsx and extract from the embedded .twb / .tds XML."""
    results: list[ExtractedContent] = []
    try:
        with zipfile.ZipFile(io.BytesIO(content_bytes)) as zf:
            for name in zf.namelist():
                if name.endswith((".twb", ".tds")):
                    xml_content = zf.read(name).decode("utf-8", errors="replace")
                    results.extend(_extract_xml(xml_content, f"{label}/{name}"))
    except zipfile.BadZipFile:
        pass
    return results


def _extract_powerbi_binary(content_bytes: bytes, label: str) -> list[ExtractedContent]:
    """Try to extract SQL from .pbix (ZIP-based Power BI file)."""
    results: list[ExtractedContent] = []
    try:
        with zipfile.ZipFile(io.BytesIO(content_bytes)) as zf:
            for name in zf.namelist():
                if "DataModel" in name or name.endswith(".json"):
                    try:
                        raw = zf.read(name).decode("utf-8", errors="replace")
                        results.extend(_extract_json(raw, f"{label}/{name}"))
                    except Exception:
                        pass
    except zipfile.BadZipFile:
        pass
    return results


def _extract_terraform(content: str, label: str) -> list[ExtractedContent]:
    """Extract SQL from Terraform HCL — BigQuery scheduled queries, Glue jobs."""
    results: list[ExtractedContent] = []
    for m in re.finditer(r'query\s*=\s*<<-?(\w+)\s*\n(.*?)\n\s*\1', content, re.DOTALL):
        sql_candidate = m.group(2).strip()
        if re.search(r"\b(SELECT|INSERT|CREATE)\b", sql_candidate, re.IGNORECASE):
            results.extend(_extract_sql(sql_candidate, f"{label}_hcl_sql"))
    for m in re.finditer(r'(?:query|sql_query)\s*=\s*"([^"]{10,})"', content):
        results.extend(_extract_sql(m.group(1), f"{label}_hcl_inline"))
    return results or [ExtractedContent(content=content, dialect="hcl", label=label)]


def _extract_text_unknown(content: str, label: str) -> list[ExtractedContent]:
    """For .txt or truly unknown files — try SQL, then return as-is."""
    if re.search(r"\b(SELECT|INSERT|CREATE|UPDATE|DELETE|MERGE|WITH)\b", content, re.IGNORECASE):
        return _extract_sql(content, label)
    return [ExtractedContent(content=content, dialect="unknown", label=label)]


def _extract_lookml(content: str, label: str) -> list[ExtractedContent]:
    """Return LookML as-is; SQL inside derived_table blocks also extracted."""
    results: list[ExtractedContent] = [
        ExtractedContent(content=content, dialect="lookml", label=label)
    ]
    # Also extract inline SQL from derived_table blocks
    for m in re.finditer(r"sql\s*:\s*(.*?)\s*;;", content, re.DOTALL | re.IGNORECASE):
        sql_candidate = m.group(1).strip()
        if re.search(r"\b(SELECT|FROM)\b", sql_candidate, re.IGNORECASE):
            results.extend(_extract_sql(sql_candidate, f"{label}_derived_sql"))
    return results


def _extract_qlik(content: str, label: str) -> list[ExtractedContent]:
    """Return Qlik script as-is; embedded SQL SELECT blocks also extracted."""
    results: list[ExtractedContent] = [
        ExtractedContent(content=content, dialect="qlik", label=label)
    ]
    # Extract SQL blocks: SQL SELECT ... ;
    for m in re.finditer(r"\bSQL\s+(SELECT\b.*?);", content, re.DOTALL | re.IGNORECASE):
        results.extend(_extract_sql(m.group(1), f"{label}_qlik_sql"))
    return results


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_all_content(
    file_path: Path,
    content_bytes: bytes,
) -> list[ExtractedContent]:
    """Detect the file format and extract all SQL/code content from any file.

    This is the single entry point called by the server for every uploaded file,
    regardless of extension.

    Args:
        file_path:     Path object (used for extension detection and label).
        content_bytes: Raw file bytes.

    Returns:
        List of ExtractedContent items, each with content, dialect, and label.
        Never raises — returns an empty list on unrecoverable errors.
    """
    label = file_path.stem
    family, dialect = detect_format(file_path, content_bytes)

    # Decode text content (most formats)
    if dialect != "binary":
        try:
            content = content_bytes.decode("utf-8", errors="replace")
        except Exception:
            content = ""
    else:
        content = ""

    try:
        if family == "sql":
            return _extract_sql(content, label)
        elif family == "python":
            return _extract_python(content, label)
        elif family == "notebook":
            return _extract_notebook(content, label)
        elif family in ("rnotebook", "qmd"):
            return _extract_rmarkdown(content, label)
        elif family == "scala":
            return _extract_scala_java(content, label, "scala")
        elif family == "java":
            return _extract_scala_java(content, label, "java")
        elif family == "r":
            return _extract_r(content, label)
        elif family == "shell":
            return _extract_shell(content, label)
        elif family in ("powerbi",):
            return _extract_json(content, label)   # .pbit is JSON
        elif family == "powerbi_binary":
            return _extract_powerbi_binary(content_bytes, label)
        elif family in ("ssrs", "xml"):
            return _extract_xml(content, label)
        elif family in ("looker",):
            return _extract_lookml(content, label)
        elif family in ("qlik",):
            return _extract_qlik(content, label)
        elif family in ("tableau",):
            return _extract_xml(content, label)
        elif family == "tableau_zip":
            return _extract_tableau_zip(content_bytes, label)
        elif family == "yaml":
            return _extract_yaml(content, label)
        elif family == "json":
            return _extract_json(content, label)
        elif family == "terraform":
            return _extract_terraform(content, label)
        elif family == "text":
            return _extract_text_unknown(content, label)
        else:
            # CSV, TSV, binary, truly unknown — return as-is for metadata only
            return [ExtractedContent(content=content[:2000], dialect=dialect, label=label)]
    except Exception as exc:
        print(f"[format_detector] error on {file_path.name}: {exc}", flush=True)
        return []


def get_file_type_label(file_path: Path, content_bytes: bytes) -> str:
    """Return a human-readable file type label for the output schema.

    Used as the `file_type` column value in ColumnLineageRecord.
    """
    family, dialect = detect_format(file_path, content_bytes)
    _LABELS: dict[str, str] = {
        "sql":            "SQL",
        "python":         "PySpark",
        "notebook":       "PySpark",
        "rnotebook":      "R",
        "qmd":            "Quarto",
        "scala":          "Spark Scala",
        "java":           "Spark Java",
        "r":              "R",
        "shell":          "Shell",
        "powerbi":        "PowerBI",
        "powerbi_binary": "PowerBI",
        "ssrs":           "SSRS",
        "xml":            "XML",
        "looker":         "Looker",
        "qlik":           "Qlik",
        "tableau":        "Tableau",
        "tableau_zip":    "Tableau",
        "yaml":           "YAML/dbt",
        "json":           "JSON",
        "terraform":      "Terraform",
        "text":           "Text",
        "data":           "Data",
        "ini":            "Config",
        "toml":           "Config",
        "hcl":            "Terraform",
        "binary":         "Binary",
        "unknown":        "Unknown",
    }
    return _LABELS.get(family, family.upper())
