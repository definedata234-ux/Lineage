"""Main orchestrator for the data lineage extraction tool.

This module ties all parsers together into a complete pipeline:
  1. Loads YAML config (pipeline directory, file patterns, exclusions, output path).
  2. Scans directories for pipeline files matching the configured patterns.
  3. Delegates to the appropriate parser based on file extension.
  4. Splits SQL content on semicolons so each statement gets its own row.
  5. Writes a CSV report with one row per SQL statement (or PySpark operation).
  6. Prints a human-readable summary to the console.

Design decisions:
  - Semicolon-based splitting: SQL files and embedded SQL strings are split on
    ';' so that each independent statement produces its own LineageRecord with
    its own source/target tables. This gives granular, per-statement lineage.
  - AST-based SQL extraction from .py files: we use Python's ast module to
    reliably find spark.sql("...") string literals, then run those through
    the SQL parser. This avoids fragile regex-based string extraction.
  - For .py files, PySpark DataFrame operations (spark.table, saveAsTable, etc.)
    produce their own row if they reference any tables. Each embedded SQL
    statement (split by ';') also gets its own row.
  - File-level errors (syntax errors, JSON parse failures) are caught and
    logged as warnings. The orchestrator continues processing other files.
    The final exit code is 1 if any file had a processing failure.
  - Relative file paths in the CSV are computed from pipeline_dir, not the
    current working directory, so the output is deterministic regardless of
    where the tool is invoked from.
"""

import ast
import csv
import re
import sys
from fnmatch import fnmatch
from pathlib import Path
from typing import Optional

import yaml

# Import the modules that this orchestrator ties together.
# Each parser handles a specific file type or extraction concern.
from lineage_extraction.parsers.metadata_extractor import extract_metadata
from lineage_extraction.models.lineage_record import LineageRecord
from lineage_extraction.parsers.pyspark_parser import parse_pyspark, parse_notebook, parse_pyspark_per_write
from lineage_extraction.parsers.sql_parser import parse_sql
from lineage_extraction.parsers.powerbi_parser import parse_powerbi
from lineage_extraction.parsers.looker_parser import parse_looker
from lineage_extraction.parsers.qlik_parser import parse_qlik


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _load_config(config_path: str) -> dict:
    """Read and parse a YAML config file.

    Uses yaml.safe_load to prevent arbitrary code execution from untrusted
    config files. This is the standard safe way to read YAML in Python.

    Args:
        config_path: Path to the YAML configuration file.

    Returns:
        A dict with keys: pipeline_dir, file_patterns, exclusions, output_path.

    Raises:
        FileNotFoundError: If the config file does not exist.
        yaml.YAMLError: If the config file contains invalid YAML.
    """
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _scan_files(
    pipeline_dir: Path,
    patterns: list[str],
    exclusions: list[str],
) -> list[Path]:
    """Scan pipeline_dir for files matching the given patterns.

    Uses pathlib.Path.rglob() for recursive directory scanning, then filters
    results using fnmatch for both inclusion (file_patterns) and exclusion
    (exclusions). Files are sorted for deterministic output ordering.

    Args:
        pipeline_dir: The root directory to scan recursively.
        patterns: Glob patterns for files to include (e.g., ["*.py", "*.sql"]).
        exclusions: Path patterns to exclude (e.g., ["__pycache__", "tests/"]).

    Returns:
        A sorted list of Path objects for matching, non-excluded files.
    """
    matched_files: list[Path] = []

    # rglob("*") finds all files recursively. We then check each one.
    for file_path in pipeline_dir.rglob("*"):
        # Skip directories (only process files)
        if not file_path.is_file():
            continue

        # Check if the file matches any of the inclusion patterns.
        # We match against the filename (not the full path).
        filename = file_path.name
        if not any(fnmatch(filename, pat) for pat in patterns):
            continue

        # Check if any part of the relative path matches an exclusion pattern.
        # We convert the file's relative path to forward-slash format for
        # consistent pattern matching across platforms.
        rel_path = file_path.relative_to(pipeline_dir)
        rel_str = str(rel_path).replace("\\", "/")

        # A file is excluded if any exclusion pattern appears anywhere in
        # its relative path. For example, "tests/" excludes anything under
        # a "tests" directory, and "__pycache__" excludes cached files.
        excluded = False
        for excl in exclusions:
            # Strip trailing slash for directory-style patterns
            excl_clean = excl.rstrip("/")
            # Check if any path component or the full path matches
            parts = rel_str.split("/")
            if excl_clean in parts:
                excluded = True
                break

        if not excluded:
            matched_files.append(file_path)

    # Sort for deterministic ordering (important for reproducible CSV output)
    return sorted(matched_files)


def _split_sql_statements(content: str) -> list[str]:
    """Split SQL content into individual statements on semicolons.

    Each semicolon-terminated block becomes its own statement.
    Empty/whitespace-only blocks (e.g., from trailing semicolons or
    blank lines between statements) are filtered out.

    Why split here instead of inside parse_sql()?
      - parse_sql() treats its input as a SINGLE logical SQL statement.
      - The orchestrator is the right place to decide granularity — it
        controls how many LineageRecords are created per file.
      - Keeping parse_sql() simple means it doesn't need to know about
        multi-statement file content.

    Args:
        content: Raw SQL text potentially containing multiple statements
                 separated by semicolons.

    Returns:
        A list of non-empty SQL statement strings (without trailing semicolons).
    """
    statements = [stmt.strip() for stmt in content.split(";")]
    return [stmt for stmt in statements if stmt]


def _extract_sql_strings_from_python(content: str) -> list[str]:
    """Extract string literals from spark.sql() calls in Python source code.

    Uses Python's ast module to reliably parse the source and find calls
    matching the pattern spark.sql("..."). This is more robust than regex
    because it handles multi-line strings, escaped quotes, and comments
    correctly.

    Why AST instead of regex?
      - Regex cannot reliably distinguish spark.sql() from other .sql()
        calls (e.g., cursor.sql()).
      - AST handles multi-line strings, raw strings, and escapes correctly.
      - AST ignores spark.sql() calls inside comments or string literals.

    Args:
        content: Python source code as a string.

    Returns:
        A list of SQL string literals found in spark.sql() calls.
        Returns an empty list if the code has syntax errors.
    """
    # Guard against syntax errors -- the file might be broken.
    # We don't want to crash the orchestrator for one bad file.
    try:
        tree = ast.parse(content)
    except SyntaxError:
        return []

    sql_strings: list[str] = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        # Check if this is spark.sql(...) -- the func must be an Attribute
        # node where the value is a Name with id "spark" and attr is "sql".
        func = node.func
        if not isinstance(func, ast.Attribute):
            continue
        if func.attr != "sql":
            continue
        if not (isinstance(func.value, ast.Name) and func.value.id == "spark"):
            continue

        # Extract the first argument if it's a string constant.
        # We only support literal strings -- variable references and
        # expressions are not extracted.
        if node.args and isinstance(node.args[0], ast.Constant):
            if isinstance(node.args[0].value, str):
                sql_strings.append(node.args[0].value)

    return sql_strings


def _process_file(
    file_path: Path,
    pipeline_dir: Path,
) -> tuple[list[LineageRecord], list[str], Optional[str]]:
    """Process a single pipeline file and extract lineage records.

    This is the main file-level processor. It routes to the correct parser
    based on file extension, merges results from multiple parsers (for .py
    files that contain both PySpark code and embedded SQL), and combines
    everything with metadata extraction.

    For .py files specifically:
      1. Run parse_pyspark() on the full file content.
      2. Extract SQL strings from spark.sql() calls via AST.
      3. Run parse_sql() on each extracted SQL string.
      4. Merge: combine sources/targets/keys from both parsers, deduplicate.
      5. Run extract_metadata() for domain, subject_area, schedule, owner.
      6. Build the transformation_logic by joining PySpark logic with SQL.

    For .sql files:
      1. Run parse_sql() on the full content.
      2. Run extract_metadata() for SQL-style comment metadata.

    For .ipynb files:
      1. Run parse_notebook() on the JSON content.
      2. Run extract_metadata() for notebook code cell metadata.

    Args:
        file_path: Absolute path to the pipeline file.
        pipeline_dir: Root pipeline directory (for computing relative paths).

    Returns:
        A tuple of (records, warnings, error):
          - records: list of LineageRecord objects (one per semicolon-separated
            SQL statement, or one per PySpark operation + one per embedded SQL
            statement for .py files).
          - warnings: list of warning message strings.
          - error: An error message string if processing failed, else None.
    """
    warnings: list[str] = []
    records: list[LineageRecord] = []

    # Compute the relative path from the pipeline root directory.
    # This is stored in the CSV so consumers can locate the file.
    try:
        rel_path = file_path.relative_to(pipeline_dir)
    except ValueError:
        # If file_path is not under pipeline_dir, use the full filename.
        rel_path = Path(file_path.name)

    # Convert to forward-slash format for consistent CSV output across OSes.
    file_path_str = str(rel_path).replace("\\", "/")

    # job_name is the filename without extension (e.g., "customer_etl" from
    # "customer_etl.py"). This is the human-readable pipeline identifier.
    job_name = file_path.stem

    # Read the file content. If reading fails, return an error.
    try:
        content = file_path.read_text(encoding="utf-8")
    except OSError as exc:
        return ([], [], f"Failed to read {file_path}: {exc}")

    suffix = file_path.suffix.lower()

    # -----------------------------------------------------------------------
    # .SQL files: SQL-only processing path
    # Each semicolon-separated statement produces its own LineageRecord
    # so that every SQL block gets its own row with its own source/target
    # tables instead of merging everything into one record.
    # -----------------------------------------------------------------------
    if suffix == ".sql":
        # Metadata (DOMAIN, SUBJECT_AREA, etc.) is file-level, extracted once
        # and shared across all statement records from this file.
        meta = extract_metadata(file_path, content)

        # Split the full file content into individual statements on ';'.
        statements = _split_sql_statements(content)

        if not statements:
            warnings.append(f"{file_path}: no SQL statements found")
            return (records, warnings, None)

        for i, stmt in enumerate(statements):
            try:
                parsed = parse_sql(stmt)
            except Exception as exc:
                warnings.append(
                    f"{file_path} statement {i + 1}: parse error: {exc}"
                )
                continue

            # Per-statement warnings (not file-level, so the operator knows
            # which specific statement is missing lineage).
            if not parsed["target_tables"]:
                warnings.append(
                    f"{file_path} statement {i + 1}: no target table found"
                )
            if not parsed["source_tables"]:
                warnings.append(
                    f"{file_path} statement {i + 1}: no source tables found"
                )

            record = LineageRecord(
                domain=meta["domain"],
                subject_area=meta["subject_area"],
                job_name=job_name,
                file_path=file_path_str,
                source_tables=parsed["source_tables"],
                target_tables=parsed["target_tables"],
                key_columns=parsed["key_columns"],
                transformation_logic=parsed["transformation_logic"],
                kpi_metrics=parsed["kpi_metrics"],
                job_type=parsed["job_type"],
                schedule=meta["schedule"],
                owner=meta["owner"],
            )
            records.append(record)

        # File-level metadata warnings (apply to the whole file, not per statement)
        if meta["domain"] == "unknown":
            warnings.append(f"{file_path}: missing DOMAIN")
        if meta["subject_area"] == "unknown":
            warnings.append(f"{file_path}: missing SUBJECT_AREA")

    # -----------------------------------------------------------------------
    # .PY files: PySpark + embedded SQL processing path
    # Each write operation (saveAsTable, insertInto) gets its own row so
    # curated and aggregation flows are clearly separated. Each embedded
    # SQL statement (split by ';') also gets its own row.
    # -----------------------------------------------------------------------
    elif suffix == ".py":
        # Before running parsers, check for syntax errors explicitly.
        try:
            ast.parse(content)
        except SyntaxError as exc:
            return ([], [], f"Syntax error in {file_path}: {exc}")

        # Extract metadata once — it's file-level, shared across all records.
        meta = extract_metadata(file_path, content)

        # Step 1: Run per-write PySpark parser.
        # This walks statements in order and emits one result per write
        # target (saveAsTable/insertInto), with sources/transforms accumulated
        # up to that point. spark.sql() calls are NOT included here — they
        # are handled separately in Step 2.
        try:
            pyspark_results = parse_pyspark_per_write(content)
        except Exception as exc:
            return ([], [], f"PySpark parser error on {file_path}: {exc}")

        for result in pyspark_results:
            record = LineageRecord(
                domain=meta["domain"],
                subject_area=meta["subject_area"],
                job_name=job_name,
                file_path=file_path_str,
                source_tables=result["source_tables"],
                target_tables=result["target_tables"],
                key_columns=result["key_columns"],
                transformation_logic=result["transformation_logic"],
                kpi_metrics=result["kpi_metrics"],
                job_type=result["job_type"],
                schedule=meta["schedule"],
                owner=meta["owner"],
            )
            records.append(record)

        # Step 2: Extract SQL strings from spark.sql("...") calls via AST,
        # split each on ';', and create one record per statement.
        sql_strings = _extract_sql_strings_from_python(content)
        for sql_text in sql_strings:
            statements = _split_sql_statements(sql_text)
            for stmt in statements:
                try:
                    parsed = parse_sql(stmt)
                except Exception:
                    continue

                if not parsed["source_tables"] and not parsed["target_tables"]:
                    continue

                record = LineageRecord(
                    domain=meta["domain"],
                    subject_area=meta["subject_area"],
                    job_name=job_name,
                    file_path=file_path_str,
                    source_tables=parsed["source_tables"],
                    target_tables=parsed["target_tables"],
                    key_columns=parsed["key_columns"],
                    transformation_logic=parsed["transformation_logic"],
                    kpi_metrics=parsed["kpi_metrics"],
                    job_type=parsed["job_type"],
                    schedule=meta["schedule"],
                    owner=meta["owner"],
                )
                records.append(record)

        # Check for missing target/source across ALL records from this file.
        if not any(r.source_tables for r in records):
            warnings.append(f"{file_path}: no source tables found")
        if not any(r.target_tables for r in records):
            warnings.append(f"{file_path}: no target table found")

        # File-level metadata warnings
        if meta["domain"] == "unknown":
            warnings.append(f"{file_path}: missing DOMAIN")
        if meta["subject_area"] == "unknown":
            warnings.append(f"{file_path}: missing SUBJECT_AREA")

    # -----------------------------------------------------------------------
    # .IPYNB files: Jupyter notebook processing path
    # -----------------------------------------------------------------------
    elif suffix == ".ipynb":
        try:
            parsed = parse_notebook(content)
        except Exception as exc:
            return ([], [], f"Notebook parser error on {file_path}: {exc}")

        # Extract metadata from notebook code cells
        meta = extract_metadata(file_path, content)

        # Check for missing target/source and log warnings
        if not parsed["target_tables"]:
            warnings.append(f"{file_path}: no target table found")
        if not parsed["source_tables"]:
            warnings.append(f"{file_path}: no source tables found")

        # Check for missing required metadata
        if meta["domain"] == "unknown":
            warnings.append(f"{file_path}: missing DOMAIN")
        if meta["subject_area"] == "unknown":
            warnings.append(f"{file_path}: missing SUBJECT_AREA")

        record = LineageRecord(
            domain=meta["domain"],
            subject_area=meta["subject_area"],
            job_name=job_name,
            file_path=file_path_str,
            source_tables=parsed["source_tables"],
            target_tables=parsed["target_tables"],
            key_columns=parsed["key_columns"],
            transformation_logic=parsed["transformation_logic"],
            kpi_metrics=parsed["kpi_metrics"],
            job_type=parsed["job_type"],
            schedule=meta["schedule"],
            owner=meta["owner"],
        )
        records.append(record)

    # -----------------------------------------------------------------------
    # .PBIT files: Power BI template processing path
    # Extracts lineage from Power BI data sources, transformations, and models.
    # -----------------------------------------------------------------------
    elif suffix == ".pbit":
        try:
            parsed = parse_powerbi(file_path)
        except Exception as exc:
            return ([], [], f"Power BI parser error on {file_path}: {exc}")

        # Extract metadata from PBIT file structure (includes DOMAIN, SUBJECT_AREA, etc.)
        try:
            import json
            pbit_content = json.loads(content)
            pbit_metadata = pbit_content.get("metadata", {})
            meta = {
                "domain": pbit_metadata.get("DOMAIN", "unknown"),
                "subject_area": pbit_metadata.get("SUBJECT_AREA", "unknown"),
                "schedule": pbit_metadata.get("SCHEDULE"),
                "owner": pbit_metadata.get("OWNER"),
            }
        except (json.JSONDecodeError, KeyError):
            # Fallback to metadata extractor if JSON parsing fails
            meta = extract_metadata(file_path, content)

        # Check for missing target/source and log warnings
        if not parsed["target_tables"]:
            warnings.append(f"{file_path}: no target table found")
        if not parsed["source_tables"]:
            warnings.append(f"{file_path}: no source tables found")

        # Check for missing required metadata
        if meta["domain"] == "unknown":
            warnings.append(f"{file_path}: missing DOMAIN")
        if meta["subject_area"] == "unknown":
            warnings.append(f"{file_path}: missing SUBJECT_AREA")

        # Create records for each major Power BI component
        # 1. Data sources and SQL queries
        for i, ds_info in enumerate(parsed.get("data_sources", [])):
            ds_name = ds_info.get("name", f"datasource_{i}")
            record = LineageRecord(
                domain=meta["domain"],
                subject_area=meta["subject_area"],
                job_name=f"{job_name}_{ds_name}",
                file_path=file_path_str,
                source_tables=parsed["source_tables"],
                target_tables=parsed["target_tables"],
                key_columns=parsed["key_columns"],
                transformation_logic=parsed["transformation_logic"],
                kpi_metrics=parsed["kpi_metrics"],
                job_type=parsed["job_type"],
                schedule=meta["schedule"],
                owner=meta["owner"],
            )
            records.append(record)

        # 2. Power Query transformations (if any)
        for transform_name in parsed.get("power_query_transformations", []):
            record = LineageRecord(
                domain=meta["domain"],
                subject_area=meta["subject_area"],
                job_name=f"{job_name}_{transform_name}",
                file_path=file_path_str,
                source_tables=parsed["source_tables"],
                target_tables=parsed["target_tables"],
                key_columns=parsed["key_columns"],
                transformation_logic=parsed["transformation_logic"],
                kpi_metrics=parsed["kpi_metrics"],
                job_type=parsed["job_type"],
                schedule=meta["schedule"],
                owner=meta["owner"],
            )
            records.append(record)

        # 3. Data model relationships (if any exist)
        if parsed["relationships"]:
            rel_record = LineageRecord(
                domain=meta["domain"],
                subject_area=meta["subject_area"],
                job_name=f"{job_name}_relationships",
                file_path=file_path_str,
                source_tables=parsed["source_tables"],
                target_tables=parsed["target_tables"],
                key_columns=parsed["key_columns"],
                transformation_logic=f"Data model with {len(parsed['relationships'])} relationships",
                kpi_metrics=parsed["kpi_metrics"],
                job_type=parsed["job_type"],
                schedule=meta["schedule"],
                owner=meta["owner"],
            )
            records.append(rel_record)

        # Ensure at least one record is created
        if not records:
            record = LineageRecord(
                domain=meta["domain"],
                subject_area=meta["subject_area"],
                job_name=job_name,
                file_path=file_path_str,
                source_tables=parsed["source_tables"],
                target_tables=parsed["target_tables"],
                key_columns=parsed["key_columns"],
                transformation_logic=parsed["transformation_logic"],
                kpi_metrics=parsed["kpi_metrics"],
                job_type=parsed["job_type"],
                schedule=meta["schedule"],
                owner=meta["owner"],
            )
            records.append(record)

    # -----------------------------------------------------------------------
    # .LKML files: Looker LookML processing path
    # Extracts lineage from views, explores, joins, measures, and dashboards.
    # -----------------------------------------------------------------------
    elif suffix == ".lkml":
        try:
            parsed = parse_looker(file_path)
        except Exception as exc:
            return ([], [], f"Looker parser error on {file_path}: {exc}")

        # Extract metadata from LookML file
        meta = extract_metadata(file_path, content)

        # Check for missing target/source and log warnings
        if not parsed["target_tables"]:
            warnings.append(f"{file_path}: no target table found")
        if not parsed["source_tables"]:
            warnings.append(f"{file_path}: no source tables found")

        # Check for missing required metadata
        if meta["domain"] == "unknown":
            warnings.append(f"{file_path}: missing DOMAIN")
        if meta["subject_area"] == "unknown":
            warnings.append(f"{file_path}: missing SUBJECT_AREA")

        # Create records for each LookML object type
        # 1. Views (each view becomes a record)
        for view in parsed.get("views", []):
            view_name = view.get("name", "unknown_view")

            record = LineageRecord(
                domain=meta["domain"],
                subject_area=meta["subject_area"],
                job_name=f"{job_name}_{view_name}",
                file_path=file_path_str,
                source_tables=parsed["source_tables"],
                target_tables=[view_name],
                key_columns=parsed["key_columns"],
                transformation_logic=parsed["transformation_logic"],
                kpi_metrics=parsed["kpi_metrics"],
                job_type=parsed["job_type"],
                schedule=meta["schedule"],
                owner=meta["owner"],
            )
            records.append(record)

        # 2. Explores (each explore becomes a record)
        for explore in parsed.get("explores", []):
            explore_name = explore.get("name", "unknown_explore")

            record = LineageRecord(
                domain=meta["domain"],
                subject_area=meta["subject_area"],
                job_name=f"{job_name}_{explore_name}",
                file_path=file_path_str,
                source_tables=parsed["source_tables"],
                target_tables=[explore_name],
                key_columns=parsed["key_columns"],
                transformation_logic=parsed["transformation_logic"],
                kpi_metrics=parsed["kpi_metrics"],
                job_type=parsed["job_type"],
                schedule=meta["schedule"],
                owner=meta["owner"],
            )
            records.append(record)

        # 3. Dashboards (each dashboard becomes a record)
        for dashboard in parsed.get("dashboards", []):
            dash_name = dashboard.get("name", dashboard.get("title", "unknown_dashboard"))

            record = LineageRecord(
                domain=meta["domain"],
                subject_area=meta["subject_area"],
                job_name=f"{job_name}_{dash_name}",
                file_path=file_path_str,
                source_tables=parsed["source_tables"],
                target_tables=[dash_name],
                key_columns=parsed["key_columns"],
                transformation_logic=parsed["transformation_logic"],
                kpi_metrics=parsed["kpi_metrics"],
                job_type=parsed["job_type"],
                schedule=meta["schedule"],
                owner=meta["owner"],
            )
            records.append(record)

        # Ensure at least one record is created (for files with no views/explores/dashboards)
        if not records:
            record = LineageRecord(
                domain=meta["domain"],
                subject_area=meta["subject_area"],
                job_name=job_name,
                file_path=file_path_str,
                source_tables=parsed["source_tables"],
                target_tables=parsed["target_tables"],
                key_columns=parsed["key_columns"],
                transformation_logic=parsed["transformation_logic"],
                kpi_metrics=parsed["kpi_metrics"],
                job_type=parsed["job_type"],
                schedule=meta["schedule"],
                owner=meta["owner"],
            )
            records.append(record)

    # -----------------------------------------------------------------------
    # .QVS files: Qlik Sense script processing path
    # Extracts lineage from LOAD/FROM statements, embedded SQL blocks,
    # STORE targets, JOIN conditions, and aggregation KPIs.
    # -----------------------------------------------------------------------
    elif suffix == ".qvs":
        try:
            parsed = parse_qlik(file_path)
        except Exception as exc:
            return ([], [], f"Qlik parser error on {file_path}: {exc}")

        meta = extract_metadata(file_path, content)

        if not parsed["target_tables"]:
            warnings.append(f"{file_path}: no target table found")
        if not parsed["source_tables"]:
            warnings.append(f"{file_path}: no source tables found")
        if meta["domain"] == "unknown":
            warnings.append(f"{file_path}: missing DOMAIN")
        if meta["subject_area"] == "unknown":
            warnings.append(f"{file_path}: missing SUBJECT_AREA")

        record = LineageRecord(
            domain=meta["domain"],
            subject_area=meta["subject_area"],
            job_name=job_name,
            file_path=file_path_str,
            source_tables=parsed["source_tables"],
            target_tables=parsed["target_tables"],
            key_columns=parsed["key_columns"],
            transformation_logic=parsed["transformation_logic"],
            kpi_metrics=parsed["kpi_metrics"],
            job_type=parsed["job_type"],
            schedule=meta["schedule"],
            owner=meta["owner"],
        )
        records.append(record)

    else:
        # Unknown file type: skip with a warning.
        warnings.append(f"{file_path}: unsupported file type '{suffix}'")

    return (records, warnings, None)


def _write_csv(records: list[LineageRecord], output_path: str) -> None:
    """Write lineage records to a CSV file.

    Uses the canonical column order from LineageRecord.csv_columns().
    The output directory is created automatically if it doesn't exist.

    Args:
        records: List of LineageRecord objects to serialize.
        output_path: Path where the CSV file will be written.
    """
    # Ensure the output directory exists before writing.
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    columns = LineageRecord.csv_columns()

    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for record in records:
            writer.writerow(record.to_csv_row())


def _versioned_output_path(base_path: str) -> str:
    """Return a versioned output path based on existing files.

    Scans the output directory for files matching the pattern
    <stem>_v<number>.csv and picks the next available version number.
    For example, if lineage_report_v1.csv and lineage_report_v2.csv exist,
    this returns lineage_report_v3.csv.

    If no versioned files exist yet, returns <stem>_v1.csv.

    Why auto-versioning instead of overwriting?
      - Previous extraction runs represent a point-in-time snapshot. Overwriting
        them destroys history — you can't compare v1 vs v2 to see what changed
        when new pipelines were added or existing ones were modified.
      - Incremental versioning (v1, v2, v3...) is simpler than timestamps and
        easier to reference in conversations ("check v3 output").

    Args:
        base_path: The base output path from config (e.g.,
            "lineage_extraction/output/lineage_report.csv").

    Returns:
        A versioned path string (e.g.,
            "lineage_extraction/output/lineage_report_v3.csv").
    """
    out = Path(base_path)
    stem = out.stem  # e.g., "lineage_report"
    suffix = out.suffix  # e.g., ".csv"
    parent = out.parent

    # Ensure the output directory exists before scanning.
    parent.mkdir(parents=True, exist_ok=True)

    # Scan for existing versioned files matching <stem>_v<number>.csv
    # and find the highest version number.
    version_pattern = re.compile(rf"^{re.escape(stem)}_v(\d+){re.escape(suffix)}$")
    max_version = 0

    for existing in parent.iterdir():
        if not existing.is_file():
            continue
        match = version_pattern.match(existing.name)
        if match:
            version = int(match.group(1))
            if version > max_version:
                max_version = version

    next_version = max_version + 1
    versioned_name = f"{stem}_v{next_version}{suffix}"
    return str(parent / versioned_name)


def _print_summary(
    files_scanned: int,
    records_created: int,
    warnings: list[str],
    failures: list[str],
    output_path: str,
) -> None:
    """Print a human-readable summary of the extraction run to the console.

    This gives the operator immediate feedback on what happened:
      - How many files were scanned
      - How many lineage records were created
      - Any warnings (missing metadata, no targets, etc.)
      - Individual failure messages for files that could not be processed
      - Where the output CSV was written

    Args:
        files_scanned: Number of pipeline files found and processed.
        records_created: Total number of LineageRecord objects written.
        warnings: List of warning messages encountered.
        failures: List of error messages for files that could not be processed.
        output_path: Path to the output CSV file.
    """
    print("=" * 60)
    print("LINEAGE EXTRACTION SUMMARY")
    print("=" * 60)
    print(f"Files scanned:   {files_scanned}")
    print(f"Records created: {records_created}  (some files produce multiple records)")
    print(f"Warnings:         {len(warnings)}")
    print(f"Failed:           {len(failures)}")
    print()

    # Print individual warnings -- these are informational and don't affect
    # the exit code (only hard failures do).
    if warnings:
        print("WARNINGS:")
        for w in warnings:
            print(f"  {w}")
        print()

    # Print individual failure messages so the operator can see exactly
    # which files failed and why, without having to dig through stderr logs.
    if failures:
        print("FAILURES:")
        for f in failures:
            print(f"  {f}")
        print()

    print(f"Output saved to: {output_path}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def run_lineage_extraction(config_path: str) -> int:
    """Run the full lineage extraction pipeline.

    This is the main entry point for programmatic usage. It:
      1. Loads the YAML config.
      2. Scans for pipeline files.
      3. Processes each file (parsing + metadata extraction).
      4. Writes all records to a CSV file.
      5. Prints a summary.

    Args:
        config_path: Path to the YAML configuration file.

    Returns:
        0 if all files were processed successfully.
        1 if any file had a processing failure.
    """
    # Step 1: Load configuration
    config = _load_config(config_path)

    pipeline_dir = Path(config["pipeline_dir"])
    patterns: list[str] = config.get("file_patterns", ["*.py", "*.sql", "*.ipynb"])
    exclusions: list[str] = config.get("exclusions", [])
    output_path: str = config.get(
        "output_path",
        "scripts/lineage_extraction/output/lineage_report.csv",
    )

    # Step 2: Scan for pipeline files matching the configured patterns
    files = _scan_files(pipeline_dir, patterns, exclusions)

    # Step 3: Process each file and collect results
    all_records: list[LineageRecord] = []
    all_warnings: list[str] = []
    all_failures: list[str] = []

    for file_path in files:
        records, warnings, error = _process_file(file_path, pipeline_dir)

        all_records.extend(records)
        all_warnings.extend(warnings)

        if error is not None:
            # Log the error but continue processing other files.
            # A single broken file should not prevent processing of others.
            print(f"ERROR: {error}", file=sys.stderr)
            all_failures.append(error)

    # Step 4: Write the CSV report (auto-versioned so previous runs are kept)
    actual_output = _versioned_output_path(output_path)
    _write_csv(all_records, actual_output)

    # Step 5: Print summary
    _print_summary(
        files_scanned=len(files),
        records_created=len(all_records),
        warnings=all_warnings,
        failures=all_failures,
        output_path=actual_output,
    )

    # Return exit code: 0 for success, 1 if any file processing failures
    return 1 if all_failures else 0


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point: python -m lineage_extraction.lineage_extractor [config.yaml]

    If no config path is provided, uses the default configs/lineage_config.yaml
    relative to the package root.

    Exit codes:
        0 - All files processed successfully.
        1 - One or more files had processing failures.
    """
    # Default config path: relative to the package root (tools/lineage_extraction_tool/).
    # Users can override by passing a config path as the first CLI argument.
    default_config = "configs/lineage_config.yaml"

    config_path = sys.argv[1] if len(sys.argv) > 1 else default_config
    exit_code = run_lineage_extraction(config_path)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
