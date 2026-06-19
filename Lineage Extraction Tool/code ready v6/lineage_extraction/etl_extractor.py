# scripts/lineage_extraction/python/etl_extractor.py
"""ETL migration construct extraction orchestrator.

Scans Databricks pipeline files (.sql, .py, .ipynb) and produces an Excel
workbook with one sheet per file type ("SQL", "PySpark"). Each row represents
one file with all detected Databricks-specific constructs.

Design decisions:
  - Modeled on reporting_extractor.py: same _load_config / _scan / _process /
    _write_excel / _print_summary flow.
  - Reuses pipeline_config.yaml and metadata_extractor.py as-is.
  - BI report files (.pbit, .lkml, .qvs) are skipped with a warning — they
    are handled by the reporting extractor, not the ETL extractor.
  - .ipynb files have code cells extracted and are routed to the PySpark parser.
  - Versioned output (etl_migration_v1.xlsx, v2.xlsx...) preserves previous runs.
  - File-level errors are caught and logged; processing continues for remaining
    files. Exit code 1 if any file had a failure.
"""

import json
import re
import sys
from fnmatch import fnmatch
from pathlib import Path
from typing import Optional

import yaml
from openpyxl import Workbook

from lineage_extraction.parsers.metadata_extractor import extract_metadata
from lineage_extraction.models.etl_record import ETLRecord
from lineage_extraction.parsers.databricks_sql_parser import parse_databricks_sql
from lineage_extraction.parsers.databricks_pyspark_parser import parse_databricks_pyspark


# File suffixes that belong to the reporting extractor, not the ETL extractor.
# When encountered during scanning, they are skipped with a warning.
_BI_SUFFIXES = {".pbit", ".lkml", ".qvs"}


def _load_config(config_path: str) -> dict:
    """Load and parse the YAML pipeline config file."""
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _scan_etl_files(
    pipeline_dir: Path,
    patterns: list[str],
    exclusions: list[str],
) -> list[Path]:
    """Recursively scan pipeline_dir for pipeline files.

    Args:
        pipeline_dir: Root directory to scan.
        patterns: Glob patterns for files to include.
        exclusions: Path component patterns to exclude.

    Returns:
        Sorted list of matching, non-excluded file paths.
    """
    matched: list[Path] = []
    for file_path in pipeline_dir.rglob("*"):
        if not file_path.is_file():
            continue
        if not any(fnmatch(file_path.name, pat) for pat in patterns):
            continue
        rel_str = str(file_path.relative_to(pipeline_dir)).replace("\\", "/")
        parts = rel_str.split("/")
        if any(excl.rstrip("/") in parts for excl in exclusions):
            continue
        matched.append(file_path)
    return sorted(matched)


def _versioned_etl_output_path(base_path: str) -> str:
    """Return the next versioned output path for the ETL migration Excel file.

    Scans for existing etl_migration_v<N>.xlsx and picks N+1.
    """
    out = Path(base_path)
    stem = out.stem
    suffix = out.suffix
    parent = out.parent
    parent.mkdir(parents=True, exist_ok=True)

    version_pattern = re.compile(
        rf"^{re.escape(stem)}_v(\d+){re.escape(suffix)}$"
    )
    max_version = 0
    if parent.exists():
        for existing in parent.iterdir():
            if not existing.is_file():
                continue
            m = version_pattern.match(existing.name)
            if m:
                v = int(m.group(1))
                if v > max_version:
                    max_version = v

    return str(parent / f"{stem}_v{max_version + 1}{suffix}")


def _extract_notebook_code(notebook_json: str) -> str:
    """Extract and concatenate code cells from a Jupyter notebook."""
    try:
        notebook = json.loads(notebook_json)
    except (json.JSONDecodeError, TypeError):
        return ""
    code_parts: list[str] = []
    for cell in notebook.get("cells", []):
        if cell.get("cell_type") == "code":
            source = cell.get("source", [])
            if isinstance(source, list):
                code_parts.append("".join(source))
            elif isinstance(source, str):
                code_parts.append(source)
    return "\n".join(code_parts)


def _process_file(
    file_path: Path,
) -> tuple[Optional[ETLRecord], list[str], Optional[str]]:
    """Process one pipeline file and return an ETLRecord.

    Args:
        file_path: Absolute path to the pipeline file.

    Returns:
        Tuple of (record, warnings, error_message).
        record is None if the file was skipped or processing failed.
    """
    warnings: list[str] = []
    suffix = file_path.suffix.lower()
    file_name = file_path.name
    job_name = file_path.stem

    # Skip BI report files
    if suffix in _BI_SUFFIXES:
        return (None, [f"Skipping BI report file: {file_path}"], None)

    # Read file content
    try:
        content = file_path.read_text(encoding="utf-8")
    except OSError as exc:
        return (None, [], f"Failed to read {file_path}: {exc}")

    # Get domain from metadata extractor and route to the correct parser.
    # Both steps are wrapped in a single try/except because metadata
    # extraction can fail for corrupt files (e.g. invalid .ipynb JSON),
    # and we want to catch that and report it as a processing failure
    # rather than crashing the entire orchestrator.
    try:
        meta = extract_metadata(file_path, content)
        domain: str = meta.get("domain", "unknown") or "unknown"

        if domain == "unknown":
            warnings.append(f"{file_path}: missing DOMAIN metadata")

        if suffix == ".sql":
            file_type = "SQL"
            parsed = parse_databricks_sql(content)
        elif suffix == ".py":
            file_type = "PySpark"
            parsed = parse_databricks_pyspark(content)
        elif suffix == ".ipynb":
            file_type = "PySpark"
            code = _extract_notebook_code(content)
            if not code.strip():
                return (None, [], f"No code cells in notebook: {file_path}")
            parsed = parse_databricks_pyspark(code)
        else:
            return (None, [f"Unsupported file type: {suffix}"], None)
    except Exception as exc:
        return (None, [], f"Parser error on {file_path}: {exc}")

    record = ETLRecord(
        domain=domain,
        file_type=file_type,
        job_name=job_name,
        file_name=file_name,
        source_database=parsed["source_db"],
        source_table=parsed["source_tables"],
        target_database=parsed["target_db"],
        target_table=parsed["target_tables"],
        constructs_found=parsed["constructs_found"],
        has_udf=parsed["has_udf"],
        has_delta_ops=parsed["has_delta_ops"],
        complexity=parsed["complexity"],
    )
    return (record, warnings, None)


def _write_excel(records: list[ETLRecord], output_path: str) -> None:
    """Write ETL records to an Excel file with one sheet per file_type.

    Groups records by file_type ("SQL", "PySpark") and creates a separate
    sheet for each. If no records exist for a type, that sheet is omitted.
    """
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    wb = Workbook()
    col_names = ETLRecord.column_headers()

    if not records:
        ws = wb.active
        ws.title = "Summary"
        ws.append(["No ETL records were extracted."])
        wb.save(str(out))
        return

    # Remove the default sheet created by openpyxl
    wb.remove(wb.active)

    # Group records by file_type, preserving order
    type_order: list[str] = []
    type_records: dict[str, list[ETLRecord]] = {}
    for record in records:
        ftype = record.file_type
        if ftype not in type_records:
            type_order.append(ftype)
            type_records[ftype] = []
        type_records[ftype].append(record)

    for ftype in type_order:
        ws = wb.create_sheet(title=ftype)
        ws.append(col_names)
        for record in type_records[ftype]:
            ws.append([record.to_row()[col] for col in col_names])

    wb.save(str(out))


def _print_summary(
    files_scanned: int,
    records_created: int,
    warnings: list[str],
    failures: list[str],
    output_path: str,
) -> None:
    """Print a human-readable run summary to stdout."""
    print("=" * 60)
    print("ETL MIGRATION EXTRACTION SUMMARY")
    print("=" * 60)
    print(f"Files scanned:   {files_scanned}")
    print(f"Records created: {records_created}")
    print(f"Warnings:        {len(warnings)}")
    print(f"Failed:          {len(failures)}")
    if warnings:
        print("\nWARNINGS:")
        for w in warnings:
            print(f"  {w}")
    if failures:
        print("\nFAILURES:", file=sys.stderr)
        for f in failures:
            print(f"  {f}", file=sys.stderr)
    print(f"\nOutput saved to: {output_path}")
    print("=" * 60)


def run_etl_extraction(
    config_path: str,
    output_dir: Optional[str] = None,
) -> int:
    """Run the full ETL migration extraction pipeline.

    Args:
        config_path: Path to the YAML pipeline config file.
        output_dir: Optional override for the output directory.

    Returns:
        0 on success, 1 if any file had a processing failure.
    """
    config = _load_config(config_path)

    pipeline_dir = Path(config["pipeline_dir"])
    patterns: list[str] = config.get(
        "file_patterns",
        ["*.py", "*.sql", "*.ipynb", "*.pbit", "*.lkml", "*.qvs"],
    )
    exclusions: list[str] = config.get("exclusions", [])

    # Determine output path
    if output_dir:
        base_output = str(Path(output_dir) / "etl_migration.xlsx")
    else:
        base_output = str(
            Path(config_path).resolve().parents[1]
            / "output"
            / "etl_migration.xlsx"
        )

    files = _scan_etl_files(pipeline_dir, patterns, exclusions)

    all_records: list[ETLRecord] = []
    all_warnings: list[str] = []
    all_failures: list[str] = []

    for file_path in files:
        record, warnings, error = _process_file(file_path)
        all_warnings.extend(warnings)
        if error is not None:
            print(f"ERROR: {error}", file=sys.stderr)
            all_failures.append(error)
        elif record is not None:
            all_records.append(record)

    actual_output = _versioned_etl_output_path(base_output)
    _write_excel(all_records, actual_output)
    _print_summary(
        files_scanned=len(files),
        records_created=len(all_records),
        warnings=all_warnings,
        failures=all_failures,
        output_path=actual_output,
    )

    return 1 if all_failures else 0


def main() -> None:
    """CLI entry point: python -m lineage_extraction.etl_extractor [config.yaml]"""
    default_config = "configs/etl_config.yaml"
    config_path = sys.argv[1] if len(sys.argv) > 1 else default_config
    sys.exit(run_etl_extraction(config_path))


if __name__ == "__main__":
    main()
