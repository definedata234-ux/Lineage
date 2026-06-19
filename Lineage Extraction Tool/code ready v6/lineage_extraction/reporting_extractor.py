"""Reporting tool lineage extraction orchestrator.

This module is the reporting counterpart to lineage_extractor.py. It scans
BI report files (.pbit, .qvs, .lkml) and produces an Excel workbook with
one sheet per BI tool (PowerBI, Looker, Qlik), each containing rows with:
domain, subdomain, file_name, report_name, sql_name, tables, columns,
operation, tool_name.

Design decisions:
  - Completely separate from lineage_extractor.py. The ETL pipeline is
    never imported or modified.
  - One ReportingRecord per file — all sql_name/table/column/operation
    values from that file are merged into semicolon-separated lists in a
    single row.
  - Domain/subdomain come from extract_metadata() in metadata_extractor.py,
    the same function used by the ETL extractor.
  - Excel output with one sheet per BI tool so analysts can review
    each tool's lineage independently.
  - Versioned output (reporting_lineage_v1.xlsx, v2.xlsx...) so previous
    runs are preserved for comparison.
  - File-level errors are caught and logged; processing continues for
    remaining files. Exit code 1 if any file fails.
"""

import re
import sys
from fnmatch import fnmatch
from pathlib import Path
from typing import Optional

import yaml
from openpyxl import Workbook

from metadata_extractor import extract_metadata
from reporting_record import ReportingRecord
from powerbi_parser import extract_reporting_lineage as _powerbi_extract
from looker_parser import extract_reporting_lineage as _looker_extract
from qlik_parser import extract_reporting_lineage as _qlik_extract


# Mapping of file suffix to the appropriate extract_reporting_lineage function
_PARSER_MAP = {
    ".pbit": _powerbi_extract,
    ".lkml": _looker_extract,
    ".qvs": _qlik_extract,
}

# Mapping of file suffix to human-readable tool name for Excel sheet naming
_SUFFIX_TO_TOOL = {
    ".pbit": "PowerBI",
    ".lkml": "Looker",
    ".qvs": "Qlik",
}


def _load_config(config_path: str) -> dict:
    """Load and parse the YAML pipeline config file."""
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _scan_reporting_files(
    pipeline_dir: Path,
    patterns: list[str],
    exclusions: list[str],
) -> list[Path]:
    """Recursively scan pipeline_dir for reporting files.

    Args:
        pipeline_dir: Root directory to scan.
        patterns: Glob patterns for files to include (e.g. ["*.pbit"]).
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


def _versioned_reporting_output_path(base_path: str) -> str:
    """Return the next versioned output path for the reporting Excel file.

    Scans the output directory for existing reporting_lineage_v<N>.xlsx
    files and picks N+1. Starts at v1 if none exist.

    Args:
        base_path: Base output path (e.g. ".../output/reporting_lineage.xlsx").

    Returns:
        Versioned path string (e.g. ".../output/reporting_lineage_v1.xlsx").
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


def _process_file(
    file_path: Path,
) -> tuple[Optional[ReportingRecord], list[str], Optional[str]]:
    """Process one reporting file and return a ReportingRecord.

    Args:
        file_path: Absolute path to the reporting file.

    Returns:
        Tuple of (record, warnings, error_message).
        record is None if processing failed.
    """
    warnings: list[str] = []
    suffix = file_path.suffix.lower()
    report_name = file_path.stem
    file_name = file_path.name

    # Read file content — .pbit files are binary ZIP archives, all others are text
    try:
        if suffix == ".pbit":
            # .pbit is a ZIP — extract text content from DataModel entry for metadata
            import zipfile as _zf
            content = ""
            try:
                with _zf.ZipFile(file_path, "r") as zf:
                    # Try DataModel first, then any JSON-like entry
                    for entry in zf.namelist():
                        if entry in ("DataModel", "Report/Layout") or entry.endswith(".json"):
                            try:
                                content = zf.read(entry).decode("utf-8", errors="replace")
                                break
                            except Exception:
                                continue
            except _zf.BadZipFile:
                # Some .pbit files are plain JSON — try reading as text
                try:
                    content = file_path.read_text(encoding="utf-8", errors="replace")
                except OSError as exc:
                    return (None, [], f"Failed to read {file_path}: {exc}")
        else:
            content = file_path.read_text(encoding="utf-8")
    except OSError as exc:
        return (None, [], f"Failed to read {file_path}: {exc}")

    # Route to the correct parser
    parser = _PARSER_MAP.get(suffix)
    if parser is None:
        return (None, [f"Unsupported file type: {suffix}"], None)

    try:
        parsed = parser(file_path)
    except Exception as exc:
        return (None, [], f"Parser error on {file_path}: {exc}")

    # Get domain/subdomain from existing metadata extractor
    meta = extract_metadata(file_path, content)
    domain: str = meta.get("domain", "unknown") or "unknown"
    subdomain: str = meta.get("subject_area", "unknown") or "unknown"

    if domain == "unknown":
        warnings.append(f"{file_path}: missing DOMAIN metadata")
    if subdomain == "unknown":
        warnings.append(f"{file_path}: missing SUBJECT_AREA metadata")

    # Warn on empty key fields
    if not parsed.get("sql_name"):
        warnings.append(f"{file_path}: no sql_name extracted")
    if not parsed.get("tables"):
        warnings.append(f"{file_path}: no tables extracted")
    if not parsed.get("columns"):
        warnings.append(f"{file_path}: no columns extracted")

    # Derive tool name from file suffix
    tool_name = _SUFFIX_TO_TOOL.get(suffix, "Unknown")

    record = ReportingRecord(
        domain=domain,
        subdomain=subdomain,
        file_name=file_name,
        report_name=report_name,
        tool_name=tool_name,
        sql_name=parsed.get("sql_name", []),
        tables=parsed.get("tables", []),
        columns=parsed.get("columns", []),
        operation=parsed.get("operation", []),
    )
    return (record, warnings, None)


def _write_excel(records: list[ReportingRecord], output_path: str) -> None:
    """Write reporting records to an Excel file with one sheet per BI tool.

    Groups records by tool_name and creates a separate sheet for each.
    """
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    wb = Workbook()
    col_names = ReportingRecord.column_headers()

    # Handle empty records — keep the default sheet with a message
    if not records:
        ws = wb.active
        ws.title = "Summary"
        ws.append(["No reporting records were extracted."])
        wb.save(str(out))
        return

    # Remove the default sheet created by openpyxl
    wb.remove(wb.active)

    # Group records by tool name, preserving the order tools appear in
    tool_order: list[str] = []
    tool_records: dict[str, list[ReportingRecord]] = {}
    for record in records:
        tool = record.tool_name
        if tool not in tool_records:
            tool_order.append(tool)
            tool_records[tool] = []
        tool_records[tool].append(record)

    for tool in tool_order:
        ws = wb.create_sheet(title=tool)
        # Write header row
        ws.append(col_names)
        # Write data rows
        for record in tool_records[tool]:
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
    print("REPORTING LINEAGE EXTRACTION SUMMARY")
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


def run_reporting_extraction(
    config_path: str,
    output_dir: Optional[str] = None,
) -> int:
    """Run the full reporting lineage extraction pipeline.

    Args:
        config_path: Path to the YAML pipeline config file.
        output_dir: Optional override for the output directory.
                    Defaults to scripts/lineage_extraction/output/.

    Returns:
        0 on success, 1 if any file had a processing failure.
    """
    config = _load_config(config_path)

    pipeline_dir = Path(config["pipeline_dir"])
    patterns: list[str] = config.get(
        "reporting_patterns", ["*.pbit", "*.lkml", "*.qvs"]
    )
    exclusions: list[str] = config.get("exclusions", [])

    # Determine output path
    if output_dir:
        base_output = str(Path(output_dir) / "reporting_lineage.xlsx")
    else:
        base_output = str(
            Path(config_path).resolve().parents[1]
            / "output"
            / "reporting_lineage.xlsx"
        )

    files = _scan_reporting_files(pipeline_dir, patterns, exclusions)

    all_records: list[ReportingRecord] = []
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

    actual_output = _versioned_reporting_output_path(base_output)
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
    """CLI entry point: python -m lineage_extraction.reporting_extractor [config.yaml]"""
    # Default config path for the standalone package
    default_config = "configs/reporting_config.yaml"
    config_path = sys.argv[1] if len(sys.argv) > 1 else default_config
    sys.exit(run_reporting_extraction(config_path))


if __name__ == "__main__":
    main()
