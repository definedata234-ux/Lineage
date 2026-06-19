"""Metadata variable extractor for Python, SQL, Jupyter, Power BI, and Looker files.

Every ETL pipeline in the Verizon-Frontier migration should declare
metadata variables at the top of its file so the lineage extraction
tool can classify and route the pipeline automatically.

This module extracts four metadata variables:
  - DOMAIN (required): Business domain, e.g. "billing", "network"
  - SUBJECT_AREA (required): Sub-domain, e.g. "payments", "outages"
  - SCHEDULE (optional): Cron expression or schedule description
  - OWNER (optional): Team or individual responsible

Extraction strategy depends on file type:
  - .py files: parsed via Python's ast module to find module-level
    string assignments (no f-strings, no expressions).
  - .sql files: regex scan for '-- KEY: value' or '-- @KEY: value'
    comment patterns.
  - .ipynb files: JSON is parsed, code cells are extracted and
    concatenated, then the combined source is parsed via AST.
  - .pbit files: JSON is parsed and metadata extracted from the "metadata" section.
  - .lkml files: regex scan for '# KEY: value' comment patterns.
"""

import ast
import json
import re
from pathlib import Path
from typing import TypedDict


# -- TypedDict for the result -------------------------------------------------
# Using TypedDict (not Pydantic BaseModel) because this is a lightweight
# structured dict returned by a helper function, not a validated domain model.
class MetadataVars(TypedDict):
    """Metadata variables extracted from a pipeline file.

    Attributes:
        domain: Business domain (defaults to "unknown" if not found).
        subject_area: Sub-domain (defaults to "unknown" if not found).
        schedule: Cron expression or schedule string (None if not found).
        owner: Team or person responsible (None if not found).
    """

    domain: str
    subject_area: str
    schedule: str | None
    owner: str | None


# -- Internal defaults --------------------------------------------------------
# When a required variable is missing, we use these fallback values.
# DOMAIN and SUBJECT_AREA are required, so they get a sentinel string.
# SCHEDULE and OWNER are optional, so they get None.
_METADATA_VARS: dict[str, str | None] = {
    "DOMAIN": "unknown",
    "SUBJECT_AREA": "unknown",
    "SCHEDULE": None,
    "OWNER": None,
}


# -- Internal helpers ---------------------------------------------------------


def _extract_python_metadata(code: str) -> dict[str, str]:
    """Extract metadata variables from Python source using AST.

    Why AST instead of regex? AST gives us reliable, structure-aware
    parsing that handles edge cases like:
      - Comments that mention DOMAIN but aren't assignments
      - String concatenation (we ignore non-constant values)
      - Nested scopes (we only look at module-level assignments)

    Only simple string literal assignments are extracted. F-strings,
    expressions, or non-string values are silently skipped.

    Args:
        code: Python source code as a string.

    Returns:
        A dict mapping variable names (e.g. "DOMAIN") to their string
        values. Only variables that were found and had simple string
        literal values are included.
    """
    found: dict[str, str] = {}
    # Guard against syntax errors in real-world pipeline files.
    # One broken file must not crash the entire lineage extraction process.
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return found

    for node in ast.iter_child_nodes(tree):
        # We only care about module-level Assign statements, not
        # assignments inside functions or classes.
        if not isinstance(node, ast.Assign):
            continue

        # An Assign node can have multiple targets (e.g. a = b = "x"),
        # but in practice metadata declarations use a single target.
        for target in node.targets:
            # Only handle simple name targets (e.g. DOMAIN = "finance"),
            # not attribute access (obj.DOMAIN = ...) or subscript.
            if not isinstance(target, ast.Name):
                continue

            # Only extract variables that we recognise as metadata keys.
            if target.id not in _METADATA_VARS:
                continue

            # Only extract simple string constants. Skip f-strings,
            # binary ops, function calls, etc.
            if not isinstance(node.value, ast.Constant):
                continue
            if not isinstance(node.value.value, str):
                continue

            found[target.id] = node.value.value

    return found


def _extract_sql_metadata(sql: str) -> dict[str, str]:
    """Extract metadata variables from SQL comments using regex.

    Supports two comment patterns commonly used in data engineering:
      - '-- KEY: value'        (plain comment)
      - '-- @KEY: value'       (tagged comment, e.g. dbt-style)

    The regex is intentionally simple: it looks for lines starting with
    '--' (optional whitespace), followed by an optional '@', then one
    of the known metadata variable names, a colon, and the value.

    Args:
        sql: SQL source code as a string.

    Returns:
        A dict mapping variable names to their extracted values.
        Only found variables are included.
    """
    found: dict[str, str] = {}

    # Build a regex that matches any of the known metadata keys.
    # Pattern breakdown:
    #   ^\s*--\s*     : line start, optional whitespace, '--', optional space
    #   @?            : optional '@' prefix (supports both styles)
    #   (KEY)         : capture group for the variable name
    #   \s*:\s*       : colon with optional surrounding whitespace
    #   (.+)          : capture group for the value (rest of line)
    #   $             : end of line
    keys_pattern = "|".join(_METADATA_VARS.keys())
    pattern = re.compile(
        rf"^\s*--\s*@?({keys_pattern})\s*:\s*(.+)$",
        re.MULTILINE,
    )

    for match in pattern.finditer(sql):
        key = match.group(1)
        value = match.group(2).strip()
        found[key] = value

    return found


def _extract_notebook_code(notebook_json: str) -> str:
    """Extract and concatenate code cells from a Jupyter notebook.

    Jupyter notebooks (.ipynb) are JSON files. We parse the JSON,
    iterate over cells, and collect only code cell source lines.
    Markdown cells and other cell types are ignored because they
    don't contain executable metadata declarations.

    The source field of a cell is typically a list of strings (one per
    line), but we handle both list and single-string formats.

    Args:
        notebook_json: Raw JSON string of a .ipynb file.

    Returns:
        A single string containing all code cell source lines
        concatenated together.
    """
    notebook = json.loads(notebook_json)
    code_lines: list[str] = []

    for cell in notebook.get("cells", []):
        # Only extract from code cells -- markdown cells and raw cells
        # don't contain executable metadata declarations.
        if cell.get("cell_type") != "code":
            continue

        source = cell.get("source", [])
        # The source can be a list of strings or a single string.
        # Jupyter's standard format uses a list.
        if isinstance(source, list):
            code_lines.extend(source)
        elif isinstance(source, str):
            code_lines.append(source)

    return "".join(code_lines)


def _extract_pbit_metadata(pbit_json: str) -> dict[str, str]:
    """Extract metadata variables from a Power BI .pbit file.

    .pbit files are JSON format with a "metadata" section at the top level
    that contains DOMAIN, SUBJECT_AREA, SCHEDULE, and OWNER fields.

    Args:
        pbit_json: Raw JSON string of a .pbit file.

    Returns:
        A dict mapping variable names to their extracted values.
        Only found variables are included.
    """
    found: dict[str, str] = {}

    try:
        pbit = json.loads(pbit_json)
        metadata = pbit.get("metadata", {})

        # Extract each metadata field if present
        for var_name in _METADATA_VARS:
            if var_name in metadata and metadata[var_name]:
                found[var_name] = metadata[var_name]
    except json.JSONDecodeError:
        # Invalid JSON - return empty dict
        pass

    return found


def _extract_qvs_metadata(qvs: str) -> dict[str, str]:
    """Extract metadata variables from Qlik Sense script (.qvs) files.

    QVS uses '//' line comments for metadata declarations:
      - '// KEY: value' comment pattern

    Args:
        qvs: QVS script source code as a string.

    Returns:
        A dict mapping variable names to their extracted values.
        Only found variables are included.
    """
    found: dict[str, str] = {}

    keys_pattern = "|".join(_METADATA_VARS.keys())
    pattern = re.compile(
        rf"^\s*//\s*({keys_pattern})\s*:\s*(.+)$",
        re.MULTILINE,
    )

    for match in pattern.finditer(qvs):
        key = match.group(1)
        value = match.group(2).strip()
        found[key] = value

    return found


def _extract_lookml_metadata(lookml: str) -> dict[str, str]:
    """Extract metadata variables from LookML files using regex.

    LookML uses '#' comments for metadata declarations, similar to SQL:
      - '# KEY: value' comment pattern

    Args:
        lookml: LookML source code as a string.

    Returns:
        A dict mapping variable names to their extracted values.
        Only found variables are included.
    """
    found: dict[str, str] = {}

    # Build a regex that matches any of the known metadata keys.
    # Pattern breakdown:
    #   ^\s*#\s+      : line start, optional whitespace, '#', space
    #   (KEY)         : capture group for the variable name
    #   \s*:\s*       : colon with optional surrounding whitespace
    #   (.+)          : capture group for the value (rest of line)
    #   $             : end of line
    keys_pattern = "|".join(_METADATA_VARS.keys())
    pattern = re.compile(
        rf"^\s*#\s+({keys_pattern})\s*:\s*(.+)$",
        re.MULTILINE,
    )

    for match in pattern.finditer(lookml):
        key = match.group(1)
        value = match.group(2).strip()
        found[key] = value

    return found


# -- Public API ---------------------------------------------------------------


def extract_metadata(file_path: str | Path, content: str) -> MetadataVars:
    """Extract metadata variables from a pipeline file.

    This is the main entry point. It routes extraction to the correct
    strategy based on the file extension:
      - .py   -> AST-based Python extraction
      - .sql  -> Regex-based SQL comment extraction
      - .ipynb -> Notebook JSON -> extract code cells -> AST extraction
      - .pbit -> JSON parsing -> extract from "metadata" section
      - .lkml -> Regex-based LookML comment extraction
      - .qvs  -> Regex-based QVS // comment extraction

    Any missing required variable defaults to "unknown". Optional
    variables default to None.

    Args:
        file_path: Path or filename used only to determine the file type.
        content: The full file content as a string.

    Returns:
        A MetadataVars TypedDict with all four metadata fields populated
        (or defaulted).
    """
    path = Path(file_path)
    suffix = path.suffix.lower()

    # Route to the correct extraction strategy based on file extension.
    if suffix == ".py":
        found = _extract_python_metadata(content)
    elif suffix == ".sql":
        found = _extract_sql_metadata(content)
    elif suffix == ".ipynb":
        # For notebooks: first extract code cells, then parse as Python.
        code = _extract_notebook_code(content)
        found = _extract_python_metadata(code)
    elif suffix == ".pbit":
        found = _extract_pbit_metadata(content)
    elif suffix == ".lkml":
        found = _extract_lookml_metadata(content)
    elif suffix == ".qvs":
        found = _extract_qvs_metadata(content)
    else:
        # Unknown file type: no metadata extracted.
        found = {}

    # Build the result dict, applying defaults for anything not found.
    # Map from uppercase variable names (as declared in code) to the
    # lowercase TypedDict keys used in the return value.
    key_map = {
        "DOMAIN": "domain",
        "SUBJECT_AREA": "subject_area",
        "SCHEDULE": "schedule",
        "OWNER": "owner",
    }

    result: MetadataVars = {}
    for var_name, dict_key in key_map.items():
        if var_name in found:
            result[dict_key] = found[var_name]  # type: ignore[literal-required]
        else:
            # Use the default value from _METADATA_VARS.
            # DOMAIN and SUBJECT_AREA default to "unknown";
            # SCHEDULE and OWNER default to None.
            default = _METADATA_VARS[var_name]
            result[dict_key] = default  # type: ignore[literal-required]

    return result
