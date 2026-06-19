# Lineage Extraction Tool v2.0 — Setup Guide

## What changed in v2

| Old (v1) | New (v2) |
|---|---|
| Table-level lineage | **Column-level lineage** (one row per column mapping) |
| `domain` as identifier | **`file_path`** as identifier (local path or Git URL) |
| UDF / delta / complexity columns | **Removed** |
| One input mode (upload) | **Three input modes**: Upload · Git · Paste |
| No job description | **Sheet 2: AI-generated job description** per pipeline |

---

## Files in this package

```
server.py                        ← FastAPI server (v2)
lineage_extraction_tool.html     ← Web UI (v2)
requirements.txt                 ← Updated dependencies
column_lineage_record.py         ← New Pydantic models
column_sql_parser.py             ← Column-level SQL parser
column_pyspark_parser.py         ← Column-level PySpark parser
column_lineage_extractor.py      ← Orchestrator + Excel writer
```

Place all files in your project root alongside the existing `lineage_extraction/` package.

---

## Install

```bash
pip install -e .
pip install -r requirements.txt
```

For Git repository support (optional):
```bash
pip install gitpython
```

---

## Start the server

```bash
uvicorn server:app --host 0.0.0.0 --port 8000 --reload
```

Open: `http://localhost:8000`

---

## Output schema

### Sheet 1 — Column Lineage
One row per `source_column → target_column` mapping.

| Column | Description |
|---|---|
| `file_path` | Full path or Git URL of the pipeline file |
| `file_type` | `SQL` or `PySpark` |
| `job_name` | Filename stem |
| `source_database` | Schema/DB of the source table (blank if unqualified) |
| `source_table` | Table being read from |
| `source_column` | Column in the source |
| `target_database` | Schema/DB of the target table |
| `target_table` | Table being written to |
| `target_column` | Column in the target (alias or same name) |
| `sql_operation` | `SELECT`, `AGGREGATE`, `JOIN`, `FILTER`, `WINDOW`, `ALIAS`, `UNKNOWN` |

### Sheet 2 — Job Summary
One row per pipeline file.

| Column | Description |
|---|---|
| `file_path` | Full path or Git URL |
| `file_type` | `SQL` or `PySpark` |
| `job_name` | Filename stem |
| `source_tables` | Semicolon-separated source tables |
| `target_tables` | Semicolon-separated target tables |
| `operations_summary` | Comma-separated operation types detected |
| `job_description` | AI-generated plain-English description |

---

## API Endpoints

| Method | Endpoint | Input | Output |
|---|---|---|---|
| `POST` | `/extract/upload` | `.sql`, `.py`, `.ipynb` files (multipart) | `.xlsx` |
| `POST` | `/extract/from-git` | JSON: `{repo_url, branch, path_filter, file_types}` | `.xlsx` |
| `GET` | `/health` | — | `{"status":"ok","version":"2.0.0"}` |
| `GET` | `/` | — | HTML UI |
| `GET` | `/docs` | — | Auto-generated API docs |

### Git endpoint request body
```json
{
  "repo_url": "https://github.com/org/repo",
  "branch": "main",
  "path_filter": "pipelines/finance",
  "file_types": [".sql", ".py", ".ipynb"],
  "include_ai_descriptions": true
}
```

---

## Troubleshooting

**`ModuleNotFoundError: No module named 'lineage_extraction'`**
→ Run `pip install -e .` from the project root.

**Git clone fails**
→ Check the repo URL is correct and accessible.
→ For private repos, set up SSH or use a personal access token in the URL.

**AI descriptions show the fallback template**
→ The server needs network access to `api.anthropic.com`.
→ The Anthropic API key must be available in the server environment.
→ Toggle off "Generate AI descriptions" to skip this step.

**No column mappings extracted**
→ SQL files need `SELECT` statements with explicit column names (not just `SELECT *`).
→ PySpark files need `.select("col")`, `.withColumn()`, or `.agg()` calls.
→ Files with only DDL (CREATE TABLE, DROP) produce no mappings.
