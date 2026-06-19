"""ColumnLineageRecord — one row per source-column → target-column mapping.

This replaces LineageRecord and ETLRecord with a single unified model
at column-level granularity, per manager feedback:

  "Lineage has to be at column level. Source table, column one maps to
   target table column one. It has to be granular."

Key design decisions:
  - One record = one column mapping. A 20-column SELECT produces 20 rows.
  - `file_path` replaces `domain` as the primary identifier — captures the
    full path or Git URL of the source file.
  - `sql_operation` classifies what happened to the column:
      SELECT   = direct pass-through (col appears in SELECT as-is)
      ALIAS    = renamed (SUM(x) AS total → source=x, target=total, op=AGGREGATE)
      AGGREGATE= col is an aggregate input (SUM, COUNT, AVG, MAX, MIN)
      JOIN     = col used in a join condition (ON a.x = b.y)
      FILTER   = col used in WHERE / HAVING clause
      WINDOW   = col used in a window function (OVER PARTITION BY / ORDER BY)
      UNKNOWN  = col detected but operation could not be determined
  - `source_database` and `target_database` are kept but may be blank when
    the query uses bare (unqualified) table names.
  - `job_name` is the filename stem (human-readable pipeline identifier).
  - `file_type` is "SQL" or "PySpark" so consumers can filter by language.
"""

from typing import Literal, Optional

from pydantic import BaseModel, Field


class ColumnLineageRecord(BaseModel):
    """One column-level lineage mapping row.

    Attributes:
        file_path:        Full path or Git URL of the source pipeline file.
        file_type:        Detected file type label (e.g. "SQL", "PySpark",
                          "YAML/dbt", "JSON", "R", "Looker", "PowerBI", etc.).
                          Kept as plain str so all formats returned by
                          universal_format_detector.get_file_type_label() are
                          accepted without raising a Pydantic ValidationError.
        job_name:         Filename stem (e.g. "etl_finance_revenue_agg").
        source_database:  Schema/DB prefix of the source table (blank if none).
        source_table:     Table being read from.
        source_column:    Column name in the source table.
        target_database:  Schema/DB prefix of the target table (blank if none).
        target_table:     Table being written to.
        target_column:    Column name in the target (alias or same as source).
        sql_operation:    What happened to this column in the transformation.
    """

    file_path: str
    file_type: str  # was Literal["SQL","PySpark"] — now accepts all format labels
    job_name: str
    source_database: str = ""
    source_table: str = ""
    source_column: str = ""
    target_database: str = ""
    target_table: str = ""
    target_column: str = ""
    sql_operation: str = "UNKNOWN"
    '''sql_operation: Literal[
        "SELECT", "AGGREGATE", "JOIN", "FILTER", "WINDOW", "ALIAS", "UNKNOWN"
    ] = "UNKNOWN"
    '''
    def to_row(self) -> dict:
        """Serialise to a flat dict for Excel/CSV writing."""
        return {
            "file_path":       self.file_path,
            "file_type":       self.file_type,
            "job_name":        self.job_name,
            "source_database": self.source_database,
            "source_table":    self.source_table,
            "source_column":   self.source_column,
            "target_database": self.target_database,
            "target_table":    self.target_table,
            "target_column":   self.target_column,
            "sql_operation":   self.sql_operation,
        }

    @staticmethod
    def column_headers() -> list[str]:
        """Single source of truth for Excel column ordering."""
        return [
            "file_path",
            "file_type",
            "job_name",
            "source_database",
            "source_table",
            "source_column",
            "target_database",
            "target_table",
            "target_column",
            "sql_operation",
        ]


class JobSummaryRecord(BaseModel):
    """One job-level summary row for Sheet 2 (AI-generated descriptions).

    Attributes:
        file_path:          Full path or Git URL.
        file_type:          "SQL" or "PySpark".
        job_name:           Filename stem.
        source_tables:      All source tables in this job (semicolon-separated).
        target_tables:      All target tables in this job (semicolon-separated).
        operations_summary: Comma-separated SQL operation types detected.
        job_description:    AI-generated plain-English description of the job.
    """

    file_path: str
    file_type: str  # was Literal["SQL","PySpark"] — now accepts all format labels
    job_name: str
    source_tables: str = ""
    target_tables: str = ""
    operations_summary: str = ""
    job_description: str = ""

    def to_row(self) -> dict:
        return {
            "file_path":          self.file_path,
            "file_type":          self.file_type,
            "job_name":           self.job_name,
            "source_tables":      self.source_tables,
            "target_tables":      self.target_tables,
            "operations_summary": self.operations_summary,
            "job_description":    self.job_description,
        }

    @staticmethod
    def column_headers() -> list[str]:
        return [
            "file_path",
            "file_type",
            "job_name",
            "source_tables",
            "target_tables",
            "operations_summary",
            "job_description",
        ]
