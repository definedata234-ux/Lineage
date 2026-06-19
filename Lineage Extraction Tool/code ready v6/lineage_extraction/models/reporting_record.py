"""ReportingRecord Pydantic model for BI report lineage extraction.

This model captures the 9 fields extracted from each BI report file
(PowerBI .pbit, Qlik .qvs, Looker .lkml).

Design decisions:
  - Separate from LineageRecord (models.py) — BI reports have a fundamentally
    different shape (no source/target tables, no job_type, no schedule/owner).
  - List fields default to empty list so partial results never cause
    ValidationError when a parser cannot extract certain fields.
  - to_row() joins lists with semicolons for easy consumption in Excel.
  - column_headers() is the single source of truth for column ordering.
    Named column_headers (not columns) to avoid shadowing the columns field.
  - tool_name and file_name were added to support per-tool Excel tabs and
    traceability back to the source file.
"""

from pydantic import BaseModel, Field


class ReportingRecord(BaseModel):
    """A single row of reporting tool lineage metadata.

    Attributes:
        domain: Business domain (e.g. "sales", "finance").
        subdomain: Sub-domain (e.g. "analytics", "revenue").
        file_name: Name of the source file (e.g. "bi_customer_360_simple.pbit").
        report_name: Filename stem of the report file.
        sql_name: Named queries / datasets within the report.
        tables: All tables referenced across queries, deduplicated.
        columns: All columns referenced across queries, deduplicated.
        operation: SQL operation types detected: SELECT, JOIN, AGGREGATE.
        tool_name: BI tool that produced the file: PowerBI, Looker, or Qlik.
    """

    domain: str
    subdomain: str
    file_name: str
    report_name: str
    tool_name: str
    sql_name: list[str] = Field(default_factory=list)
    tables: list[str] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)
    operation: list[str] = Field(default_factory=list)

    def to_row(self) -> dict:
        """Serialise this record to a flat dict for Excel writing.

        List fields are joined with semicolons. Empty lists become "".
        Scalar fields pass through unchanged.
        """
        def _join(lst: list[str]) -> str:
            return ";".join(lst) if lst else ""

        return {
            "domain": self.domain,
            "subdomain": self.subdomain,
            "file_name": self.file_name,
            "report_name": self.report_name,
            "sql_name": _join(self.sql_name),
            "tables": _join(self.tables),
            "columns": _join(self.columns),
            "operation": _join(self.operation),
            "tool_name": self.tool_name,
        }

    @staticmethod
    def column_headers() -> list[str]:
        """Return canonical column order for the Excel header."""
        return [
            "domain",
            "subdomain",
            "file_name",
            "report_name",
            "sql_name",
            "tables",
            "columns",
            "operation",
            "tool_name",
        ]
