# scripts/lineage_extraction/python/etl_models.py
"""ETLRecord Pydantic model for Databricks migration construct extraction.

One record per pipeline file. Each row in the Excel output represents one file
with all detected Databricks-specific constructs compressed into a single record.

Design decisions:
  - Separate from ReportingRecord — ETL pipeline extraction has a fundamentally
    different shape (source/target tables with database prefixes, construct
    detection, complexity assessment).
  - construct_count is auto-derived via Pydantic computed_field — it can never
    be manually set and is always len(constructs_found).
  - to_row() joins lists with semicolons and converts bools to "yes"/"no" for
    easy consumption in Excel.
  - column_headers() is the single source of truth for column ordering.
"""

from typing import Literal

from pydantic import BaseModel, Field, computed_field


class ETLRecord(BaseModel):
    """A single row of ETL migration construct metadata.

    Attributes:
        domain: Business domain (e.g. "finance", "hr").
        file_type: Either "SQL" or "PySpark" — determines the Excel sheet.
        job_name: Filename stem of the pipeline file.
        file_name: Full filename including extension.
        source_database: Database/schema names from source tables (preserved prefix).
        source_table: Bare table names read from.
        target_database: Database/schema names from target tables (preserved prefix).
        target_table: Bare table names written to.
        constructs_found: All Databricks-specific constructs detected.
        construct_count: Auto-derived count of constructs_found.
        has_udf: Whether any UDF patterns were detected.
        has_delta_ops: Whether any Delta Lake operations were detected.
        complexity: Migration complexity — "direct", "rewrite", or "manual".
    """

    domain: str
    file_type: Literal["SQL", "PySpark"]
    job_name: str
    file_name: str
    source_database: list[str] = Field(default_factory=list)
    source_table: list[str] = Field(default_factory=list)
    target_database: list[str] = Field(default_factory=list)
    target_table: list[str] = Field(default_factory=list)
    constructs_found: list[str] = Field(default_factory=list)
    has_udf: bool = False
    has_delta_ops: bool = False
    complexity: Literal["direct", "rewrite", "manual"] = "direct"

    @computed_field
    @property
    def construct_count(self) -> int:
        """Auto-derived count — always matches len(constructs_found)."""
        return len(self.constructs_found)

    def to_row(self) -> dict:
        """Serialise this record to a flat dict for Excel writing.

        List fields are joined with semicolons. Empty lists become "".
        Bools become "yes"/"no". construct_count is the derived int.
        """
        def _join(lst: list[str]) -> str:
            return ";".join(lst) if lst else ""

        def _bool(val: bool) -> str:
            return "yes" if val else "no"

        return {
            "domain": self.domain,
            "file_type": self.file_type,
            "job_name": self.job_name,
            "file_name": self.file_name,
            "source_database": _join(self.source_database),
            "source_table": _join(self.source_table),
            "target_database": _join(self.target_database),
            "target_table": _join(self.target_table),
            "constructs_found": _join(self.constructs_found),
            "construct_count": self.construct_count,
            "has_udf": _bool(self.has_udf),
            "has_delta_ops": _bool(self.has_delta_ops),
            "complexity": self.complexity,
        }

    @staticmethod
    def column_headers() -> list[str]:
        """Return canonical column order for the Excel header."""
        return [
            "domain", "file_type", "job_name", "file_name",
            "source_database", "source_table",
            "target_database", "target_table",
            "constructs_found", "construct_count",
            "has_udf", "has_delta_ops", "complexity",
        ]
