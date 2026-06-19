"""LineageRecord Pydantic model for data lineage extraction.

This model captures all metadata about one data flow within a pipeline file.
It is the foundation for every other module in the lineage extraction tool:
  - SQL / PySpark parsers populate a list of these records
  - The CSV writer calls to_csv_row() to serialise them
  - The CLI / orchestrator passes them through the pipeline

Design decisions:
  - Lists (source_tables, target_tables, etc.) are stored as Python lists
    internally for easy manipulation, and serialised to semicolon-delimited
    strings when writing to CSV.
  - job_type is constrained to Literal["curated", "aggregation"] because
    those are the only two pipeline categories in the Verizon-Frontier
    migration scope.
  - schedule and owner are Optional[str] because they are not always
    present in every pipeline file.
"""

from typing import Literal, Optional

from pydantic import BaseModel, Field


class LineageRecord(BaseModel):
    """A single row of data lineage metadata.

    Attributes:
        domain: Business domain (e.g. "billing", "network").
        subject_area: Sub-domain or data subject area.
        job_name: Name of the pipeline / job.
        file_path: Path to the pipeline file on disk.
        source_tables: Tables this pipeline reads from.
        target_tables: Tables this pipeline writes to.
        key_columns: Primary / foreign key columns used in joins.
        transformation_logic: Free-text description of the transformation.
        kpi_metrics: Business KPI columns produced by this pipeline.
        job_type: Either "curated" (bronze-to-silver) or "aggregation"
                  (silver-to-gold rollups).
        schedule: Cron expression or schedule description, if known.
        owner: Team or individual responsible for the pipeline.
    """

    # -- Required fields --
    domain: str
    subject_area: str
    job_name: str
    file_path: str

    # -- List fields (default to empty list for convenience) --
    source_tables: list[str] = Field(default_factory=list)
    target_tables: list[str] = Field(default_factory=list)
    key_columns: list[str] = Field(default_factory=list)
    kpi_metrics: list[str] = Field(default_factory=list)

    # -- String fields with sensible defaults --
    # transformation_logic defaults to empty string because most pipelines
    # will have some logic filled in later by the parser, but a missing
    # value should not cause a ValidationError.
    transformation_logic: str = ""

    # -- Constrained field --
    # Only two pipeline categories exist in the Verizon-Frontier migration.
    job_type: Literal["curated", "aggregation"] = "curated"

    # -- Truly optional fields (None when not available) --
    schedule: Optional[str] = None
    owner: Optional[str] = None

    def to_csv_row(self) -> dict:
        """Serialise this record to a flat dict suitable for CSV writing.

        List fields are joined with newlines so each value appears on its
        own line within the cell. Excel and Google Sheets render newlines
        inside quoted CSV cells as line breaks, making multi-value fields
        easy to read without splitting the record across rows.
        Empty lists become an empty string.  All other fields pass through
        unchanged (including None, which the CSV writer will convert to "").

        Returns:
            A dict mapping column names to scalar / serialised values.
        """
        return {
            "domain": self.domain,
            "subject_area": self.subject_area,
            "job_name": self.job_name,
            "file_path": self.file_path,
            "source_tables": "\n".join(self.source_tables),
            "target_tables": "\n".join(self.target_tables),
            "key_columns": "\n".join(self.key_columns),
            "transformation_logic": self.transformation_logic,
            "kpi_metrics": "\n".join(self.kpi_metrics),
            "job_type": self.job_type,
            "schedule": self.schedule or "",
            "owner": self.owner or "",
        }

    @staticmethod
    def csv_columns() -> list[str]:
        """Return the canonical column order for the CSV header.

        This is the single source of truth for column ordering.  Every
        CSV file produced by the lineage tool uses this order.

        Returns:
            A list of column name strings in the correct order.
        """
        return [
            "domain",
            "subject_area",
            "job_name",
            "file_path",
            "source_tables",
            "target_tables",
            "key_columns",
            "transformation_logic",
            "kpi_metrics",
            "job_type",
            "schedule",
            "owner",
        ]
