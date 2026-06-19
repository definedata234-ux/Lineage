"""
Metadata Extractor Stress Test — Python

Tests every edge case in _extract_python_metadata():

1. DOMAIN appears twice — only the FIRST assignment should win
   (ast.iter_child_nodes visits in source order, first write wins via dict)
2. SUBJECT_AREA assigned via a concatenation expression (ast.BinOp) — must be SKIPPED
3. SCHEDULE assigned via an f-string (ast.JoinedStr) — must be SKIPPED
4. OWNER assigned inside a function — must be SKIPPED (not module-level)
5. A class attribute DOMAIN = "wrong" — must be SKIPPED
6. DOMAIN in a comment — must be SKIPPED
7. A variable named DOMAIN_BACKUP — must NOT trigger DOMAIN extraction
   (only exact key names in _METADATA_VARS match)
8. All four valid metadata fields set correctly at module level
"""

# Valid module-level metadata — extractor should pick these up
DOMAIN = "logistics"
SUBJECT_AREA = "fleet_tracking"
SCHEDULE = "0 8 * * *"
OWNER = "fleet-ops-team"

# --- Edge case 1: DOMAIN re-assigned below — first assignment wins ---
DOMAIN = "should_be_ignored"    # second assignment; first "logistics" must be kept

# --- Edge case 2: SUBJECT_AREA via BinOp (concatenation) — must be SKIPPED ---
SUBJECT_AREA = "fleet_" + "tracking_v2"

# --- Edge case 3: SCHEDULE via f-string — must be SKIPPED ---
cron_hour = 8
SCHEDULE = f"0 {cron_hour} * * *"

# --- Edge case 4: DOMAIN_BACKUP — similar name but NOT a metadata key ---
DOMAIN_BACKUP = "logistics_backup"    # must NOT overwrite or appear as DOMAIN

# --- Edge case 5: OWNER inside a function — must be SKIPPED ---
def _configure():
    OWNER = "wrong-team"              # function-level; not module-level
    return OWNER

# --- Edge case 6: DOMAIN inside a class attribute — must be SKIPPED ---
class PipelineConfig:
    DOMAIN = "wrong_class_domain"     # class-level; not module-level

# --- Edge case 7: DOMAIN in a comment — must be SKIPPED ---
# DOMAIN = "comment_domain"

# --- Normal pipeline code below ---
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
vehicles = spark.table("frontier_bronze.fleet_vehicles")
vehicles.write.saveAsTable("frontier_silver.fleet_vehicles_curated")
