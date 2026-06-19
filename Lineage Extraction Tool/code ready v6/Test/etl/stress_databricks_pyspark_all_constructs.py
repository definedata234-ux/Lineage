"""
PySpark Stress Test — All Databricks PySpark Constructs

Tests every AST detection path in databricks_pyspark_parser.py:
  - pandas_udf as decorator (two forms) AND as a bare function call
  - udf() as a function call AND spark.udf.register()
  - Window.partitionBy / orderBy / rowsBetween / rangeBetween + .over()
  - format("delta"), DeltaTable.forPath(), DeltaTable.forName(), .merge()
  - format("parquet"), format("csv"), format("json"), format("orc")
  - repartition, coalesce, partitionBy, bucketBy, sortBy
  - broadcast()
  - spark.table() and spark.read.table() for source extraction
  - saveAsTable() and insertInto() for target extraction
  - Deeply chained calls that could fool shallow AST walkers
  - Columns/variables named after constructs (false-positive traps)
"""

# Pipeline metadata
DOMAIN = "network"
SUBJECT_AREA = "stress_test"
SCHEDULE = "0 4 * * *"
OWNER = "platform-migration-team"

import ast  # stdlib import — must NOT confuse metadata extractor

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.functions import broadcast, col, pandas_udf, udf
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType
)
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("stress_test").getOrCreate()

# =============================================================================
# 1. SOURCES — spark.table() and spark.read.table() with qualified names
# =============================================================================
# Parser must extract: db="frontier_bronze", table each bare name below.
# All six qualified names must split correctly.

events       = spark.table("frontier_bronze.network_events")
devices      = spark.table("frontier_bronze.network_devices")
towers       = spark.read.table("frontier_bronze.cell_towers")
regions      = spark.read.table("frontier_bronze.geo_regions")
# Unqualified table — db should be empty string, table = "lookup_codes"
lookup_codes = spark.table("lookup_codes")

# =============================================================================
# 2. pandas_udf — FORM 1: bare decorator @pandas_udf
# =============================================================================
@pandas_udf(DoubleType())
def normalise_signal(series):
    return (series - series.mean()) / series.std()

# =============================================================================
# 3. pandas_udf — FORM 2: decorator with explicit return type call
# =============================================================================
@pandas_udf(returnType=DoubleType())
def clip_signal(series):
    return series.clip(-3.0, 3.0)

# =============================================================================
# 4. pandas_udf — FORM 3: called as a function (not a decorator)
# =============================================================================
# This is the non-decorator form: pandas_udf(fn, returnType).
# The AST walker must catch this ast.Call node separately.
encode_region = pandas_udf(lambda s: s.str.lower(), StringType())

# =============================================================================
# 5. udf() — bare function call
# =============================================================================
@udf(returnType=StringType())
def classify_event(event_type):
    if event_type in ("DROP", "FAIL"):
        return "critical"
    return "normal"

# =============================================================================
# 6. spark.udf.register() — attribute chain
# =============================================================================
spark.udf.register("classify_event_sql", classify_event)

# =============================================================================
# 7. Window operations — partitionBy, orderBy, rowsBetween, rangeBetween + .over()
# =============================================================================
win_partition   = Window.partitionBy("tower_id").orderBy("event_ts")
win_rows        = Window.partitionBy("tower_id").rowsBetween(-6, 0)
win_range       = Window.partitionBy("region_id").rangeBetween(
                      Window.unboundedPreceding, Window.currentRow
                  )

enriched = events.withColumn("rn",          F.row_number().over(win_partition)) \
                 .withColumn("rolling_avg",  F.avg("signal_strength").over(win_rows)) \
                 .withColumn("cum_sum",      F.sum("packet_loss").over(win_range))  \
                 .withColumn("norm_signal",  normalise_signal(col("signal_strength"))) \
                 .withColumn("clipped",      clip_signal(col("signal_strength"))) \
                 .withColumn("severity",     classify_event(col("event_type")))

# =============================================================================
# 8. broadcast() join
# =============================================================================
with_region = enriched.join(
    broadcast(regions),
    enriched.region_id == regions.region_id,
    "left"
)

with_tower = with_region.join(
    broadcast(towers),
    with_region.tower_id == towers.tower_id,
    "inner"
)

# =============================================================================
# 9. repartition / coalesce / partitionBy / sortBy / bucketBy
# =============================================================================
# repartition and coalesce
repart   = with_tower.repartition(200, "region_id")
coalesced = repart.coalesce(50)

# Write path: partitionBy + sortBy + bucketBy
# These are DataFrameWriter methods, distinct from Window.partitionBy above.
(
    coalesced.write
    .format("parquet")
    .partitionBy("region_id", "event_date")
    .sortBy("event_ts")
    .bucketBy(64, "tower_id")
    .saveAsTable("frontier_silver.network_events_partitioned")
)

# =============================================================================
# 10. format() variants — delta, csv, json, orc
# =============================================================================
# format("delta")
(
    coalesced.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("frontier_silver.network_events_delta")
)

# format("csv")
(
    coalesced.write
    .format("csv")
    .option("header", "true")
    .saveAsTable("frontier_silver.network_events_csv")
)

# format("json")
(
    coalesced.write
    .format("json")
    .saveAsTable("frontier_silver.network_events_json")
)

# format("orc")
(
    coalesced.write
    .format("orc")
    .saveAsTable("frontier_silver.network_events_orc")
)

# =============================================================================
# 11. DeltaTable.forPath() and .merge()
# =============================================================================
delta_tbl = DeltaTable.forPath(spark, "/mnt/delta/network_events_delta")

(
    delta_tbl.alias("tgt")
    .merge(
        coalesced.alias("src"),
        "tgt.event_id = src.event_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# =============================================================================
# 12. DeltaTable.forName() (second DeltaTable detection path)
# =============================================================================
delta_by_name = DeltaTable.forName(spark, "frontier_silver.network_events_delta")

# =============================================================================
# 13. insertInto() target
# =============================================================================
coalesced.write.insertInto("frontier_gold.network_event_summary")

# =============================================================================
# 14. Embedded spark.sql() with its OWN source/target
#     (tests that SQL sources/targets do NOT bleed into PySpark source_tables)
# =============================================================================
spark.sql("""
    INSERT INTO frontier_gold.network_exec_dashboard
    SELECT tower_id, COUNT(*) AS event_count
    FROM frontier_silver.network_events_delta
    GROUP BY tower_id
""")

# =============================================================================
# 15. False-positive variable name traps
#     Variables named after constructs must NOT trigger parser detections.
# =============================================================================
# "format" is a Python builtin — only .format("...") method calls should match
file_format   = "parquet"                  # variable named format-ish
window_size   = 7                          # variable named "window_size"
over_threshold = 0.95                      # variable named "over_threshold"
merge_key     = "event_id"                 # variable named "merge_key"
udf_name      = "classify_event_sql"       # variable named "udf_name"
coalesce_val  = None                       # variable named "coalesce_val"
partition_col = "region_id"                # variable named "partition_col"

# =============================================================================
# 16. Chained method call — deeply nested, tests that every .over() fires
# =============================================================================
deep_chain = (
    spark.table("frontier_bronze.deep_chain_source")
    .withColumn("w1", F.rank().over(
        Window.partitionBy("a").orderBy("b")
    ))
    .withColumn("w2", F.dense_rank().over(
        Window.partitionBy("c").rowsBetween(-3, 3)
    ))
    .repartition(100)
    .coalesce(10)
    .write
    .format("delta")
    .partitionBy("a")
    .sortBy("b")
    .saveAsTable("frontier_gold.deep_chain_output")
)
