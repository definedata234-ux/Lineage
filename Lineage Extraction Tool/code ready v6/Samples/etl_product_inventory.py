# DOMAIN: supply_chain
# SUBJECT_AREA: inventory
# SCHEDULE: 0 4 * * *
# OWNER: data-engineering

"""
PySpark pipeline: Product Inventory Aggregation
Tests: spark.table(), spark.read.table(), saveAsTable(), withColumn(),
       select(), filter(), join(), groupBy(), agg()
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("ProductInventory").getOrCreate()

# Read source tables
inventory_df = spark.table("silver.product_inventory")
products_df  = spark.read.table("silver.products")
warehouses_df = spark.table("silver.warehouses")

# Filter active products
active_inventory = inventory_df.filter(
    F.col("is_active") == True
).filter(
    F.col("stock_quantity") > 0
)

# Join with product details
enriched = active_inventory.join(
    products_df,
    on="product_id",
    how="inner"
).join(
    warehouses_df,
    on="warehouse_id",
    how="left"
)

# Calculate inventory value
with_value = enriched.withColumn(
    "inventory_value",
    F.col("stock_quantity") * F.col("unit_cost")
).withColumn(
    "days_on_hand",
    F.datediff(F.current_date(), F.col("last_received_date"))
)

# Aggregate by product and warehouse
summary = with_value.groupBy(
    "product_id",
    "product_name",
    "product_category",
    "warehouse_id",
    "warehouse_name",
    "region"
).agg(
    F.sum("stock_quantity").alias("total_stock"),
    F.sum("inventory_value").alias("total_value"),
    F.avg("days_on_hand").alias("avg_days_on_hand"),
    F.max("last_received_date").alias("latest_receipt")
)

# Write to target
summary.write.mode("overwrite").saveAsTable("gold.product_inventory_summary")

# Also write low-stock alerts
low_stock = summary.filter(F.col("total_stock") < 100)
low_stock.write.mode("overwrite").insertInto("gold.low_stock_alerts")
