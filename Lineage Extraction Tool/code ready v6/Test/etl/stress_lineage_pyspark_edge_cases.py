"""
Lineage Extractor Stress Test — PySpark

No DOMAIN or SUBJECT_AREA declared at module level.
Expected: domain="unknown", subject_area="unknown", warning emitted.

Tests for lineage_extractor.py specifically:
  - Multiple spark.sql() calls, each producing its own LineageRecord row
  - spark.sql() with a multi-line triple-quoted string
  - spark.sql() with a string variable (non-literal) — must be SKIPPED
  - spark.table() / saveAsTable() producing ONE PySpark lineage row
  - insertInto() also captured as a target
  - Nested function calls that look like spark.sql but aren't
  - A class with a spark.sql inside a method — must be ignored (not module-level)
"""

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

# ── Source reads ──────────────────────────────────────────────────────────────
orders    = spark.table("frontier_bronze.orders")
customers = spark.table("frontier_bronze.customers")
products  = spark.table("frontier_bronze.products")

joined = orders.join(customers, "customer_id", "left") \
               .join(products, "product_id", "inner")

# ── Target writes ─────────────────────────────────────────────────────────────
joined.write.saveAsTable("frontier_silver.orders_enriched")
joined.write.insertInto("frontier_silver.orders_enriched_v2")

# ── spark.sql() call 1 — simple single-line string ───────────────────────────
spark.sql("INSERT INTO frontier_silver.sql_target_one SELECT id, val FROM frontier_bronze.sql_source_one")

# ── spark.sql() call 2 — multi-line triple-quoted ────────────────────────────
spark.sql("""
    INSERT INTO frontier_silver.sql_target_two
    SELECT
        o.order_id,
        c.customer_name,
        p.product_name
    FROM frontier_bronze.sql_source_two o
    JOIN frontier_bronze.sql_customers c ON o.customer_id = c.id
    JOIN frontier_bronze.sql_products p  ON o.product_id = p.id
    WHERE o.status = 'completed'
""")

# ── spark.sql() call 3 — CTE inside spark.sql ────────────────────────────────
spark.sql("""
    WITH agg AS (
        SELECT region_id, SUM(amount) AS total
        FROM frontier_bronze.sql_source_three
        GROUP BY region_id
    )
    INSERT INTO frontier_gold.sql_target_three
    SELECT * FROM agg WHERE total > 1000
""")

# ── spark.sql() with a non-literal variable — must be SKIPPED ────────────────
# The parser uses AST and only handles ast.Constant string nodes.
# A variable reference (ast.Name) cannot be resolved statically.
dynamic_query = f"SELECT * FROM frontier_bronze.dynamic_source"
spark.sql(dynamic_query)   # <-- should produce no LineageRecord

# ── spark.sql() inside a class method — must be IGNORED by lineage extractor ─
# The lineage extractor only processes module-level spark.sql calls (via
# ast.iter_child_nodes on the module body), not calls inside class methods.
class _Internal:
    def run(self):
        spark.sql("INSERT INTO should_not_appear SELECT 1")

# ── A call that looks like spark.sql but is not ───────────────────────────────
# Tests that only spark.sql(...) is matched, not other_obj.sql(...)
class FakeSpark:
    def sql(self, q): pass

other = FakeSpark()
other.sql("INSERT INTO fake_target SELECT * FROM fake_source")  # must be ignored
