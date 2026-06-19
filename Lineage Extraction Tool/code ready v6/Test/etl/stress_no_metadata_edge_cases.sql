-- NO METADATA AT ALL
-- This file intentionally has no DOMAIN, SUBJECT_AREA, SCHEDULE, or OWNER.
-- Expected behaviour:
--   domain       = "unknown"
--   subject_area = "unknown"
--   schedule     = None
--   owner        = None
-- A warning "missing DOMAIN metadata" must be emitted.
-- The extractor must NOT crash — it must still produce a record.

-- Also tests: deep CTE nesting, self-join, correlated subquery,
--             multiple INSERT INTO targets in one file (semicolon split),
--             keyword trap: table named "values_table" contains the word VALUES
--             which is a SQL keyword — must still extract as a source table.

WITH base AS (
    SELECT a.id, a.val, a.grp
    FROM frontier_bronze.base_table a
),
recursive_like AS (
    -- Tests: CTE names "recursive_like", "dedup", "ranked" must NOT appear as source tables
    SELECT b.id, b.val, b.grp,
           ROW_NUMBER() OVER (PARTITION BY b.grp ORDER BY b.val DESC) AS rn
    FROM base b
),
dedup AS (
    SELECT r.id, r.val, r.grp
    FROM recursive_like r
    WHERE r.rn = 1
),
ranked AS (
    SELECT d.id, d.val, d.grp,
           RANK() OVER (ORDER BY d.val DESC) AS rnk
    FROM dedup d
),
-- Correlated subquery inside a CTE — tests that "inner_sub" is not mistaken
-- for a real table name
with_correlated AS (
    SELECT r.id, r.val, r.grp, r.rnk,
        (SELECT COUNT(*) FROM frontier_bronze.values_table v
         WHERE v.grp = r.grp) AS grp_count
    FROM ranked r
),
-- Self-join — "with_correlated" must NOT appear as a source table
self_joined AS (
    SELECT a.id, a.val, b.val AS peer_val
    FROM with_correlated a
    JOIN with_correlated b
      ON a.grp = b.grp AND a.id <> b.id
)

INSERT INTO frontier_silver.output_table_one
SELECT id, val, peer_val, grp_count
FROM self_joined sj
JOIN with_correlated wc ON sj.id = wc.id;

-- Second statement — separate INSERT; semicolon split must yield a second record
-- with its own source and target.
INSERT INTO frontier_silver.output_table_two
SELECT id, val FROM frontier_bronze.supplemental_source
WHERE val > (SELECT AVG(val) FROM frontier_bronze.supplemental_source);

-- Third statement — CREATE TABLE AS (target extraction via CREATE TABLE ... AS)
CREATE OR REPLACE TABLE frontier_gold.final_snapshot
AS
SELECT s.id, s.val, s.peer_val
FROM frontier_silver.output_table_one s
JOIN frontier_bronze.dimension_lookup dl ON s.id = dl.id;
