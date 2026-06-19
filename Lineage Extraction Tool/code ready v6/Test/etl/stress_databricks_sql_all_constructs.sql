-- DOMAIN: finance
-- SUBJECT_AREA: stress_test
-- SCHEDULE: 0 1 * * *
-- OWNER: migration-test-team

-- =============================================================================
-- STRESS TEST: Every Databricks SQL construct in one file
-- Tests: All 40+ construct detections, CTE alias exclusion, multi-target DML,
--        deeply nested subqueries, keyword false-positive traps, qualified names
-- =============================================================================

-- ── 1. LATERAL VIEW + EXPLODE (rewrite) ──────────────────────────────────────
-- Parser must detect LATERAL VIEW and EXPLODE separately.
-- "exploded_tag" must NOT appear as a source table.
WITH raw_events AS (
    SELECT
        e.event_id,
        e.user_id,
        e.event_date,
        e.tags_array
    FROM frontier_bronze.clickstream_events e
    LATERAL VIEW EXPLODE(e.tags_array) tag_table AS exploded_tag
    WHERE e.event_date >= DATE_SUB(CURRENT_DATE(), 90)
),

-- ── 2. POSEXPLODE (rewrite) ───────────────────────────────────────────────────
-- Separate from EXPLODE; tests that both constructs are independently detected.
positional_tags AS (
    SELECT
        e.event_id,
        pos,
        tag_val
    FROM frontier_bronze.clickstream_events e
    LATERAL VIEW POSEXPLODE(e.tags_array) t AS pos, tag_val
),

-- ── 3. COLLECT_LIST / COLLECT_SET (rewrite) ───────────────────────────────────
-- Tests aggregate collection functions.
user_tag_aggregates AS (
    SELECT
        re.user_id,
        COLLECT_LIST(re.exploded_tag)  AS all_tags,
        COLLECT_SET(re.exploded_tag)   AS unique_tags,
        ANY_VALUE(re.event_date)       AS sample_date,
        APPROX_COUNT_DISTINCT(re.exploded_tag) AS approx_unique_tag_count
    FROM raw_events re
    GROUP BY re.user_id
),

-- ── 4. MAP / ARRAY / STRUCT constructors (rewrite) ───────────────────────────
-- Tests complex type constructors. The trailing '(' is required for detection.
-- Also traps: a column called "map_id" must NOT trigger MAP detection.
type_constructor_test AS (
    SELECT
        u.user_id,
        MAP('clicks', CAST(u.approx_unique_tag_count AS STRING), 'tags', SIZE(u.all_tags)) AS metrics_map,
        ARRAY(u.approx_unique_tag_count, SIZE(u.unique_tags))                              AS counts_array,
        STRUCT(u.user_id, u.sample_date, SIZE(u.all_tags))                                AS user_struct,
        u.sample_date
    FROM user_tag_aggregates u
),

-- ── 5. TRANSFORM / FILTER / AGGREGATE higher-order functions (rewrite) ────────
higher_order_test AS (
    SELECT
        t.user_id,
        TRANSFORM(t.counts_array, x -> x * 2)         AS doubled_counts,
        FILTER(t.counts_array, x -> x > 0)            AS positive_counts,
        AGGREGATE(t.counts_array, 0, (acc, x) -> acc + x) AS summed_counts
    FROM type_constructor_test t
),

-- ── 6. STACK (rewrite) ────────────────────────────────────────────────────────
stacked_metrics AS (
    SELECT
        s.user_id,
        metric_name,
        metric_value
    FROM higher_order_test s
    LATERAL VIEW STACK(2,
        'doubled_total', s.summed_counts,
        'positive_total', SIZE(s.positive_counts)
    ) metrics_table AS metric_name, metric_value
),

-- ── 7. PIVOT / UNPIVOT (rewrite) ──────────────────────────────────────────────
-- PIVOT requires a subquery; tests detection inside nested scope.
pivoted_metrics AS (
    SELECT *
    FROM (
        SELECT user_id, metric_name, metric_value
        FROM stacked_metrics
    ) src
    PIVOT (
        SUM(metric_value)
        FOR metric_name IN ('doubled_total', 'positive_total')
    )
),

-- ── 8. Window functions: ROW_NUMBER, RANK, DENSE_RANK, NTILE,
--       PERCENT_RANK, LAG, LEAD (direct) ────────────────────────────────────
window_ranked AS (
    SELECT
        p.user_id,
        ROW_NUMBER()   OVER (ORDER BY p.doubled_total DESC NULLS LAST)   AS rn,
        RANK()         OVER (ORDER BY p.doubled_total DESC)               AS rnk,
        DENSE_RANK()   OVER (ORDER BY p.doubled_total DESC)               AS dense_rnk,
        NTILE(10)      OVER (ORDER BY p.doubled_total DESC)               AS decile,
        PERCENT_RANK() OVER (ORDER BY p.doubled_total DESC)               AS pct_rank,
        LAG(p.doubled_total,  1, 0) OVER (ORDER BY p.doubled_total DESC)  AS prev_val,
        LEAD(p.doubled_total, 1, 0) OVER (ORDER BY p.doubled_total DESC)  AS next_val,
        p.doubled_total,
        p.positive_total
    FROM pivoted_metrics p
),

-- ── 9. QUALIFY (manual) ───────────────────────────────────────────────────────
-- The hardest construct — no BigQuery equivalent without a subquery rewrite.
-- Tests that QUALIFY is detected even when trailing a complex WHERE.
qualify_filtered AS (
    SELECT *
    FROM window_ranked
    WHERE doubled_total > 0
    QUALIFY rn <= 1000
),

-- ── 10. DATE functions: DATE_SUB, DATE_ADD, DATEDIFF, DATE_TRUNC (direct) ────
date_enriched AS (
    SELECT
        q.user_id,
        q.rn,
        q.rnk,
        q.dense_rnk,
        q.decile,
        q.pct_rank,
        CURRENT_DATE()                               AS today,
        DATE_SUB(CURRENT_DATE(), 7)                  AS week_ago,
        DATE_ADD(CURRENT_DATE(), 30)                 AS month_ahead,
        DATEDIFF(DATE_ADD(CURRENT_DATE(), 30),
                 DATE_SUB(CURRENT_DATE(), 7))        AS day_span,
        DATE_TRUNC('MONTH', CURRENT_DATE())          AS month_start
    FROM qualify_filtered q
),

-- ── 11. TRY_CAST (direct) ─────────────────────────────────────────────────────
-- Tests type-safe cast detection. Also traps CAST(...) which is NOT a construct.
cast_test AS (
    SELECT
        d.user_id,
        TRY_CAST(d.rn AS DOUBLE)                         AS rn_double,
        TRY_CAST('not_a_number' AS BIGINT)               AS safe_null,
        CAST(d.dense_rnk AS STRING)                      AS rank_str  -- CAST alone is not a construct
    FROM date_enriched d
),

-- ── 12. INLINE (rewrite) ──────────────────────────────────────────────────────
inline_test AS (
    SELECT t.user_id, col1, col2
    FROM cast_test t
    LATERAL VIEW INLINE(ARRAY(STRUCT(1, 'a'), STRUCT(2, 'b'))) tbl AS col1, col2
),

-- ── 13. Delta Lake: MERGE INTO (manual) ───────────────────────────────────────
-- Must detect MERGE INTO as a target AND as a construct.
-- Parser must NOT double-count "frontier_gold.user_event_metrics" as both
-- a MERGE INTO target and a FROM source.
merge_placeholder AS (
    SELECT i.user_id, i.rn_double, i.safe_null
    FROM inline_test i
)

-- Final INSERT — primary target table
INSERT INTO frontier_gold.user_event_metrics
SELECT
    mp.user_id,
    mp.rn_double,
    mp.safe_null,
    CURRENT_TIMESTAMP() AS load_ts
FROM merge_placeholder mp;

-- ── 14. MERGE INTO as a separate DML statement ────────────────────────────────
MERGE INTO frontier_gold.user_event_metrics tgt
USING (
    SELECT user_id, rn_double FROM merge_placeholder
) src
ON tgt.user_id = src.user_id
WHEN MATCHED THEN UPDATE SET tgt.rn_double = src.rn_double
WHEN NOT MATCHED THEN INSERT (user_id, rn_double) VALUES (src.user_id, src.rn_double);

-- ── 15. Delta Lake DDL constructs (manual) ────────────────────────────────────
-- USING DELTA, CONVERT TO DELTA, RESTORE TABLE, VACUUM, OPTIMIZE
CREATE TABLE IF NOT EXISTS frontier_gold.user_event_archive
USING DELTA
AS SELECT * FROM frontier_gold.user_event_metrics WHERE 1=0;

CONVERT TO DELTA frontier_bronze.legacy_clickstream_events;

RESTORE TABLE frontier_gold.user_event_metrics TO VERSION AS OF 5;

VACUUM frontier_gold.user_event_metrics RETAIN 168 HOURS;

OPTIMIZE frontier_gold.user_event_metrics ZORDER BY (user_id);

-- ── 16. CREATE TABLE AS (target extraction) ───────────────────────────────────
CREATE OR REPLACE TABLE frontier_gold.user_risk_snapshot
AS
SELECT * FROM frontier_gold.user_event_metrics LIMIT 0;

-- ── 17. Keyword false-positive traps ──────────────────────────────────────────
-- Columns named after keywords must NOT be detected as constructs.
-- Tables with keyword-like names should still extract correctly.
SELECT
    filter_id,          -- column named "filter_id" — must NOT trigger FILTER
    map_key,            -- column named "map_key"  — must NOT trigger MAP
    array_index,        -- column named "array_index" — must NOT trigger ARRAY
    transform_type,     -- column named "transform_type" — must NOT trigger TRANSFORM
    aggregate_id        -- column named "aggregate_id" — must NOT trigger AGGREGATE
FROM frontier_bronze.keyword_trap_table
WHERE filter_id > 0
  AND map_key IS NOT NULL;
