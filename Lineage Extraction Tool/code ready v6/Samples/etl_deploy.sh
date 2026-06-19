#!/bin/bash
# DOMAIN: platform
# SUBJECT_AREA: data_ops
# OWNER: platform-engineering

# Shell script with embedded SQL
# Tests: SQL inside heredoc and sqlplus/bq calls

echo "Starting ETL deployment pipeline..."

# Run BigQuery SQL via bq CLI
bq query --use_legacy_sql=false --project_id=my-project << 'BQSQL'
INSERT INTO `my-project.gold.daily_metrics`
SELECT
    m.metric_date,
    m.pipeline_name,
    m.records_processed,
    m.records_failed,
    m.processing_time_secs,
    e.environment,
    e.cluster_name
FROM `my-project.silver.pipeline_metrics` m
JOIN `my-project.silver.environments` e
    ON m.environment_id = e.environment_id
WHERE m.metric_date = CURRENT_DATE()
BQSQL

# Run PostgreSQL refresh
psql -h localhost -U datauser -d warehouse -c "
    INSERT INTO gold.sla_monitoring
    SELECT
        pipeline_name,
        metric_date,
        processing_time_secs,
        CASE WHEN processing_time_secs > 3600 THEN 'BREACH'
             WHEN processing_time_secs > 1800 THEN 'WARNING'
             ELSE 'OK' END AS sla_status
    FROM gold.daily_metrics
    WHERE metric_date = CURRENT_DATE;
"

echo "ETL deployment complete."
