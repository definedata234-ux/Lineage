# DOMAIN: operations
# SUBJECT_AREA: network_health
# SCHEDULE: HOURLY
# OWNER: noc-analytics@corp.com

# =============================================================================
# STRESS TEST: Maximum LookML complexity
# Tests:
#   - Multiple views (6 views)
#   - sql_table_name extraction (4 direct tables)
#   - derived_table with inline SQL (2 derived tables — tables inside SQL must be extracted)
#   - dimension, dimension_group, measure of every type
#   - SELECT, JOIN, AGGREGATE all detected in derived_table SQL
#   - sql_trigger block (table reference must be extracted)
#   - Columns with names that look like SQL keywords (false-positive traps)
#   - Comments inside sql blocks that contain table names (must NOT extract)
# =============================================================================

view: network_incidents {
  sql_table_name: ops.fact_network_incidents ;;

  dimension: incident_id {
    primary_key: yes
    type: number
    sql: ${TABLE}.incident_id ;;
  }

  dimension: tower_id {
    type: number
    sql: ${TABLE}.tower_id ;;
  }

  dimension: region_id {
    type: number
    sql: ${TABLE}.region_id ;;
  }

  dimension: severity_level {
    type: string
    sql: ${TABLE}.severity ;;
  }

  dimension: incident_type {
    type: string
    sql: ${TABLE}.incident_type ;;
  }

  # Column named "select_flag" — must NOT trigger SELECT detection
  dimension: select_flag {
    type: yesno
    sql: ${TABLE}.is_selected ;;
  }

  # Column named "join_key" — must NOT trigger JOIN detection
  dimension: join_key {
    type: string
    sql: ${TABLE}.external_join_key ;;
  }

  dimension: resolution_minutes {
    type: number
    sql: ${TABLE}.resolution_time_mins ;;
  }

  dimension: is_sla_breach {
    type: yesno
    sql: ${TABLE}.resolution_time_mins > ${TABLE}.sla_threshold_mins ;;
  }

  dimension_group: opened {
    type: time
    timeframes: [time, date, week, month, quarter, year]
    sql: ${TABLE}.opened_at ;;
  }

  dimension_group: closed {
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.closed_at ;;
  }

  measure: incident_count {
    type: count
    drill_fields: [incident_id, tower_id, severity_level, opened_time]
  }

  measure: sla_breach_count {
    type: count
    filters: [is_sla_breach: "yes"]
  }

  measure: avg_resolution_time {
    type: average
    sql: ${resolution_minutes} ;;
  }

  measure: p95_resolution_time {
    type: percentile
    percentile: 95
    sql: ${resolution_minutes} ;;
  }

  measure: total_downtime_mins {
    type: sum
    sql: ${resolution_minutes} ;;
  }
}

view: cell_towers {
  sql_table_name: ops.dim_cell_towers ;;

  dimension: tower_id {
    primary_key: yes
    type: number
    sql: ${TABLE}.tower_id ;;
  }

  dimension: tower_name {
    type: string
    sql: ${TABLE}.tower_name ;;
  }

  dimension: technology_type {
    type: string
    sql: ${TABLE}.tech_type ;;  # 4G, 5G, LTE
  }

  dimension: vendor {
    type: string
    sql: ${TABLE}.equipment_vendor ;;
  }

  dimension: install_year {
    type: number
    sql: ${TABLE}.install_year ;;
  }

  measure: tower_count {
    type: count_distinct
    sql: ${tower_id} ;;
  }
}

view: geo_regions {
  sql_table_name: ops.dim_geo_regions ;;

  dimension: region_id {
    primary_key: yes
    type: number
    sql: ${TABLE}.region_id ;;
  }

  dimension: region_name {
    type: string
    sql: ${TABLE}.region_name ;;
  }

  dimension: country {
    type: string
    sql: ${TABLE}.country_code ;;
  }

  dimension: climate_zone {
    type: string
    sql: ${TABLE}.climate_zone ;;
  }
}

view: maintenance_schedule {
  sql_table_name: ops.fact_maintenance_schedule ;;

  dimension: schedule_id {
    primary_key: yes
    type: number
    sql: ${TABLE}.schedule_id ;;
  }

  dimension: tower_id {
    type: number
    sql: ${TABLE}.tower_id ;;
  }

  dimension: maintenance_type {
    type: string
    sql: ${TABLE}.maint_type ;;
  }

  dimension_group: scheduled {
    type: time
    timeframes: [date, week, month]
    sql: ${TABLE}.scheduled_date ;;
  }

  measure: scheduled_count {
    type: count
  }
}

# =============================================================================
# DERIVED TABLE 1 — inline SQL with JOIN, GROUP BY, window function
# Tests: tables inside the sql block are extracted as sources
#        SELECT, JOIN, AGGREGATE operations detected
# =============================================================================
view: tower_incident_summary {
  derived_table: {
    persist_for: "1 hour"
    sql:
      SELECT
          t.tower_id,
          t.tower_name,
          t.tech_type,
          r.region_name,
          r.country_code,
          COUNT(i.incident_id)                                          AS total_incidents,
          SUM(CASE WHEN i.severity = 'P1' THEN 1 ELSE 0 END)           AS p1_count,
          AVG(i.resolution_time_mins)                                   AS avg_resolution,
          MAX(i.resolution_time_mins)                                   AS max_resolution,
          RANK() OVER (PARTITION BY r.region_name ORDER BY COUNT(i.incident_id) DESC) AS region_rank
      FROM ops.fact_network_incidents i
      JOIN ops.dim_cell_towers t         ON i.tower_id = t.tower_id
      JOIN ops.dim_geo_regions r         ON i.region_id = r.region_id
      LEFT JOIN ops.fact_maintenance_schedule m ON t.tower_id = m.tower_id
      GROUP BY t.tower_id, t.tower_name, t.tech_type, r.region_name, r.country_code
    ;;
  }

  dimension: tower_id        { type: number  sql: ${TABLE}.tower_id ;;       }
  dimension: tower_name      { type: string  sql: ${TABLE}.tower_name ;;     }
  dimension: region_name     { type: string  sql: ${TABLE}.region_name ;;    }
  dimension: country_code    { type: string  sql: ${TABLE}.country_code ;;   }
  dimension: tech_type       { type: string  sql: ${TABLE}.tech_type ;;      }
  dimension: region_rank     { type: number  sql: ${TABLE}.region_rank ;;    }

  measure: total_incidents   { type: sum     sql: ${TABLE}.total_incidents ;; }
  measure: p1_incidents      { type: sum     sql: ${TABLE}.p1_count ;;        }
  measure: avg_resolution    { type: average sql: ${TABLE}.avg_resolution ;;  }
}

# =============================================================================
# DERIVED TABLE 2 — uses a subquery referencing another derived table equivalent
# and a sql_trigger that also references a real table
# =============================================================================
view: sla_breach_rollup {
  derived_table: {
    max_cache_age: "2 hours"
    datagroup: sla_rollup_datagroup
    sql:
      WITH breach_base AS (
          SELECT
              i.region_id,
              i.incident_type,
              COUNT(*) AS total_incidents,
              SUM(CASE WHEN i.resolution_time_mins > i.sla_threshold_mins THEN 1 ELSE 0 END) AS breaches,
              AVG(i.resolution_time_mins) AS avg_mins
          FROM ops.fact_network_incidents i
          GROUP BY i.region_id, i.incident_type
      ),
      enriched AS (
          SELECT b.*, r.region_name, r.country_code
          FROM breach_base b
          JOIN ops.dim_geo_regions r ON b.region_id = r.region_id
      )
      SELECT * FROM enriched
      WHERE total_incidents > 5
    ;;
  }

  dimension: region_id       { type: number  sql: ${TABLE}.region_id ;;      }
  dimension: incident_type   { type: string  sql: ${TABLE}.incident_type ;;  }
  dimension: region_name     { type: string  sql: ${TABLE}.region_name ;;    }

  measure: breach_rate {
    type: number
    sql: NULLIF(${TABLE}.breaches, 0) * 1.0 / NULLIF(${TABLE}.total_incidents, 0) ;;
  }
}

datagroup: sla_rollup_datagroup {
  max_cache_age: "2 hours"
  sql_trigger: SELECT MAX(opened_at) FROM ops.fact_network_incidents ;;
}
