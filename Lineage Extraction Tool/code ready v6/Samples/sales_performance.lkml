# DOMAIN: retail
# SUBJECT_AREA: sales_performance
# OWNER: bi-team

# Sales Performance LookML View
# Tests: LookML view/dimension/measure extraction, SQL table references

view: sales_performance {
  sql_table_name: gold.customer_order_summary ;;

  dimension: customer_id {
    type: string
    sql: ${TABLE}.customer_id ;;
  }

  dimension: customer_name {
    type: string
    sql: ${TABLE}.customer_name ;;
  }

  dimension: region {
    type: string
    sql: ${TABLE}.region ;;
  }

  dimension: customer_segment {
    type: string
    sql: ${TABLE}.customer_segment ;;
  }

  dimension_group: last_order {
    type: time
    timeframes: [date, week, month, quarter, year]
    sql: ${TABLE}.last_order_date ;;
  }

  measure: total_customers {
    type: count
    sql: ${TABLE}.customer_id ;;
  }

  measure: total_revenue {
    type: sum
    sql: ${TABLE}.total_revenue ;;
    value_format_name: usd
  }

  measure: avg_order_value {
    type: average
    sql: ${TABLE}.avg_unit_price ;;
    value_format_name: usd
  }

  measure: total_orders {
    type: sum
    sql: ${TABLE}.total_orders ;;
  }
}

view: high_value_customers {
  sql_table_name: gold.high_value_customers ;;

  derived_table: {
    sql:
      SELECT
        hvc.customer_id,
        hvc.customer_name,
        hvc.region,
        hvc.total_revenue,
        cos.total_orders,
        cos.last_order_date
      FROM gold.high_value_customers hvc
      JOIN gold.customer_order_summary cos
        ON hvc.customer_id = cos.customer_id
      WHERE hvc.total_revenue >= 10000
    ;;
  }

  dimension: customer_id {
    type: string
    sql: ${TABLE}.customer_id ;;
  }

  measure: count_high_value {
    type: count
    sql: ${TABLE}.customer_id ;;
  }

  measure: total_high_value_revenue {
    type: sum
    sql: ${TABLE}.total_revenue ;;
  }
}
