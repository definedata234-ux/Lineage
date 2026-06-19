# DOMAIN: SALES
# SUBJECT_AREA: ANALYTICS
# SCHEDULE: DAILY
# OWNER: analytics@verizon.com

view: orders {
  sql_table_name: enterprise_sales.fact_orders ;;
  label: "Orders"

  dimension: order_id {
    primary_key: yes
    type: number
    sql: ${TABLE}.order_id ;;
  }

  dimension: customer_id {
    type: number
    sql: ${TABLE}.customer_id ;;
  }

  dimension: order_date {
    type: date
    sql: ${TABLE}.order_date ;;
  }

  dimension: order_month {
    type: date_month
    sql: ${order_date} ;;
  }

  dimension: revenue_amount {
    type: number
    sql: ${TABLE}.total_amount ;;
    value_format_name: usd
  }

  dimension: order_status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: is_completed {
    type: yesno
    sql: ${order_status} = 'COMPLETED' ;;
  }

  dimension_group: created {
    type: time
    timeframes: [time, date, week, month, quarter, year]
    sql: ${TABLE}.created_at ;;
  }

  measure: total_revenue {
    type: sum
    sql: ${revenue_amount} ;;
    value_format_name: usd
    drill_fields: [order_id, customer_id, order_date]
  }

  measure: average_order_value {
    type: average
    sql: ${revenue_amount} ;;
    value_format_name: usd
  }

  measure: order_count {
    type: count
    drill_fields: [order_id, customer_id]
  }

  measure: completed_orders {
    type: count
    filters: [is_completed: "yes"]
  }

  measure: revenue_per_order {
    type: number
    sql: ${total_revenue} / NULLIF(${order_count}, 0) ;;
    value_format_name: usd
  }
}

view: customers {
  sql_table_name: enterprise_sales.dim_customers ;;
  label: "Customers"

  dimension: customer_key {
    primary_key: yes
    type: number
    sql: ${TABLE}.customer_key ;;
  }

  dimension: customer_id {
    type: number
    sql: ${TABLE}.customer_id ;;
  }

  dimension: customer_name {
    type: string
    sql: ${TABLE}.customer_name ;;
  }

  dimension: customer_segment {
    type: string
    sql: ${TABLE}.segment ;;
  }

  dimension: tier_level {
    type: string
    sql: ${TABLE}.tier ;;
  }

  dimension: signup_date {
    type: date
    sql: ${TABLE}.signup_date ;;
  }

  dimension: account_manager_id {
    type: number
    sql: ${TABLE}.account_manager_id ;;
  }

  dimension: days_as_customer {
    type: number
    sql: DATEDIFF(day, ${signup_date}, CURRENT_DATE()) ;;
  }

  measure: customer_count {
    type: count
    drill_fields: [customer_key, customer_name, customer_segment]
  }

  measure: active_customers {
    type: count
    filters: [days_as_customer: ">30"]
  }
}

view: order_items {
  sql_table_name: enterprise_sales.fact_order_items ;;
  label: "Order Items"

  dimension: item_id {
    primary_key: yes
    type: number
    sql: ${TABLE}.item_id ;;
  }

  dimension: order_id {
    type: number
    sql: ${TABLE}.order_id ;;
  }

  dimension: product_id {
    type: number
    sql: ${TABLE}.product_id ;;
  }

  dimension: quantity {
    type: number
    sql: ${TABLE}.quantity ;;
  }

  dimension: unit_price {
    type: number
    sql: ${TABLE}.unit_price ;;
    value_format_name: usd
  }

  dimension: line_total {
    type: number
    sql: ${quantity} * ${unit_price} ;;
    value_format_name: usd
  }

  measure: total_items_sold {
    type: sum
    sql: ${quantity} ;;
  }

  measure: total_revenue {
    type: sum
    sql: ${line_total} ;;
    value_format_name: usd
  }

  measure: avg_unit_price {
    type: average
    sql: ${unit_price} ;;
    value_format_name: usd
  }
}

view: products {
  sql_table_name: enterprise_sales.dim_products ;;
  label: "Products"

  dimension: product_key {
    primary_key: yes
    type: number
    sql: ${TABLE}.product_key ;;
  }

  dimension: product_id {
    type: number
    sql: ${TABLE}.product_id ;;
  }

  dimension: product_name {
    type: string
    sql: ${TABLE}.product_name ;;
  }

  dimension: product_category {
    type: string
    sql: ${TABLE}.category ;;
  }

  dimension: product_line {
    type: string
    sql: ${TABLE}.product_line ;;
  }

  dimension: business_unit {
    type: string
    sql: ${TABLE}.business_unit ;;
  }

  dimension: base_price {
    type: number
    sql: ${TABLE}.base_price ;;
    value_format_name: usd
  }

  measure: product_count {
    type: count
    drill_fields: [product_key, product_name, product_category]
  }
}

view: revenue_summary {
  derived_table: {
    persist_for: "24 hours"
    datagroup: revenue_summary_datagroup
    sql: SELECT
        o.order_date,
        c.customer_segment,
        c.tier_level,
        p.product_category,
        p.business_unit,
        COUNT(DISTINCT o.order_id) as order_count,
        COUNT(DISTINCT o.customer_id) as customer_count,
        SUM(oi.quantity) as items_sold,
        SUM(oi.line_total) as revenue_amount,
        AVG(oi.unit_price) as avg_unit_price
    FROM enterprise_sales.fact_orders o
    LEFT JOIN enterprise_sales.dim_customers c ON o.customer_id = c.customer_id
    LEFT JOIN enterprise_sales.fact_order_items oi ON o.order_id = oi.order_id
    LEFT JOIN enterprise_sales.dim_products p ON oi.product_id = p.product_id
    WHERE o.order_date >= DATEADD(month, -3, GETDATE())
      AND o.status = 'COMPLETED'
    GROUP BY
        o.order_date,
        c.customer_segment,
        c.tier_level,
        p.product_category,
        p.business_unit
    ;;
  }

  dimension: order_date {
    type: date
    sql: ${TABLE}.order_date ;;
  }

  dimension: customer_segment {
    type: string
    sql: ${TABLE}.customer_segment ;;
  }

  dimension: tier_level {
    type: string
    sql: ${TABLE}.tier_level ;;
  }

  dimension: product_category {
    type: string
    sql: ${TABLE}.product_category ;;
  }

  dimension: business_unit {
    type: string
    sql: ${TABLE}.business_unit ;;
  }

  measure: total_revenue {
    type: sum
    sql: ${TABLE}.revenue_amount ;;
    value_format_name: usd
  }

  measure: total_orders {
    type: sum
    sql: ${TABLE}.order_count ;;
  }

  measure: total_customers {
    type: sum
    sql: ${TABLE}.customer_count ;;
  }

  measure: total_items {
    type: sum
    sql: ${TABLE}.items_sold ;;
  }
}

datagroup: revenue_summary_datagroup {
  max_cache_age: "24 hours"
  sql_trigger: SELECT MAX(order_date) FROM enterprise_sales.fact_orders ;;
}
