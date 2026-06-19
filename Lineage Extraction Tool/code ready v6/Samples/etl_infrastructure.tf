# DOMAIN: platform
# SUBJECT_AREA: infrastructure
# OWNER: platform-engineering

# Terraform HCL with embedded SQL
# Tests: SQL inside terraform template/provisioner blocks

resource "google_bigquery_table" "customer_summary" {
  dataset_id = "gold"
  table_id   = "customer_order_summary"
  project    = "my-gcp-project"

  schema = jsonencode([
    { name = "customer_id",      type = "STRING"  },
    { name = "customer_name",    type = "STRING"  },
    { name = "region",           type = "STRING"  },
    { name = "total_revenue",    type = "FLOAT64" },
    { name = "total_orders",     type = "INT64"   }
  ])
}

resource "google_bigquery_job" "load_customer_summary" {
  job_id  = "load_customer_summary"
  project = "my-gcp-project"

  query {
    query = <<-SQL
      INSERT INTO `my-gcp-project.gold.customer_order_summary`
      SELECT
          c.customer_id,
          c.customer_name,
          c.region,
          c.customer_segment,
          SUM(o.order_total)    AS total_revenue,
          COUNT(o.order_id)     AS total_orders,
          MAX(o.order_date)     AS last_order_date
      FROM `my-gcp-project.silver.customers` c
      JOIN `my-gcp-project.silver.orders` o
          ON c.customer_id = o.customer_id
      WHERE o.status = 'completed'
      GROUP BY c.customer_id, c.customer_name, c.region, c.customer_segment
    SQL
    use_legacy_sql = false
  }
}
