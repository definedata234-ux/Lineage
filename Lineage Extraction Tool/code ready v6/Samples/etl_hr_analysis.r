# DOMAIN: hr
# SUBJECT_AREA: workforce_analytics
# OWNER: analytics-team

# R Pipeline: HR Workforce Analytics
# Tests: R with embedded SQL via DBI/RODBC

library(DBI)
library(dplyr)

# Connect to data warehouse
con <- dbConnect(odbc::odbc(), dsn = "DataWarehouse")

# Extract HR data using SQL
headcount_data <- dbGetQuery(con, "
  SELECT
      e.employee_id,
      e.employee_name,
      e.department,
      e.job_title,
      e.hire_date,
      e.salary,
      e.performance_rating,
      e.manager_id,
      d.department_name,
      d.cost_centre,
      l.office_location,
      l.country
  FROM hr.employees e
  JOIN hr.departments d ON e.department = d.department_id
  JOIN hr.locations l ON d.location_id = l.location_id
  WHERE e.employment_status = 'ACTIVE'
")

# Aggregate workforce metrics
workforce_summary <- headcount_data %>%
  group_by(department_name, cost_centre, country) %>%
  summarise(
    headcount        = n(),
    avg_salary       = mean(salary),
    avg_performance  = mean(performance_rating),
    avg_tenure_years = mean(as.numeric(Sys.Date() - as.Date(hire_date)) / 365)
  )

# Write to target
dbWriteTable(con, DBI::Id(schema = "gold", table = "workforce_summary"),
             workforce_summary, overwrite = TRUE)

dbExecute(con, "
  INSERT INTO gold.high_performers
  SELECT
      employee_id,
      employee_name,
      department_name,
      salary,
      performance_rating
  FROM gold.workforce_summary
  WHERE performance_rating >= 4.0
")

dbDisconnect(con)
