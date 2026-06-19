"""
HR Employee Data Curation Pipeline

Combines employee records, department info, payroll, and performance
data into a curated employee master table. Also produces an aggregation
table for headcount metrics by department.
"""

# Pipeline metadata
DOMAIN = "hr"
SUBJECT_AREA = "employee"
SCHEDULE = "0 6 * * 1-5"
OWNER = "hr-analytics-team"

from pyspark.sql import functions as F

# --- Source: Read raw tables from bronze layer ---

employees = spark.table("frontier_bronze.hr_employees")
departments = spark.table("frontier_bronze.hr_departments")
payroll = spark.table("frontier_bronze.hr_payroll")
performance = spark.table("frontier_bronze.hr_performance_reviews")
locations = spark.table("frontier_bronze.hr_office_locations")

# --- Transform: Join and enrich employee data ---

# Join employees with departments
emp_dept = employees.join(
    departments,
    employees.dept_id == departments.dept_id,
    "left"
)

# Join with office locations
emp_location = emp_dept.join(
    locations,
    emp_dept.office_id == locations.location_id,
    "left"
)

# Filter active employees and seniority >= 6 months
active_employees = emp_location.filter(
    (emp_location.employment_status == "active") &
    (emp_location.tenure_months >= 6)
)

# Join with latest payroll data
with_payroll = active_employees.join(
    payroll,
    active_employees.emp_id == payroll.employee_id,
    "left"
)

# Join with performance reviews
with_reviews = with_payroll.join(
    performance,
    with_payroll.emp_id == performance.emp_id,
    "left"
)

# Select and rename final columns
curated = with_reviews.select(
    employees.emp_id,
    employees.first_name,
    employees.last_name,
    employees.email,
    departments.dept_name,
    departments.division,
    locations.city,
    locations.country,
    payroll.base_salary,
    payroll.bonus_pct,
    performance.review_score,
    performance.review_year,
)

# --- Target: Write curated employee master table ---
curated.write.saveAsTable("frontier_silver.hr_employee_master")

# --- Aggregation: Build headcount metrics by department ---
headcount = curated.groupBy("dept_name", "division", "country").agg(
    F.count("emp_id").alias("employee_count"),
    F.avg("base_salary").alias("avg_salary"),
    F.avg("review_score").alias("avg_review_score"),
    F.sum("bonus_pct").alias("total_bonus_pct"),
)

# --- Target: Write headcount aggregation ---
headcount.write.insertInto("frontier_silver.hr_dept_headcount_metrics")

# --- Also run a legacy SQL aggregation for executive dashboard ---
spark.sql("""
    INSERT INTO frontier_silver.hr_executive_summary
    SELECT
        division,
        COUNT(*) AS headcount,
        SUM(base_salary) AS total_payroll,
        AVG(review_score) AS avg_performance
    FROM frontier_silver.hr_employee_master
    GROUP BY division
""")
