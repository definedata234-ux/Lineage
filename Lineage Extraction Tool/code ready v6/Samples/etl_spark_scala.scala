// DOMAIN: logistics
// SUBJECT_AREA: fleet_tracking
// OWNER: data-engineering

/**
 * Spark Scala Pipeline: Fleet Tracking Aggregation
 * Tests: spark.sql() embedded SQL, DataFrame API, saveAsTable
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FleetTrackingPipeline {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("FleetTracking")
      .getOrCreate()

    // Embedded SQL extraction
    val fleetData = spark.sql("""
      SELECT
          v.vehicle_id,
          v.vehicle_type,
          v.driver_id,
          d.driver_name,
          d.license_class,
          t.trip_id,
          t.origin_location,
          t.destination_location,
          t.start_time,
          t.end_time,
          t.distance_km,
          t.fuel_consumed_litres
      FROM silver.vehicles v
      JOIN silver.drivers d ON v.driver_id = d.driver_id
      JOIN silver.trips t ON v.vehicle_id = t.vehicle_id
      WHERE t.trip_date >= '2024-01-01'
        AND t.status = 'completed'
    """)

    // Aggregate efficiency metrics
    val efficiencyMetrics = fleetData
      .groupBy("vehicle_id", "vehicle_type", "driver_id", "driver_name")
      .agg(
        count("trip_id").alias("total_trips"),
        sum("distance_km").alias("total_distance"),
        sum("fuel_consumed_litres").alias("total_fuel"),
        avg("distance_km").alias("avg_trip_distance")
      )
      .withColumn("fuel_efficiency_km_per_l",
        col("total_distance") / col("total_fuel")
      )

    // Write output
    efficiencyMetrics.write
      .mode("overwrite")
      .saveAsTable("gold.fleet_efficiency_summary")
  }
}
