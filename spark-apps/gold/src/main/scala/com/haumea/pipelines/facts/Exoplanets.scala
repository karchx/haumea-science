package com.haumea.pipelines.facts

import com.haumea.core.Pipeline
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.expressions.Window
import java.sql.Connection
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object Exoplanets extends Pipeline {
  def createTable(spark: SparkSession, tableName: String): Unit = {
    val createTableQuery = s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        planet_sk STRING,
        star_sk STRING,
        discovery_sk STRING,
        discovery_locale STRING,
        planet_mass DOUBLE,
        release_date DATE
      )
      USING iceberg
      TBLPROPERTIES (
        'format-version' = '2'
      )
    """

    spark.sql(createTableQuery)
  }

  override def run(spark: SparkSession, connection: Connection) {
    val tableName = "lakehouse.gold.fact_exoplanets"
    createTable(spark, tableName)

    val dfExoplanetSolutions = spark.read
      .format("iceberg")
      .load("lakehouse.silver.exoplanet_solutions")

    val dimDiscoveries = spark.read
      .format("iceberg")
      .load("lakehouse.gold.dim_discovery")
      .select("discovery_id", "discovery_method", "discovery_year", "discovery_telescope")

    val factExoplanets = dfExoplanetSolutions.alias("ex").join(
        dimDiscoveries.alias("dim"), 
        F.col("dim.discovery_id") === F.col("ex.discovery_id"),
        "left"
      )
      .select(
        F.col("ex.planet_name"),
        F.col("ex.host_name"),
        F.col("ex.planet_mass"),
        F.col("dim.discovery_method"),
        F.col("dim.discovery_year"),
        F.col("dim.discovery_telescope"),
        F.col("ex.discovery_locale"),
        F.col("ex.release_date"),
      )

    factExoplanets.createOrReplaceTempView("stage_new")

    val mergeQuery = s"""
      MERGE INTO $tableName t
      USING (
        SELECT
          planet_name,
          host_name,
          discovery_method,
          discovery_telescope,
          discovery_locale,
          discovery_year,
          planet_mass,
          release_date
        FROM stage_new
      ) s
      ON t.planet_name = s.planet_name AND s.release_date > t.release_date
      WHEN MATCHED THEN
        UPDATE SET
          t.planet_name = s.planet_name,
          t.host_name = s.host_name,
          t.discovery_method = s.discovery_method,
          t.discovery_telescope = s.discovery_telescope,
          t.discovery_locale = s.discovery_locale,
          t.discovery_year = s.discovery_year,
          t.planet_mass = s.planet_mass,
          t.release_date = s.release_date
      WHEN NOT MATCHED THEN
        INSERT (planet_name, host_name, discovery_method, discovery_telescope, discovery_locale, discovery_year, planet_mass, release_date)
        VALUES (s.planet_name, s.host_name, s.discovery_method, s.discovery_telescope, s.discovery_locale, s.discovery_year, s.planet_mass, s.release_date)
    """

    spark.sql(mergeQuery)
  }
}
