package com.haumea.pipelines.dims

import com.haumea.core.Pipeline
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.expressions.Window
import java.sql.Connection
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object Planets extends Pipeline {
  def createTable(spark: SparkSession, tableName: String): Unit = {
    val createTableQuery = s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        planet_sk STRING,
        planet_name STRING,
      )
      USING iceberg
      TBLPROPERTIES (
        'format-version' = '2'
      )
    """

    spark.sql(createTableQuery)
  }

  override def run(spark: SparkSession, connection: Connection) {
    val tableName = "lakehouse.gold.dim_discovery"
    createTable(spark, tableName)
    spark.sql(s"ALTER TABLE $tableName UNSET TBLPROPERTIES ('write.spark.accept-any-schema')")

    val dfExoplanetSolutions = spark.read
      .format("iceberg")
      .load("lakehouse.silver.exoplanet_solutions")

    val dfDimDiscoveries = dfExoplanetSolutions
      .withColumn("discovery_sk", F.md5(
        F.concat_ws("||", 
          F.coalesce(F.col("discovery_method"), F.lit("unknown")),
          F.coalesce(F.col("discovery_year"), F.lit("unknown")),
          F.coalesce(F.col("discovery_telescope"), F.lit("unknown"))
        )
      ))
      .select(
        F.col("discovery_sk"),
        F.col("discovery_method"),
        F.col("discovery_year"),
        F.col("discovery_telescope"),
        F.current_date().alias("fct_date")
      )
      .dropDuplicates("discovery_sk")

    dfDimDiscoveries.createOrReplaceTempView("stg_new_dim_discoveries")

    val mergeQuery = s"""
      MERGE INTO $tableName t
      USING (
        SELECT
          discovery_sk,
          discovery_method,
          discovery_year,
          discovery_telescope,
          fct_date
        FROM stg_new_dim_discoveries
      ) s
      ON t.discovery_sk = s.discovery_sk
      WHEN MATCHED THEN
        UPDATE SET
          t.discovery_method = s.discovery_method,
          t.discovery_year = s.discovery_year,
          t.discovery_telescope = s.discovery_telescope,
          t.fct_date = s.fct_date
      WHEN NOT MATCHED THEN
        INSERT (discovery_sk, discovery_method, discovery_year, discovery_telescope, fct_date)
        VALUES (s.discovery_sk, s.discovery_method, s.discovery_year, s.discovery_telescope, s.fct_date)
    """

    spark.sql(mergeQuery)
  }
}
