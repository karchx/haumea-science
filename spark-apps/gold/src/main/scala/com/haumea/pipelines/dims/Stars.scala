package com.haumea.pipelines.dims

import com.haumea.core.Pipeline
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.types.{FloatType, LongType}
import org.apache.spark.sql.expressions.Window
import java.sql.Connection

object Stars extends Pipeline {
  override def run(spark: SparkSession, connection: Connection) {
    val tableName = "lakehouse.gold.dim_stars"

    val dfHostStars = spark.read
      .format("iceberg")
      .load("lakehouse.silver.host_stars")

    val dfGaiaSource = spark.read
      .format("iceberg")
      .load("lakehouse.silver.gaia_main_source")

    val dfGaiaAstro = spark.read
      .format("iceberg")
      .load("lakehouse.silver.gaia_astro")


    val dfDimStar = dfHostStars.alias("hs")
      .join(dfGaiaSource.alias("gs"), F.col("hs.gaia_source_id") === F.col("gs.source_id"), "left")
      .join(dfGaiaAstro.alias("ga"), F.col("ga.gaia_source_id") === F.col("hs.gaia_source_id"), "left")
      .select(
        F.col("hs.host_name"),
        F.col("hs.gaia_source_id"),
        F.col("gs.ra"),
        F.col("gs.dec"),
        F.col("gs.parallax"),
        F.col("ga.teff"),
        F.col("ga.stellar_radius"),
        F.col("ga.stellar_luminosity"),
      )

      if (!spark.catalog.tableExists(tableName)) {
        dfDimStar.writeTo(tableName)
          .tableProperty("format-version", "2")
          .tableProperty("write.spark.accept-any-schema", "true")
          .create()
      } else {
        dfDimStar.writeTo(tableName)
          .option("mergeSchema", "true")
          .replace()
      }
  }
}
