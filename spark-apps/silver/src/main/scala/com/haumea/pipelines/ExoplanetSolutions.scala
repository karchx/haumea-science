package com.haumea.pipelines

import com.haumea.core.Pipeline
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.types.{FloatType, LongType}
import org.apache.spark.sql.expressions.Window
import java.sql.Connection
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat

object ExoplanetSolutions extends Pipeline {
  override def run(spark: SparkSession, connection: Connection) {
    val today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val metadata = getMetadata(connection, "silver", "exoplanet_solutions")
    val currentOpt = metadata.get("current").flatMap(Option(_))
    val genesisOpt = metadata.get("genesis").flatMap(Option(_))
    val targetDate = currentOpt.orElse(genesisOpt).orNull
    val defaultDate: String = targetDate match {
      case d: java.sql.Date => formatter.format(d)
      case s: java.sql.Timestamp => formatter.format(s)
      case d: java.util.Date => formatter.format(d)
      case s: String => s
      case null | _ => "2026-04-02"
    }
    println(s"Processing data for date: $defaultDate")

    val tableName = "lakehouse.silver.exoplanet_solutions"

    val rawDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .parquet("s3a://gaia-source/bronze/ps/")
    
    val filterDf = rawDf.withColumn("fct_dt", F.make_date(F.col("year"), F.col("month"), F.col("day")))
       .filter(
         F.col("fct_dt") === defaultDate
       )

    val windowSpec = Window
      .partitionBy("pl_name")
      .orderBy(
        F.col("soltype").contains("Confirmed").desc,
        F.col("releasedate").desc
      )
        
    val extractWindow  = windowSpec.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    val transformeDf = filterDf
      .withColumns(Map(
        "pl_masse_null" -> F.when(
          F.col("pl_masse") === 0.0, 
          F.lit(null)
        ).otherwise(F.col("pl_masse")),
        "rn" -> F.row_number().over(windowSpec),
        "gaia_dr3_id" -> F.regexp_extract(F.col("gaia_dr3_id"), " (\\d+)", 1).cast(LongType),
        "releasedate" -> F.coalesce(
          F.to_timestamp(F.col("releasedate"), "yyyy-MM-dd HH:mm:ss"),
          F.to_timestamp(F.col("releasedate"), "yyyy-MM-dd")
        )
      ))
      .withColumn("pl_masse_imputed", F.first(F.col("pl_masse_null"), ignoreNulls = true).over(extractWindow))
      .filter(F.col("rn") === 1)
      .select(
        F.col("pl_name").alias("planet_name"),
        F.col("pl_masse_imputed").cast(FloatType).alias("planet_mass"),
        F.col("hostname").alias("host_name"),
        F.col("gaia_dr3_id").alias("gaia_source_id"),
        F.col("soltype").alias("solution_type"),
        F.col("discoverymethod").alias("discovery_method"),
        F.col("disc_year").alias("discovery_year"),
        F.col("disc_locale").alias("discovery_locale"),
        F.col("disc_telescope").alias("discovery_telescope"),
        F.col("releasedate").alias("release_date"),
        F.col("fct_dt").alias("fct_dt")
      )
      .drop("rn")
      .orderBy(F.col("discovery_year"))

      if (!spark.catalog.tableExists(tableName)) {
        transformeDf.writeTo(tableName)
          .partitionedBy(F.col("fct_dt"))
          .tableProperty("format-version", "2")
          .tableProperty("write.spark.accept-any-schema", "true")
          .create()
      } else {
        transformeDf.writeTo(tableName)
          .option("mergeSchema", "true")
          .overwritePartitions()
      }

      val metadataId = metadata.get("id").map(_.toString).getOrElse("exoplanet_solutions")
      updateMetadata(connection, metadataId, java.sql.Date.valueOf(today))
  }
}
