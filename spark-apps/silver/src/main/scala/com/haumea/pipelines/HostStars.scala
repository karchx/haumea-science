package com.haumea.pipelines

import com.haumea.core.Pipeline
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.types.{FloatType, LongType}
import org.apache.spark.sql.expressions.Window

object HostStars extends Pipeline {
  override def run(spark: org.apache.spark.sql.SparkSession, connection: java.sql.Connection): Unit = {
    val today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val metadata = getMetadata(connection, "silver", "host_stars")
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

    val tableName = "lakehouse.silver.host_stars"

    val rawDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .parquet("s3a://gaia-source/bronze/ps/")
    
    val filterDf = rawDf.withColumn("fct_dt", F.make_date(F.col("year"), F.col("month"), F.col("day")))
       .filter(
         F.col("fct_dt") === defaultDate
       )

    val windowSpec = Window
      .partitionBy("hostname")
      .orderBy(
        F.col("soltype").contains("Confirmed").desc,
        F.col("releasedate").desc
      )

    val transformeDf = filterDf
      .withColumns(Map(
        "rn" -> F.row_number().over(windowSpec),
        "gaia_dr3_id" -> F.regexp_extract(F.col("gaia_dr3_id"), " (\\d+)", 1).cast(LongType),
        "releasedate" -> F.coalesce(
          F.to_timestamp(F.col("releasedate"), "yyyy-MM-dd HH:mm:ss"),
          F.to_timestamp(F.col("releasedate"), "yyyy-MM-dd")
        )
      ))
      .filter(F.col("rn") === 1 && F.col("gaia_dr3_id").isNotNull)
      .select(
        F.col("hostname").alias("host_name"),
        F.col("gaia_dr3_id").alias("gaia_source_id"),
        F.col("dec"),
        F.col("ra"),
        F.col("elat"),
        F.col("elon"),
        F.col("fct_dt"),
        F.col("releasedate").alias("release_date")
      )
      .drop("rn")

    if (!spark.catalog.tableExists(tableName)) {
      transformeDf.writeTo(tableName)
        .partitionedBy(F.col("fct_dt"))
        .tableProperty("format-version", "2")
        .create()
    } else {
      transformeDf.writeTo(tableName)
        .option("mergeSchema", "true")
        .overwritePartitions()
    }

    val metadataId = metadata.get("id").map(_.toString).getOrElse("host_stars")
    updateMetadata(connection, metadataId, java.sql.Date.valueOf(today))
  }
}
