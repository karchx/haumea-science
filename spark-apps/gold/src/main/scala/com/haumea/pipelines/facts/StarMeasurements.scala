package com.haumea.pipelines

import com.haumea.core.Pipeline
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.{functions => F, Column}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat
import java.sql.Connection

object StarMeasurements extends Pipeline {

  def syncIcebergOrCreate(spark: SparkSession, tableName: String, df: DataFrame): Unit = {
    if (!spark.catalog.tableExists(tableName)) {
      df.limit(0).writeTo(tableName)
        .tableProperty("format-version", "2")
        .partitionedBy(F.col("healpix_id"))
        .create()

        spark.sql(s"ALTER TABLE $tableName WRITE ORDERED BY source_id")
    } else {
      val existingCols = spark.table(tableName).schema.fieldNames.map(_.toLowerCase).toSet
      val newFields = df.schema.fields.filter(f => !existingCols.contains(f.name.toLowerCase))

      if (newFields.nonEmpty) {
        val ddlColumns = newFields.map(f => s"${f.name} ${f.dataType.catalogString}").mkString(", ")
        spark.sql(s"ALTER TABLE $tableName ADD COLUMNS ($ddlColumns)")
      }
    }
  }

  def merge(spark: SparkSession, tableName: String, stageTable: String) {
     val mergeQuery = s"""
      MERGE INTO $tableName t
       USING $stageTable s
       ON t.source_id = s.source_id
       WHEN MATCHED THEN UPDATE SET *
       WHEN NOT MATCHED THEN INSERT *
      """

      spark.sql(mergeQuery)
  }

  override def run(spark: SparkSession, connection: Connection): Unit = {
    val today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val metadata = getMetadata(connection, "gold", "star_measurements")
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

    val tableName = "lakehouse.gold.star_measurements"

    val dfMainSource = spark.read
      .format("iceberg")
      .load("lakehouse.silver.gaia_main_source")
      .filter((F.col("fct_dt") >= defaultDate).and(F.col("is_high_snr")))

    val df = dfMainSource.select(
      F.col("source_id"), 
      F.col("ra"), 
      F.col("dec"), 
      F.col("parallax"), 
      F.col("pmra"),
      F.col("pmdec"),
      F.col("phot_g_mean_mag"),
      F.col("bp_rp"),
      F.col("radial_velocity"),
      F.col("healpix_id"),
      F.col("is_high_snr"),
    )
    
    syncIcebergOrCreate(spark, tableName, df)

    df.createOrReplaceTempView("stage_new")

    merge(spark, tableName, "stage_new")

    val metadataId = metadata.get("id").map(_.toString).getOrElse("star_measurements")
    updateMetadata(connection, metadataId, java.sql.Date.valueOf(today))
  }
}

