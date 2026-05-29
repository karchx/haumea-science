package com.haumea.pipelines

import com.haumea.core.Pipeline
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.{functions => F, Column}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat
import java.sql.Connection

object StellarProperties extends Pipeline {

  // def syncIcebergOrCreate(spark: SparkSession, tableName: String, df: DataFrame): Unit = {
  //   if (!spark.catalog.tableExists(tableName)) {
  //     df.limit(0).writeTo(tableName)
  //       .tableProperty("format-version", "2")
  //       .partitionedBy(F.col("fct_dt"))
  //       .create()

  //       spark.sql(s"ALTER TABLE $tableName WRITE ORDERED BY source_id")
  //   } else {
  //     val existingCols = spark.table(tableName).schema.fieldNames.map(_.toLowerCase).toSet
  //     val newFields = df.schema.fields.filter(f => !existingCols.contains(f.name.toLowerCase))

  //     if (newFields.nonEmpty) {
  //       val ddlColumns = newFields.map(f => s"${f.name} ${f.dataType.catalogString}").mkString(", ")
  //       spark.sql(s"ALTER TABLE $tableName ADD COLUMNS ($ddlColumns)")
  //     }
  //   }
  // }

  // def merge(spark: SparkSession, tableName: String, stageTable: String) {
  //   // deduplicate
  //   spark.table(stageTable)
  //     .withColumn("rn", F.expr("ROW_NUMBER() OVER (PARTITION BY source_id ORDER BY fct_dt DESC)"))
  //     .filter(F.col("rn") === F.lit(1))
  //     .drop(F.col("rn"))
  //     .createOrReplaceTempView("dedup_stage")

  //    val mergeQuery = s"""
  //     MERGE INTO $tableName t
  //      USING dedup_stage s
  //      ON t.source_id = s.source_id
  //      WHEN MATCHED AND t.is_high_snr IS NULL THEN UPDATE SET *
  //      WHEN NOT MATCHED THEN INSERT *
  //     """

  //     spark.sql(mergeQuery)
  // }

  override def run(spark: SparkSession, connection: Connection): Unit = {
    val today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val metadata = getMetadata(connection, "silver", "stellar_properties")
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

    val tableName = "lakehouse.silver.stellar_properties"
  }
} 
