package com.aries.common.iceberg

import org.apache.spark.sql.{DataFrame, Column, SparkSession}
import org.apache.spark.sql.functions.col
import cats.effect.IO

object IcebergManager {
  def syncIcebergOrCreate(
    spark: SparkSession,
    tableName: String,
    df: DataFrame,
    partitionCol: String,
    sortCols: Seq[String]
  ): IO[Unit] = IO.blocking {

    if (!spark.catalog.tableExists(tableName)) {
      var builder = df.limit(0).writeTo(tableName)
        .tableProperty("format-version", "2")
        .partitionedBy(col(partitionCol))
        .create()

      if (sortCols.nonEmpty) {
        val sortString = sortCols.mkString(", ")
        spark.sql(s"ALTER TABLE $tableName WRITE ORDERED BY $sortString")
      }
    } else {
      val existingCols = spark.table(tableName).schema.fieldNames.map(_.toLowerCase).toSet
      val newFields = df.schema.fields.filter(f => !existingCols.contains(f.name.toLowerCase))

      if (newFields.nonEmpty) {
        val ddlColumns = newFields.map(f => s"${f.name} ${f.dataType.catalogString}").mkString(", ")
        spark.sql(s"ALTER TABLE $tableName ADD COLUMNS ($ddlColumns)")
      }
    }
  }
}
