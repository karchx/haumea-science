package com.haumea.pipelines

import com.haumea.core.Pipeline
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F, Column}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object GaiaMainSource extends Pipeline {
  // ref: https://blog.g-vo.org/healpix-maps-in-general-and-in-gaia.html
  def getHealpixExpr(level: Int, colName: String): Column = {
    require(level >= 0 && level <= 12, "Healpix level must be between 0 and 12")
    val shiftBits = 35 + (12 - level) * 2

    F.shiftright(F.col(colName), shiftBits).cast("integer")
  }

  override def run(spark: SparkSession): Unit = {
    // val today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val defaultDate = "2026-04-02"
    val tableName = "lakehouse.silver.gaia_main_source"

    val rawDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .parquet("s3a://gaia-source/bronze/gaia_source/")
    
    val filterDf = rawDf.withColumn("fct_dt", F.make_date(F.col("year"), F.col("month"), F.col("day"))) 
       .filter(F.col("fct_dt") === defaultDate)

    val castDf = filterDf.withColumn("ra", F.col("ra").cast("double"))
      .withColumn("dec", F.col("dec").cast("double"))
      .withColumn("parallax", F.col("parallax").cast("double"))
      .withColumn("source_id", F.col("source_id").cast("long"))
      .withColumn("healpix_6", getHealpixExpr(6, "source_id"))
      .dropDuplicates("source_id")

    val df = castDf.select(
      F.col("source_id"), 
      F.col("ra"), 
      F.col("dec"), 
      F.col("parallax"), 
      F.col("healpix_6"), 
      F.col("fct_dt")
    )

    if (!spark.catalog.tableExists(tableName)) {
      df.writeTo(tableName)
        .tableProperty("format-version", "2")
        .tableProperty("write.parquet.row-group-size-bytes", "33554432") // 32MB
        .create()

        spark.sql(s"ALTER TABLE $tableName WRITE ORDERED BY healpix_6")
      // spark.sql(s"ALTER TABLE $tableName WRITE ORDERED BY ZORDER(ra, dec)")
    } else {
      df.writeTo(tableName).append()
    }
  }
}
