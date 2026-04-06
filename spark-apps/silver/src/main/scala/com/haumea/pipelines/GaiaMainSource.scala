package com.haumea.pipelines

import com.haumea.core.Pipeline
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F, Column}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat
import java.sql.Connection

object GaiaMainSource extends Pipeline {
  // ref: https://blog.g-vo.org/healpix-maps-in-general-and-in-gaia.html
  def getHealpixExpr(level: Int, colName: String): Column = {
    require(level >= 0 && level <= 12, "Healpix level must be between 0 and 12")
    val shiftBits = 35 + (12 - level) * 2

    F.shiftright(F.col(colName), shiftBits).cast("integer")
  }

  def calcMagnitudeAbs(wCol: String, mGCol: String): Column = {
    // d = wCol / 1000
    // MG = mGCol + 5 + 5 * log10(d)
    val d = F.col(wCol) / F.lit(1000)
    val MG = F.col(mGCol) + F.lit(5) + F.lit(5) * F.log10(d)
    MG.cast("float")
  }

  override def run(spark: SparkSession, connection: Connection): Unit = {
    val today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val metadata = getMetadata(connection, "silver", "gaia_main_source")
    val currentOpt = metadata.get("current")
    val genesisOpt = metadata.get("genesis")
    val targetDate = currentOpt.getOrElse(genesisOpt.orNull)
    val defaultDate: String = targetDate match {
      case d: java.sql.Date => formatter.format(d)
      case s: java.sql.Timestamp => formatter.format(s)
      case d: java.util.Date => formatter.format(d)
      case s: String => s
      case null | _ => "2026-04-02"
    }
    println(s"Processing data for date: $defaultDate")

    val tableName = "lakehouse.silver.gaia_main_source"

    val rawDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .parquet("s3a://gaia-source/bronze/gaia_source/")
    
    val filterDf = rawDf.withColumn("fct_dt", F.make_date(F.col("year"), F.col("month"), F.col("day"))) 
       .filter(
         F.col("fct_dt") === defaultDate &&
         F.col("parallax_over_error") > 10 && // omit SNR
         F.col("parallax") > 0 &&
         F.col("ruwe") < 1.4
       )

    val castDf = filterDf
      .withColumn("ra", F.col("ra").cast("float"))
      .withColumn("dec", F.col("dec").cast("float"))
      .withColumn("parallax", F.col("parallax").cast("float"))
      .withColumn("source_id", F.col("source_id").cast("long"))
      .withColumn("bp_rp", F.col("bp_rp").cast("float"))
      .withColumn("healpix_6", getHealpixExpr(6, "source_id"))
      .withColumn("healpix_8", getHealpixExpr(8, "source_id"))
      .withColumn("absolute_mag_g", calcMagnitudeAbs("parallax", "phot_g_mean_mag"))
      .dropDuplicates("source_id")

    val df = castDf.select(
      F.col("source_id"), 
      F.col("ra"), 
      F.col("dec"), 
      F.col("parallax"), 
      F.col("healpix_6"), 
      F.col("fct_dt"),
      F.col("bp_rp"),
      F.col("absolute_mag_g"),
      F.col("healpix_8")
    )

    if (!spark.catalog.tableExists(tableName)) {
      df.writeTo(tableName)
        .partitionedBy(F.col("fct_dt"))
        .tableProperty("format-version", "2")
        .tableProperty("write.parquet.row-group-size-bytes", "33554432") // 32MB
        .tableProperty("write.spark.accept-any-schema", "true")
        .create()

        spark.sql(s"ALTER TABLE $tableName WRITE ORDERED BY healpix_6")
    } else {
      df.writeTo(tableName)
        .option("mergeSchema", "true")
        .overwritePartitions()
    }
    val metadataId = metadata.get("id").map(_.toString).getOrElse("gaia_main_source")
    updateMetadata(connection, metadataId, java.sql.Date.valueOf(today))
  }
}
