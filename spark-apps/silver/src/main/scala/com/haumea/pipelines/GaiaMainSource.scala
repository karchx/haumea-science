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

  def writeGaiaMainSource(df: org.apache.spark.sql.DataFrame, tableName: String): Unit = {
    if (!df.sparkSession.catalog.tableExists(tableName)) {
      df.writeTo(tableName)
        .partitionedBy(F.col("fct_dt"))
        .tableProperty("format-version", "2")
        .tableProperty("write.parquet.row-group-size-bytes", "33554432") // 32MB
        .tableProperty("write.spark.accept-any-schema", "true")
        .create()

      df.sparkSession.sql(s"ALTER TABLE $tableName WRITE ORDERED BY healpix_6")
    } else {
      df.writeTo(tableName)
        .option("mergeSchema", "true")
        .overwritePartitions()
    }
  }

  def writeGaiaAstro(df: org.apache.spark.sql.DataFrame, tableName: String): Unit = {
    if (!df.sparkSession.catalog.tableExists(tableName)) {
      df.writeTo(tableName)
        .partitionedBy(F.col("fct_dt"))
        .tableProperty("format-version", "2")
        .tableProperty("write.parquet.row-group-size-bytes", "33554432") // 32MB
        .tableProperty("write.spark.accept-any-schema", "true")
        .create()
    } else {
      df.writeTo(tableName)
        .option("mergeSchema", "true")
        .overwritePartitions()
    }
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

    val rawAstroDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .parquet("s3a://gaia-source/bronze/astrophysical_parameters/")
    
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

    val dfMainSource = castDf.select(
      F.col("source_id"), 
      F.col("ra"), 
      F.col("dec"), 
      F.col("parallax"), 
      F.col("healpix_6"), 
      F.col("fct_dt"),
      F.col("bp_rp"),
      F.col("absolute_mag_g"),
      F.col("healpix_8"),
      F.col("phot_g_mean_mag"),
      F.col("pmra"),
      F.col("pmdec"),
    )


    val filterDfAstro = rawAstroDf.withColumn("fct_dt", F.make_date(F.col("year"), F.col("month"), F.col("day"))) 
      .filter(F.col("fct_dt") === defaultDate)

    val dfAstro = filterDfAstro.select(
      F.col("source_id").cast("long").alias("gaia_source_id"), 
      F.col("fct_dt"),
      F.col("teff_gspphot").cast("float").alias("teff"),
      F.col("radius_gspphot").cast("float").alias("stellar_radius"),
      F.col("lum_flame").cast("float").alias("stellar_luminosity"),
    ).filter(F.col("source_id").isNotNull)

    writeGaiaMainSource(dfMainSource, tableName)
    writeGaiaAstro(dfAstro, "lakehouse.silver.gaia_astro")

    val metadataId = metadata.get("id").map(_.toString).getOrElse("gaia_main_source")
    updateMetadata(connection, metadataId, java.sql.Date.valueOf(today))
  }
}
