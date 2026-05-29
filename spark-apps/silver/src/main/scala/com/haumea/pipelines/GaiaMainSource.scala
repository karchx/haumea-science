package com.haumea.pipelines

import com.haumea.core.Pipeline
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.{functions => F, Column}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat
import java.sql.Connection

object GaiaMainSource extends Pipeline {

  def syncIcebergOrCreate(spark: SparkSession, tableName: String, df: DataFrame): Unit = {
    if (!spark.catalog.tableExists(tableName)) {
      df.limit(0).writeTo(tableName)
        .tableProperty("format-version", "2")
        .partitionedBy(F.col("fct_dt"))
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
    // deduplicate
    spark.table(stageTable)
      .withColumn("rn", F.expr("ROW_NUMBER() OVER (PARTITION BY source_id ORDER BY fct_dt DESC)"))
      .filter(F.col("rn") === F.lit(1))
      .drop(F.col("rn"))
      .createOrReplaceTempView("dedup_stage")

     val mergeQuery = s"""
      MERGE INTO $tableName t
       USING dedup_stage s
       ON t.source_id = s.source_id
       WHEN MATCHED THEN UPDATE SET *
       WHEN NOT MATCHED THEN INSERT *
      """

      spark.sql(mergeQuery)
  }

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

    val tableName = "lakehouse.silver.gaia_main_source"

    val rawDf = spark.read
      .option("header", "true")
      .option("mergeSchema", "true")
      .parquet("s3a://gaia-source/bronze/gaia_source/")

    val filterDf = rawDf.withColumn("fct_dt", F.make_date(F.col("year"), F.col("month"), F.col("day"))) 
       .filter(F.col("fct_dt") >= defaultDate)
    
    val doubleCols = Seq(
      "ra", "dec", "parallax", "pmra", "pmdec",
      "phot_g_mean_mag", "bp_rp", "radial_velocity",
      "parallax_error", "pmra_error", "pmdec_error", 
      "radial_velocity_error", "ruwe", "astrometric_excess_noise",
      "astrometric_excess_noise_sig", "teff_gspphot", "logg_gspphot",
      "mh_gspphot", "parallax_over_error", "rv_expected_sig_to_noise"
    )

    val doubleCastsMap = doubleCols.map(c => c -> F.col(c).cast("double")).toMap

    val castDf = filterDf
      .withColumns(doubleCastsMap)
      .withColumns(Map(
          "source_id" -> F.col("source_id").cast("string").cast("long"),
          "healpix_id" -> getHealpixExpr(1, "source_id"),
          "healpix_6" -> getHealpixExpr(6, "source_id"),
          "absolute_mag_g" -> calcMagnitudeAbs("parallax", "phot_g_mean_mag"),
          "fct_dt_string" -> F.date_format(F.col("fct_dt"), "yyyyMMdd"),
          "is_high_snr" -> (F.col("parallax_over_error") > 10).and(F.col("parallax") > 0).and(F.col("ruwe") < 1.4)
      ))
      .drop(F.col("fct_dt"))

    val df = castDf.select(
      F.col("source_id"), 
      F.col("ra"), 
      F.col("dec"), 
      F.col("parallax"), 
      F.col("pmra"),
      F.col("pmdec"),
      F.col("phot_g_mean_mag"),
      F.col("bp_rp"),
      F.col("radial_velocity"),
      F.col("parallax_over_error"),
      F.col("parallax_error"),
      F.col("pmra_error"),
      F.col("pmdec_error"),
      F.col("radial_velocity_error"),
      F.col("ruwe"),
      F.col("astrometric_excess_noise"),
      F.col("astrometric_excess_noise_sig"),
      F.col("teff_gspphot"),
      F.col("logg_gspphot"),
      F.col("mh_gspphot"),
      F.col("phot_variable_flag"),
      F.col("healpix_6"), 
      F.col("absolute_mag_g"),
      F.col("healpix_id"),
      F.col("is_high_snr"),
      F.col("fct_dt_string").alias("fct_dt"),
      F.col("rv_expected_sig_to_noise"),
      F.col("ipd_gof_harmonic_amplitude"),
      F.col("ipd_frac_multi_peak"),
      F.col("non_single_star")
    )
    .drop(F.col("fct_dt_string"))
    
    syncIcebergOrCreate(spark, tableName, df)

    df.createOrReplaceTempView("stage_new")

    merge(spark, tableName, "stage_new")

    val metadataId = metadata.get("id").map(_.toString).getOrElse("gaia_main_source")
    updateMetadata(connection, metadataId, java.sql.Date.valueOf(today))
  }
}
