package com.aries.pipelines.gold

import cats.effect.IO
import java.sql.Connection
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{functions => F}

import com.aries.core.Pipeline
import com.aries.common.iceberg.IcebergManager
import com.aries.common.metadata.MetadataManager
import com.aries.transformations.Astrophysics._

object FactAries extends Pipeline {
  def overwriteDynamicIO(df: DataFrame, tableName: String): IO[Unit] = IO.blocking {
    df.sortWithinPartitions("healpix_index")
      .writeTo(tableName)
      .overwritePartitions()
  }

  override def runPipeline(spark: SparkSession, connection: Connection): IO[Unit] = {
    import spark.implicits._
    // equal to 1 arcsec
    val EPOCH_GAIA = 2016.0
    val EPOCH_SIMBAD = 2000.0
    val DELTA_T = EPOCH_GAIA - EPOCH_SIMBAD
    val MAS_TO_DEG = 1.0 / 3.6e6

    val TOLERANCE_DEGREES = 1.0 / 3600.0

    val today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val tableName = "`datalake-haumea`.gold.fact_aries"
    for {
      _ <- IO.println(s"--- Init pipeline $tableName ---")
      metadata <- getMetadata(connection, "gold", "fact_aries")

      simbadDimDf <- IO.delay(spark.table("`datalake-haumea`.gold.dim_simbad"))
      photoOpticalDf <- IO.delay(spark.table("`datalake-haumea`.silver.photometry_optical"))
      astrometryDf <- IO.delay(spark.table("`datalake-haumea`.silver.gaia_astrometry"))

      astroPhotoDF = astrometryDf.as("astro").join(
        photoOpticalDf.as("po"),
          F.col("po.source_id") === F.col("astro.source_id"),
          "inner"
        ).select(
          F.col("astro.source_id"),
          F.col("po.j_m"),
          F.col("po.h_m"),
          F.col("po.ks_m"),
          F.col("po.color_j_h"),
          F.col("po.color_h_ks"),
          F.col("astro.parallax"),
          F.col("astro.parallax_error"),
          F.col("astro.is_reliable_plx"),
          F.col("astro.pmra"),
          F.col("astro.pmdec"),
          F.col("astro.ruwe"),
          F.col("astro.x_unit"),
          F.col("astro.y_unit"),
          F.col("astro.z_unit"),
          F.coalesce(F.col("astro.ra"), F.col("po.ra")).alias("ra"),
          F.coalesce(F.col("astro.dec"), F.col("po.dec")).alias("dec"),
          F.coalesce(F.col("astro.healpix_index"), F.col("po.healpix_index")).alias("healpix_index")
        )

      joinDf = astroPhotoDF.as("ad").join(
        simbadDimDf.as("sd"),
        F.col("sd.healpix_index") === F.col("ad.healpix_index"),
       "left"
      )
      .select(
          F.col("ad.source_id"),
          F.col("ad.j_m"),
          F.col("ad.h_m"),
          F.col("ad.ks_m"),
          F.col("ad.parallax"),
          F.col("ad.parallax_error"),
          F.col("ad.pmra"),
          F.col("ad.pmdec"),
          F.col("ad.ruwe"),
          F.col("ad.ra"),
          F.col("ad.dec"),
          F.col("ad.healpix_index"),
          F.col("ad.x_unit"),
          F.col("ad.y_unit"),
          F.col("ad.z_unit"),
          F.col("ad.color_j_h"),
          F.col("ad.color_h_ks"),
          F.col("ad.is_reliable_plx"),
          F.col("sd.ra").alias("ra_r"),
          F.col("sd.dec").alias("dec_r"),
          F.col("sd.bk_simbad")
      )

      propagateDf = joinDf
        .withColumns(Map(
          "pmra_clean" -> F.coalesce(F.col("pmra"), F.lit(0.0)),
          "pmdec_clean" -> F.coalesce(F.col("pmdec"), F.lit(0.0)),
        ))
        .withColumns(Map(
          "ra_prop" -> (F.col("ra_r") + (
            F.col("pmra_clean") * F.lit(DELTA_T * MAS_TO_DEG) / F.cos(F.radians(F.col("dec_r")))
          )),
          "dec_prop" -> (F.col("dec_r") + (F.col("pmdec_clean") * F.lit(DELTA_T * MAS_TO_DEG)))
        ))

      distanceDf = propagateDf.withThresholdAngularD(
        raL = "ra",
        decL = "dec",
        raR = "ra_prop",
        decR = "dec_prop"
      )
      .withDistanceParsecs(
        parallax = "parallax",
        reliablePlx = "is_reliable_plx"
      )
      .withIntrinsicAbsoluteMagnitude(
        jM = "j_m",
        jH = "h_m",
        jKs = "ks_m",
        dc = "distance_pc"
      )
      .withColumn(
        "tangential_velocity",
        F.lit(4.74) * F.col("distance_pc") * F.sqrt(F.pow(F.col("pmra"), 2)) + F.pow(F.col("pmdec"), 2)
      )

      windowSpec = Window.partitionBy("ad.source_id").orderBy(F.col("threshold_angular_d").asc)

      dfFinal = distanceDf
        .withColumn("rank", F.row_number().over(windowSpec))
        .withColumn("bk_simbad", F.when(F.col("threshold_angular_d") <= TOLERANCE_DEGREES, F.col("bk_simbad")).otherwise(F.lit(-1)))
        .filter(F.col("rank") === 1)
        .drop("rank")
        .select(
          F.col("source_id"),
          F.col("j_m"),
          F.col("h_m"),
          F.col("ks_m"),
          F.col("parallax"),
          F.col("parallax_error"),
          F.col("pmra"),
          F.col("pmdec"),
          F.col("ruwe"),
          F.col("healpix_index"),
          F.col("ra"),
          F.col("dec"),
          F.col("bk_simbad"),
          F.col("x_unit").alias("x"),
          F.col("y_unit").alias("y"),
          F.col("z_unit").alias("z"),
          F.col("color_j_h"),
          F.col("color_h_ks"),
          F.col("distance_pc"),
          F.col("abs_mag_j"),
          F.col("abs_mag_h"), 
          F.col("abs_mag_ks"),
          F.col("tangential_velocity")
        )
      _ <- IcebergManager.syncIcebergOrCreate(
        spark = spark,
        tableName = tableName,
        df = dfFinal,
        partitionCol = "healpix_index",
        sortCols = Seq("healpix_index")
      )
      _ <- overwriteDynamicIO(dfFinal, tableName)

      _ <- IO.blocking {
        val metadataId = metadata.get("id").map(_.toString).getOrElse("fact_aries")
        IO.println(s"MetadataId: $metadataId")
        updateMetadata(connection, metadataId, java.sql.Date.valueOf(today))
      }
      _ <- IO.println(s"--- End pipeline $tableName ---")
    } yield ()
  }
}
