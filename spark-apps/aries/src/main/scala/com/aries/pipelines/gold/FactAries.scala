package com.aries.pipelines.gold

import cats.effect.IO
import java.sql.Connection
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
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
    val TOLERANCE_DEGREES = 1.0 / 3600.0
    val today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val tableName = "`datalake-haumea`.gold.fact_aries"
    for {
      _ <- IO.println(s"--- Init pipeline $tableName ---")
      metadata <- getMetadata(connection, "gold", "fact_aries")

      simbadDimDf <- IO.delay(spark.table("`datalake-haumea`.gold.dim_simbad"))
      photoOpticalDf <- IO.delay(spark.table("`datalake-haumea`.silver.photometry_optical"))
      astrometryDf <- IO.delay(spark.table("`datalake-haumea`.silver.gaia_astrometry"))

      unionAriesData = astrometryDf.as("astro").join(
        photoOpticalDf.as("po"),
          F.col("po.source_id") === F.col("astro.source_id"),
          "inner"
        ).select(
          F.col("astro.source_id"),
          F.col("po.j_m"),
          F.col("po.h_m"),
          F.col("po.ks_m"),
          F.col("astro.parallax"),
          F.col("astro.parallax_error"),
          F.col("astro.pmra"),
          F.col("astro.pmdec"),
          F.col("astro.ruwe"),
          F.coalesce(F.col("astro.ra"), F.col("po.ra")).alias("ra"),
          F.coalesce(F.col("astro.dec"), F.col("po.dec")).alias("dec"),
          F.coalesce(F.col("astro.healpix_index"), F.col("po.healpix_index")).alias("healpix_index")
        )

      joinDf = unionAriesData.as("ad").join(
        simbadDimDf.as("sd"),
        F.col("sd.healpix_index") === F.col("ad.healpix_index"),
       "inner"
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
          F.col("sd.ra").alias("ra_r"),
          F.col("sd.dec").alias("dec_r"),
          F.col("sd.main_id"),
          F.col("sd.bk_simbad")
      )

      dfFinal = joinDf.withThresholdAngularD(
        raL = "ra",
        decL = "dec",
        raR = "ra_r",
        decR = "dec_r"
      ).filter(F.col("threshold_angular_d") <= TOLERANCE_DEGREES)


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
