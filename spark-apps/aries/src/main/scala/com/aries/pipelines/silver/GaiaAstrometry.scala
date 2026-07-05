package com.aries.pipelines.silver

import cats.effect.IO
import java.sql.Connection
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.{functions => F}

import com.aries.core.Pipeline
import com.aries.common.iceberg.IcebergManager
import com.aries.common.metadata.MetadataManager
import com.aries.models.silver.GaiaSilver
import com.aries.transformations.Astrophysics._

object GaiaAstrometry extends Pipeline {
  def overwriteDynamicIO(df: DataFrame, tableName: String): IO[Unit] = IO.blocking {
    df
      .withColumn("rn", F.expr("ROW_NUMBER() OVER (PARTITION BY source_id ORDER BY fct_dt DESC)"))
      .filter(F.col("rn") === F.lit(1))
      .drop(F.col("rn"))
      .sortWithinPartitions("healpix_index")
      .writeTo(tableName)
      .overwritePartitions()
  }

  override def runPipeline(spark: SparkSession, connection: Connection): IO[Unit] = {
    import spark.implicits._
    val today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val tableName = "`datalake-haumea`.silver.gaia_astrometry"
    for {
      _ <- IO.println(s"--- Init pipeline $tableName ---")
      metadata <- getMetadata(connection, "silver", "gaia_astrometry")

      bronzeCols = Seq(
        "source_id",
        "ra",
        "dec",
        "parallax",
        "parallax_error",
        "pmra",
        "pmdec",
        "ruwe",
        "year",
        "month",
        "day"
      )

      ariesClusterRawDF <- IcebergManager.readDf(spark, "s3a://gaia-source/bronze/aries_star_cluster/", Some(bronzeCols))
      ariesGaiaRawDF <- IcebergManager.readDf(spark, "s3a://gaia-source/bronze/aries_gaia/", Some(bronzeCols))

      bronzeRawDF <- IO.delay(ariesClusterRawDF.unionByName(ariesGaiaRawDF).filter(F.col("source_id").isNotNull))

      dfTransformed <- IO.delay {
          bronzeRawDF.withHealpixIndex(raCol = "ra", decCol = "dec")
            .withUnitVectors(raCol = "ra", decCol = "dec")
            .withColumns(Map(
              "fct_dt" -> F.concat(F.col("year"), F.col("month"), F.col("day")),
              "is_reliable_astro" -> (F.col("ruwe") < F.lit(1.4)),
              "is_reliable_plx" -> (F.col("parallax_error") / F.col("parallax") < F.lit(0.2) && F.col("parallax") > F.lit(0))
            ))
      }

      dfFinalDs: Dataset[GaiaSilver] = dfTransformed.select(
        F.col("source_id"),
        F.col("ra"),
        F.col("dec"),
        F.col("parallax"),
        F.col("parallax_error"),
        F.col("pmra"),
        F.col("pmdec"),
        F.col("ruwe"),
        F.col("fct_dt"),
        F.col("healpix_index"),
        F.col("x_unit"),
        F.col("y_unit"),
        F.col("z_unit"),
        F.col("is_reliable_astro"),
        F.col("is_reliable_plx")
      )
        .as[GaiaSilver]

      dfFinal = dfFinalDs.toDF()

      _ <- IcebergManager.syncIcebergOrCreate(
        spark = spark,
        tableName = tableName,
        df = dfFinal,
        partitionCol = "healpix_index",
        sortCols = Seq("healpix_index")
      )
      _ <- overwriteDynamicIO(dfFinal, tableName)

      _ <- IO.blocking {
        val metadataId = metadata.get("id").map(_.toString).getOrElse("gaia_astrometry")
        IO.println(s"MetadataId: $metadataId")
        updateMetadata(connection, metadataId, java.sql.Date.valueOf(today))
      }
      _ <- IO.println(s"--- End pipeline $tableName ---")
    } yield ()
  }
}
