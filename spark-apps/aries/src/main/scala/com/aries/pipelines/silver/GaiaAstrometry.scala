package com.aries.pipelines.silver

import com.aries.core.Pipeline
import com.aries.common.iceberg.IcebergManager
import com.aries.common.metadata.MetadataManager
import cats.effect.IO
import java.sql.Connection
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{functions => F}

object GaiaAstrometry extends Pipeline {
  def overwriteDynamicIO(df: DataFrame, tableName: String): IO[Unit] = IO.blocking {
    df
      .withColumn("rn", F.expr("ROW_NUMBER() OVER (PARTITION BY source_id ORDER BY fct_dt DESC)"))
      .filter(F.col("rn") === F.lit(1))
      .drop(F.col("rn"))
      .writeTo(tableName)
      .overwritePartitions()
  }

  override def runPipeline(spark: SparkSession, connection: Connection): IO[Unit] = {
    val today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val tableName = "`datalake-haumea`.silver.gaia_astrometry"
    for {
      _ <- IO.println(s"--- Init pipeline $tableName ---")
      metadata <- getMetadata(connection, "silver", "gaia_astrometry")

      bronzeRawDF <- IO.delay {
        spark.read
          .option("header", "true")
          .option("mergeSchema", "true")
          .parquet("s3a://gaia-source/bronze/aries_star_cluster/")
      }

      dfFinal = insertFactibleDate(bronzeRawDF).select(
        F.col("source_id"),
        F.col("ra"),
        F.col("dec"),
        F.col("parallax"),
        F.col("parallax_error"),
        F.col("pmra"),
        F.col("pmdec"),
        F.col("ruwe"),
        F.col("fct_dt")
      )

      _ <- IcebergManager.syncIcebergOrCreate(
        spark = spark,
        tableName = tableName,
        df = dfFinal,
        partitionCol = "fct_dt",
        sortCols = Seq("source_id")
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
