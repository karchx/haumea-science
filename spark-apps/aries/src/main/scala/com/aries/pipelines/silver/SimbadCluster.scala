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
import com.aries.transformations.Astrophysics._

object SimbadCluster extends Pipeline {
  def overwriteDynamicIO(df: DataFrame, tableName: String): IO[Unit] = IO.blocking {
    df
      .sortWithinPartitions("healpix_index")
      .writeTo(tableName)
      .overwritePartitions()
  }

  override def runPipeline(spark: SparkSession, connection: Connection): IO[Unit] = {
    import spark.implicits._
    val today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val tableName = "`datalake-haumea`.silver.simbad_cluster"
    for {
      _ <- IO.println(s"--- Init pipeline $tableName ---")
      metadata <- getMetadata(connection, "silver", "simbad_cluster")

      bronzeCols = Seq(
        "main_id",
        "ra_deg",
        "dec_deg",
        "year",
        "month",
        "day"
      )

      bronzeRawDF <- IcebergManager.readDf(spark, "s3a://gaia-source/bronze/aries_simbad/", Some(bronzeCols))

      dfTransformed <- IO.delay {
          bronzeRawDF.withHealpixIndex(raCol = "ra_deg", decCol = "dec_deg")
            .withColumn("fct_dt", F.concat(F.col("year"), F.col("month"), F.col("day")))
      }

      dfFinal = dfTransformed.select(
        F.trim(F.col("main_id")).alias("main_id"),
        F.col("ra_deg").alias("ra"),
        F.col("dec_deg").alias("dec"),
        F.col("fct_dt"),
        F.col("healpix_index")
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
        val metadataId = metadata.get("id").map(_.toString).getOrElse("simbad_cluster")
        IO.println(s"MetadataId: $metadataId")
        updateMetadata(connection, metadataId, java.sql.Date.valueOf(today))
      }
      _ <- IO.println(s"--- End pipeline $tableName ---")
    } yield ()
  }
}
