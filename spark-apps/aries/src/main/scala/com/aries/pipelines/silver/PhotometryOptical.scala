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

object Transformations {
  def insertFactibleDate(df: DataFrame): DataFrame = {
    // save date: YYYYMMDD
    df.withColumn("fct_dt", F.concat(F.col("year"), F.col("month"), F.col("day"))) 
  }

  def applyPhotometryOpticalExtract(df: DataFrame): DataFrame = {
    df.withColumns(Map(
      "catalog_name" -> F.lit("2MASS")
    ))
  }
}

object PhotometryOptical extends Pipeline {
  def overwriteDynamicIO(df: DataFrame, tableName: String): IO[Unit] = IO.blocking {
    df
      .withColumn("rn", F.expr("ROW_NUMBER() OVER (PARTITION BY external_id ORDER BY fct_dt DESC)"))
      .filter(F.col("rn") === F.lit(1))
      .drop(F.col("rn"))
      .writeTo(tableName)
      .overwritePartitions()
  }

  override def runPipeline(spark: SparkSession, connection: Connection): IO[Unit] = {
    val today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

    val tableName = "`datalake-haumea`.silver.photometry_optical"

    for {
      _ <- IO.println(s"--- Init pipeline $tableName ---")
      metadata <- getMetadata(connection, "silver", "photometry_optical")
      _ <- IO.println(s"Metadata job: $metadata")
      currentOpt = metadata.get("current").flatMap(Option(_))
      genesisOpt = metadata.get("genesis").flatMap(Option(_))
      defaultDate = MetadataManager.getTargetDate(currentOpt, genesisOpt)
      _ <- IO.println(s"Metadata job: $defaultDate")

      bronzeCols = Seq(
        "source_id",
        "j_m",
        "h_m",
        "ks_m",
        "year",
        "month",
        "day"
      )
      ariesClusterRawDF <- IcebergManager.readDf(spark, "s3a://gaia-source/bronze/aries_star_cluster/", Some(bronzeCols))
      ariesGaiaRawDF <- IcebergManager.readDf(spark, "s3a://gaia-source/bronze/aries_gaia/", Some(bronzeCols))

      bronzeRawDF <- IO.delay {
        ariesClusterRawDF.union(ariesGaiaRawDF)
      }

      silverDf = Transformations.insertFactibleDate(bronzeRawDF).select(
        F.col("source_id").alias("external_id"),
        F.col("j_m"),
        F.col("h_m"),
        F.col("ks_m"),
        F.col("fct_dt")
      )

      dfFinal = Transformations.applyPhotometryOpticalExtract(silverDf)
      _ <- IcebergManager.syncIcebergOrCreate(
        spark = spark,
        tableName = tableName,
        df = dfFinal,
        partitionCol = "fct_dt",
        sortCols = Seq("external_id")
      )
      _ <- overwriteDynamicIO(dfFinal, tableName)

      _ <- IO.blocking {
        val metadataId = metadata.get("id").map(_.toString).getOrElse("photometry_optical")
        updateMetadata(connection, metadataId, java.sql.Date.valueOf(today))
      }
      _ <- IO.println(s"--- End pipeline $tableName ---")
    } yield ()
  }
}
