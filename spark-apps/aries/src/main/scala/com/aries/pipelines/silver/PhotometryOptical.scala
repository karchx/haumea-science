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
    df.withColumn("fct_dt", F.make_date(F.col("year"), F.col("month"), F.col("day"))) 
  }

  def applyPhotometryOpticalExtract(df: DataFrame): DataFrame = {
    df.withColumns(Map(
      "catalog_name" -> F.lit("2MASS")
    ))
  }
}

object PhotometryOptical extends Pipeline {
  def merge(spark: SparkSession, tableName: String, stageTable: String) {
    spark.table(stageTable)
      .withColumn("rn", F.expr("ROW_NUMBER() OVER (PARTITION BY external_id ORDER BY fct_dt DESC)"))
      .filter(F.col("rn") === F.lit(1))
      .drop(F.col("rn"))
      .createOrReplaceTempView("dedup_stage")
  }

  override def runPipeline(spark: SparkSession, connection: Connection): IO[Unit] = {
    val today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

    val tableName = "lakehouse.silver.photometry_optical"

    for {
      _ <- IO.println(s"--- Init pipeline $tableName ---")
      metadata <- getMetadata(connection, "silver", tableName)
      _ <- IO.println(s"Metadata job: $metadata")
      currentOpt = metadata.get("current").flatMap(Option(_))
      genesisOpt = metadata.get("genesis").flatMap(Option(_))
      defaultDate = MetadataManager.getTargetDate(currentOpt, genesisOpt)
      _ <- IO.println(s"Metadata job: $defaultDate")


      bronzeRawDF <- IO.delay {
        spark.read
          .option("header", "true")
          .option("mergeSchema", "true")
          .parquet("s3a://gaia-source/bronze/aries_star_cluster/")
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
      _ <- IO.blocking {
        dfFinal.createOrReplaceTempView("stage_new")
        merge(spark, tableName, "stage_new")
        val metadataId = metadata.get("id").map(_.toString).getOrElse("photometry_optical")
        updateMetadata(connection, metadataId, java.sql.Date.valueOf(today))
      }
      _ <- IO.println(s"--- End pipeline $tableName ---")
    } yield ()
  }
}
