package com.aries.pipelines.silver

import com.aries.core.Pipeline
import com.aries.common.iceberg.IcebergManager
import com.aries.common.metadata.MetadataManager
import cats.effect.IO
import java.sql.Connection
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.{functions => F}
import cats.kernel.Monoid
import cds.healpix._

case class AstroBronze(source_id: String, j_m: Option[Double], h_m: Option[Double], ks_m: Option[Double], year: String, month: String, day: String, ra: Double, dec: Double)
case class AstroSilver(source_id: String, j_m: Option[Double], h_m: Option[Double], ks_m: Option[Double], fct_dt: String, healpix_index: Long)

object Transformations {

  def addHelpixIndex(ds: Dataset[AstroBronze]): Dataset[AstroSilver] = {
    import ds.sparkSession.implicits._

    ds.mapPartitions { partitionIterator =>
      val hpx = Healpix.getNested(6) // 6 = 64 = 2^depth
      partitionIterator.map { row =>
        val hpxIndex = hpx.hash(Math.toRadians(row.ra), Math.toRadians(row.dec))

        AstroSilver(
          source_id = row.source_id,
          j_m = row.j_m,
          h_m = row.h_m,
          ks_m = row.ks_m,
          fct_dt = s"${row.year}${row.month}${row.day}",
          healpix_index = hpxIndex
        )
      }
    }
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
        "day",
        "ra",
        "dec"
      )
      ariesClusterRawDF <- IcebergManager.readDf(spark, "s3a://gaia-source/bronze/aries_star_cluster/", Some(bronzeCols))
      ariesGaiaRawDF <- IcebergManager.readDf(spark, "s3a://gaia-source/bronze/aries_gaia/", Some(bronzeCols))

      bronzeRawDF <- IO.delay(ariesClusterRawDF.union(ariesGaiaRawDF).filter(F.col("source_id").isNotNull))
      typedBronzeDs = bronzeRawDF.as[AstroBronze]
      silverDs = Transformations.addHelpixIndex(typedBronzeDs)

      dfFinal = Transformations.applyPhotometryOpticalExtract(silverDs.toDF())
      _ <- IcebergManager.syncIcebergOrCreate(
        spark = spark,
        tableName = tableName,
        df = dfFinal,
        partitionCol = "healpix_index",
        sortCols = Seq("healpix_index")
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
