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

object DimSimbad extends Pipeline {
  def overwriteDynamicIO(df: DataFrame, tableName: String): IO[Unit] = IO.blocking {
    df.sortWithinPartitions("healpix_index")
      .writeTo(tableName)
      .overwritePartitions()
  }

  override def runPipeline(spark: SparkSession, connection: Connection): IO[Unit] = {
    import spark.implicits._
    val today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val tableName = "`datalake-haumea`.gold.dim_simbad"
    for {
      _ <- IO.println(s"--- Init pipeline $tableName ---")
      metadata <- getMetadata(connection, "gold", "dim_simbad")

      simbadClusterDf <- IO.delay(spark.table("`datalake-haumea`.silver.simbad_cluster"))

      dfTransform <- IO.delay {
        simbadClusterDf.dropDuplicates("main_id")
          .withColumn("bk_simbad", F.md5(
            F.concat_ws("|", F.col("main_id"), F.col("ra"), F.col("dec"))
          ))
      }

      dfFinal = dfTransform.select(
        F.col("main_id"),
        F.col("ra"),
        F.col("dec"),
        F.col("healpix_index"),
        F.col("bk_simbad"),
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
        val metadataId = metadata.get("id").map(_.toString).getOrElse("dim_simbad")
        IO.println(s"MetadataId: $metadataId")
        updateMetadata(connection, metadataId, java.sql.Date.valueOf(today))
      }
      _ <- IO.println(s"--- End pipeline $tableName ---")
    } yield ()
  }
}
