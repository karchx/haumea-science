package com.haumea.pipelines

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F}

object PSRawNasa {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("PSRawNasa")
      .getOrCreate()

    try {
      val rawPs = spark.read
        .option("multiline", "true")
        .option("recursiveFileLookup", "true")
        .json("s3a://gaia-source/raw/ps/")

      val current_timestamp = F.current_timestamp()

      val dfTransformed = rawPs.withColumns(Map(
        "ingestion_hash" -> F.regexp_extract(F.input_file_name(), "raw/ps/([^/]+)", 1),
        "year" -> F.year(F.lit(current_timestamp)),
        "month" -> F.month(F.lit(current_timestamp)),
        "day" -> F.dayofmonth(F.lit(current_timestamp))
      ))

      dfTransformed.write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("year", "month", "day")
        .parquet("s3a://gaia-source/bronze/ps/")
    } finally {
      spark.stop()
    }
  }
}
