package com.haumea.core

import org.apache.spark.sql.SparkSession

trait Pipeline {
  def run(spark: SparkSession): Unit

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()

    try {
      run(spark)
    } finally {
      spark.stop()
    }
  }
}
