package com.aries.transformations

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.{functions => F}
import cds.healpix._

object Astrophysics {
  private val calculateHpxIndex = F.udf { (ra: Double, dec: Double, depth: Int) =>
    val hpx = Healpix.getNested(6) // 6 = 64 = 2^depth
    hpx.hash(Math.toRadians(ra), Math.toRadians(dec))
  }

  implicit class AstrophysicsDataFrame(df: DataFrame) {
    def withHealpixIndex(raCol: String, decCol: String, depth: Int = 6): DataFrame = {
      df.withColumn(
        "healpix_index",
        calculateHpxIndex(df(raCol), df(decCol), F.lit(depth))
      )
    }
  }
}
