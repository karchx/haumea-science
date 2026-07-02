package com.aries.transformations

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.{functions => F}
import cds.healpix._

object Astrophysics {
  private val calculateHpxIndex = F.udf { (ra: Double, dec: Double, depth: Int) =>
    val hpx = Healpix.getNested(6) // 6 = 64 = 2^depth
    hpx.hash(Math.toRadians(ra), Math.toRadians(dec))
  }

  private val angularDistance = F.udf { (raL: Double, decL: Double, raR: Double, decR: Double) =>
    val alpha = raL - raR
    val delta = decL - decR

    val alphaGroup = Math.pow((alpha * Math.cos(Math.toRadians(decL))), 2)
    val deltaGroup = Math.pow(delta, 2)
    Math.sqrt(alphaGroup + deltaGroup)
  }

  implicit class AstrophysicsDataFrame(df: DataFrame) {
    def withHealpixIndex(raCol: String, decCol: String, depth: Int = 6): DataFrame = {
      df.withColumn(
        "healpix_index",
        calculateHpxIndex(df(raCol), df(decCol), F.lit(depth))
      )
    }

    def withThresholdAngularD(raL: String, decL: String, raR: String, decR: String): DataFrame = {
      df.withColumn(
        "threshold_angular_d",
        angularDistance(df(raL), df(decL), df(raR), df(decR))
      )
    }
  }
}
