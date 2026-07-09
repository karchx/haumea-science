package com.aries.transformations

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.{functions => F}
import cds.healpix._

case class UnitVector(x: Double, y: Double, z: Double)

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

  private val cartesianSpatial = F.udf { (ra: Double, dec: Double) =>
    val raRadians = Math.toRadians(ra)
    val decRadians = Math.toRadians(dec)

    val x = Math.cos(raRadians) * Math.cos(decRadians)
    val y = Math.cos(decRadians) * Math.sin(raRadians)
    val z = Math.sin(decRadians)

    UnitVector ( x, y, z )
  }

  private val calculateDistancePc = F.udf { (parallax: Double) => 
    1000 / parallax
  }

  private val calculateIntrinsicMag = F.udf { (mx: Double, dc: Double) => 
    mx - (5 * Math.log10(dc)) + 5
  }

  implicit class AstrophysicsDataFrame(df: DataFrame) {
    def withHealpixIndex(raCol: String, decCol: String, depth: Int = 6): DataFrame = {
      df.withColumn(
        "healpix_index",
        calculateHpxIndex(df(raCol), df(decCol), F.lit(depth))
      )
    }

    def withUnitVectors(raCol: String, decCol: String): DataFrame = {
      val uv = cartesianSpatial(df(raCol), df(decCol))

      df.withColumns(Map(
        "x_unit" ->  uv.getField("x"),
        "y_unit" -> uv.getField("y"),
        "z_unit" -> uv.getField("z")
      ))
    }

    def withThresholdAngularD(raL: String, decL: String, raR: String, decR: String): DataFrame = {
      df.withColumn(
        "threshold_angular_d",
        angularDistance(df(raL), df(decL), df(raR), df(decR))
      )
    }

    def withDistanceParsecs(parallax: String, reliablePlx: String): DataFrame = {
      df.withColumn(
        "distance_pc",
        F.when(df(reliablePlx) === F.lit(true), calculateDistancePc(df(parallax))).otherwise(0.0)
      )
    }

    def withIntrinsicAbsoluteMagnitude(jM: String, jH: String, jKs: String, dc: String): DataFrame = {
      df.withColumns(Map(
        "abs_mag_j" -> calculateIntrinsicMag(df(jM), df(dc)),
        "abs_mag_h" -> calculateIntrinsicMag(df(jH), df(dc)),
        "abs_mag_ks" -> calculateIntrinsicMag(df(jKs), df(dc)),
      ))
    }
  }
}
