package com.aries.models.silver

case class GaiaSilver(
  source_id: Long,
  ra: Double,
  dec: Double,
  parallax: Double,
  parallax_error: Double,
  pmra: Double,
  pmdec: Double,
  ruwe: Double,
  fct_dt: String, 
  healpix_index: Long
)
