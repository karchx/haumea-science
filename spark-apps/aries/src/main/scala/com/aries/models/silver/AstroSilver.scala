package com.aries.models.silver

case class AstroSilver(
  source_id: Long,
  j_m: Option[Double], 
  h_m: Option[Double], 
  ks_m: Option[Double], 
  fct_dt: String, 
  healpix_index: Long
)
