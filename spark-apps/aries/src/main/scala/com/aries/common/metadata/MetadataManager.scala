package com.aries.common.metadata

import java.time.LocalDate
import java.time.format.DateTimeFormatter


object MetadataManager {
  def getTargetDate(currentOpt: Option[Any], genesisOpt: Option[Any]): String = {
      val today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val targetDate = currentOpt.orElse(genesisOpt).orNull

      targetDate match {
        case d: java.sql.Date => d.toLocalDate.format(formatter)
        case s: java.sql.Timestamp => s.toLocalDateTime.toLocalDate.format(formatter)
        case s: String => s
        case _ => "2026-06-19"
      }
  }
}
