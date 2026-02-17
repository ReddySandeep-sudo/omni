package com.nielsen.sei.decisionEngine.utils

import com.nielsen.sei.decisionEngine.data.{Constants, RunArguments}
import com.typesafe.config.Config

import java.text.SimpleDateFormat
import scala.collection.mutable

object PatternToPath {
  var sortedPatternsMap: Seq[(String, String)] = _

  private def createPatternMap(runArgs: RunArguments, config: Config, dateTime: DateTime): Unit = {
    val product = runArgs.product
    val clientName = runArgs.clientName
    val file = runArgs.file
    val appId = runArgs.appID

    val currentHourEpoch: Long = runArgs.processingHourEpoch.toLong * 1000

    val patterns = config.getObject("pathPatterns")
    val isPartnerFileSpecific = config.getBoolean(s"$product.$clientName.$file.isPartnerFileSpecific")

    val patternsMap = mutable.LinkedHashMap[String, String]()

    patterns.forEach((k,v) => {
      val patternName: String = k
      val patternFormat = v.unwrapped().toString

      val formatter = new SimpleDateFormat(patternFormat)
      formatter.setTimeZone(Constants.utc)

      val value = formatter.format(currentHourEpoch)

      patternsMap += (patternName -> value)
    })

    val splitName = if (appId == null || appId.isBlank) "" else config.getString(s"$product.$clientName.$file.appIds.$appId")
    patternsMap += ("splitName" -> splitName)

    val partner = if (isPartnerFileSpecific) s"$clientName-$splitName" else clientName
    patternsMap += ("partner" -> partner)

    patternsMap += ("split" -> runArgs.appID)

    patternsMap += ("fhh" -> dateTime.firstHalfHour)
    patternsMap += ("shh" -> dateTime.secondHalfHour)

    val halfHour = if (dateTime.minute.toInt > 30) "%02d".format(dateTime.pHour.toInt * 2 + 1) else "%02d".format(dateTime.pHour.toInt * 2)
    patternsMap += ("hh" -> halfHour)

    patternsMap += ("quarter" -> dateTime.pQuarter)
    patternsMap += ("epoch" -> runArgs.processingHourEpoch)

    sortedPatternsMap = patternsMap.toSeq.sortBy{ case (key, _) => (-key.length, key) }
  }

  def getPatternPath(pattern: String, config: Config, runArgs: RunArguments, dateTime: DateTime): String = {
    if (sortedPatternsMap == null) {
      createPatternMap(runArgs, config, dateTime)
    }

    val hasEqualsTo = pattern.contains("=")
    var patternValue = pattern

    sortedPatternsMap.foreach(element => {
      if (hasEqualsTo) {
        patternValue = patternValue.replace(s"=${element._1}", s"=${element._2}")
      }
      else {
        patternValue = patternValue.replace(s"${element._1}", s"${element._2}")
      }
    })

    patternValue
  }
}
