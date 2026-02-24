package com.nielsen.sei.decisionEngine.utils

import com.google.inject.Inject
import com.nielsen.sei.decisionEngine.data.RunArguments
import com.nielsen.seiUtils.MetricPublisher
import com.typesafe.config.Config

import scala.collection.mutable.ListBuffer

@Inject
class MetricUtil(config: Config, runArgs: RunArguments) extends Serializable {
  private val mantaUrl: String = config.getString("mantaUrl")
  private val (keysList, valuesList) = getLabels

  private def getLabels: (ListBuffer[String], ListBuffer[String]) = {
    val labels = config.getObject(s"${runArgs.clientName}.${runArgs.convertFor}.metricLabels")
    val runArgsMap = runArgs.getClass.getDeclaredFields.map(_.getName).zip(runArgs.productIterator.to).toMap

    val keysList = new ListBuffer[String]()
    val valuesList = new ListBuffer[String]()

    labels.forEach((k,v) => {
      val key: String = k
      var value: String = v.unwrapped().toString

      if (key == "appId") value = config.getString(s"${runArgs.clientName}.${runArgs.convertFor}.appIDs.${runArgs.appID}")
      else if (value.isEmpty) value =  runArgsMap(key).toString

      keysList += key
      valuesList += value
    })

    (keysList, valuesList)
  }

  def pushToPrometheus(jobName: String, metricName: String, metricValue: Double, metricDescription: String): Unit = {
    MetricPublisher.publishToPushGateway(mantaUrl, jobName, metricName,
      metricValue, metricDescription)(keysList:_*)(valuesList:_*)
  }
}