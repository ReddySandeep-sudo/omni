package com.nielsen.sei.decisionEngine.services.omni

import com.nielsen.sei.decisionEngine.data.RunArguments
import com.nielsen.seiUtils.MetricPublisher
import com.typesafe.config.Config

import scala.collection.mutable.ListBuffer

// $COVERAGE-OFF$
class OmniMetricUtil {
  private var mantaUrl: String = _
  private var keysList: ListBuffer[String] = _
  private var valuesList: ListBuffer[String] = _

  private var classicJobName: String = _
  private var jobName: String = _

  def init(config: Config, runArgs: RunArguments): Unit = {
    mantaUrl = config.getString("mantaUrl")

    val product = runArgs.product
    val client = runArgs.clientName
    val file = runArgs.file

    val labels = config.getObject(s"$product.$client.$file.metricLabels")
    val runArgsMap = runArgs.getClass.getDeclaredFields.map(_.getName).zip(runArgs.productIterator.to).toMap

    keysList = new ListBuffer[String]()
    valuesList = new ListBuffer[String]()

    labels.forEach((k,v) => {
      val key: String = k
      var value: String = v.unwrapped().toString

      if (key == "appId") value = config.getString(s"$product.$client.$file.appIds.${runArgs.appID}")
      else if (value.isEmpty) value =  runArgsMap(key).toString

      keysList += key
      valuesList += value
    })

    classicJobName = s"sei_de_${client}_${runArgs.convertFor}"
    jobName = s"decisionEngine_${product}_${client}_$file"

    if (runArgs.appID != null && runArgs.appID.nonEmpty) {
      val platform = config.getString(s"$product.$client.$file.appIds.${runArgs.appID}")

      classicJobName = s"${classicJobName}_$platform"
      jobName = s"${jobName}_$platform"
    }
  }

  def pushToPrometheus(metricName: String, metricValue: Double, metricDescription: String): Unit = {
    MetricPublisher.publishToPushGateway(mantaUrl, jobName, metricName, metricValue, metricDescription)(keysList:_*)(valuesList:_*)
    MetricPublisher.publishToPushGateway(mantaUrl, classicJobName, metricName, metricValue, metricDescription)(keysList:_*)(valuesList:_*)
  }
}
