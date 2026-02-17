package com.nielsen.sei.decisionEngine.services.omni

import com.nielsen.sei.decisionEngine.data.RunArguments
import com.nielsen.sei.decisionEngine.utils.S3Util
import com.typesafe.config.{Config, ConfigObject}
import spray.json.JsObject

import java.io.File
import scala.collection.mutable
import scala.io.Source
import spray.json._

class OmniServices(s3Util: S3Util, runArgs: RunArguments) {
  private var etlJsonObject: JsObject = _

  private var etlConfig: Config = _
  var isIntegratedETLConfig: Boolean = _

  def init(config: Config): Unit = {
    val product = runArgs.product
    val client = runArgs.clientName
    val file = runArgs.file

    etlConfig = config.getConfig(s"$product.$client.$file.etlConfig")
    isIntegratedETLConfig = etlConfig.getBoolean("isIntegratedETLConfig")
  }

  def getSparkOptions(configObject: ConfigObject): mutable.Map[String, String] = {
    val readOptions = mutable.Map.empty[String, String]

    configObject.forEach { (k, v) =>
      val key: String = k
      val value: String = v.unwrapped().toString
      readOptions += (key -> value)
    }

    readOptions
  }

  def getETLConfig(configType: String, indentLevel: String): JsObject = {
    var bucket: String = null
    var prefix: String = null
    var fileName: String = null

    bucket = etlConfig.getString("etlConfigBucket")

    if (isIntegratedETLConfig) {
      prefix = etlConfig.getString("integratedConfigPrefix")
      fileName = etlConfig.getString("integratedConfigFileName")

      if (etlJsonObject != null) {
        println(s"${indentLevel}ConfigType: $configType, loading from previous config")
        return etlJsonObject.fields(configType).asJsObject
      }
    }
    else {
      prefix = etlConfig.getString(s"${configType}Prefix")
      fileName = etlConfig.getString(s"${configType}FileName")
    }

    println(s"${indentLevel}ConfigType: $configType, loading from `$bucket/$prefix/$fileName`")

    val contentString = if (bucket.startsWith("s3://")) readFromS3(bucket, s"$prefix/$fileName") else readFromLocal(s"$bucket/$prefix/$fileName")

    etlJsonObject = contentString.parseJson.asJsObject
    etlJsonObject.fields(configType).asJsObject
  }

  private def readFromS3(bucket: String, key: String): String = {
    val s3Client = s3Util.getS3Client(runArgs.region)

    s3Client.getObjectAsString(bucket.split("/")(2), key)
  }

  private def readFromLocal(path: String): String = {
    val sourceObject = Source.fromFile(new File(path))
    val content = sourceObject.mkString

    sourceObject.close()
    content
  }
}
