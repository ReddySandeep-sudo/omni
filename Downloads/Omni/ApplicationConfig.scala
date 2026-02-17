package com.nielsen.sei.decisionEngine.utils

import com.google.inject.Inject
import com.nielsen.sei.decisionEngine.data.Constants
import com.typesafe.config.{Config, ConfigFactory}

@Inject
class ApplicationConfig(s3Util: S3Util, location: String, region: String) extends Serializable {

  var appConfig: Config = null
  println("App conf location: " + location)

  def apply(): Config = {
    if (location.equals("decisionEngine.conf")) appConfig = ConfigFactory.load("decisionEngine.conf")
    else appConfig = ConfigFactory.parseString(read(location))
    appConfig
  }

  private def read(location: String): String = {
    val s3Client = s3Util.getS3Client(if (null==region || ""==region) Constants.AWSREGION_USEAST1 else region)
    val configContent = s3Client.getObjectAsString(location, Constants.DEFAULT_APPLICATION_CONF_FILENAME)
    configContent
  }
}
