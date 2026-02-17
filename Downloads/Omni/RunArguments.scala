package com.nielsen.sei.decisionEngine.data

import com.google.inject.Inject
import com.typesafe.config.Config

import scala.collection.mutable
import scala.language.implicitConversions

@Inject
case class RunArguments(
                        isLocalMode: Boolean,
                        clientName: String,
                        convertFor: String,
                        appID: String,
                        processingHourEpoch: String,
                        appConfLocation: String,
                        env: String,
                        country: String = "all",
                        region: String = Constants.AWSREGION_USEAST1,
                        product: String = "",
                        file: String = ""
                       )

object RunArguments {

  def apply(inputMap: mutable.Map[String, String], config: Config): RunArguments = {
    val isLocalMode = if (inputMap.isEmpty) true else false
    println("Is local mode: " + isLocalMode)

    val clientName = if (isLocalMode) config.getString("clientName") else inputMap(Constants.CLIENT_NAME_KEY)
    val convertFor = if (isLocalMode) config.getString("convertFor") else inputMap(Constants.CONVERT_FOR_KEY)
    val appID = if (isLocalMode) config.getString(s"appID") else inputMap("appID")
    val processingHourEpoch = if (isLocalMode) config.getString("processingHourEpoch") else inputMap("processingHourEpoch")
    val appConfigPath = if (isLocalMode) "decisionEngine.conf" else inputMap(Constants.APP_CONF_LOCATION_KEY)
    val env = if (isLocalMode) "test" else inputMap(Constants.PROJECT_ENV)
    val country = if(inputMap.contains("country")) inputMap("country") else "all"
    val product = if(inputMap.contains("product")) inputMap("product") else "undefined"
    val file = if(inputMap.contains("file")) inputMap("file") else "undefined"
    val region = if (inputMap.contains("region")) inputMap("region") else Constants.AWSREGION_USEAST1

    if (clientName.isEmpty || convertFor.isEmpty || appConfigPath.isEmpty || processingHourEpoch.isEmpty) {
      throw new IllegalArgumentException("Blank parameter value passed.")
    }

    RunArguments(isLocalMode = isLocalMode, clientName = clientName, convertFor = convertFor, appID = appID, processingHourEpoch = processingHourEpoch, appConfLocation = appConfigPath, env = env, country = country, region= region, product = product, file = file)
  }
}

