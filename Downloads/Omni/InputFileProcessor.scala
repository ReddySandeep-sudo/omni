package com.nielsen.sei.decisionEngine.traits

import com.nielsen.sei.decisionEngine.data.Constants
import com.typesafe.config.Config
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

trait InputFileProcessor extends Logging with Serializable {

  def process(config: Config): Unit

  def writeData(rawData: DataFrame, outputFilePath: String): Unit = {
    try {
      rawData
        .write
        .mode(Constants.SAVE_MODE)
        .parquet(outputFilePath)
      println("Output writing completed")

    }
    catch {
      case e: Exception =>
        println("Error occurred in writing output")
        throw e
    }
  }

}
