package com.nielsen.sei.decisionEngine.processors

import com.nielsen.sei.decisionEngine.data.RunArguments
import com.nielsen.sei.decisionEngine.services.omni.{OmniMetricUtil, OmniServices, Transformer}
import com.nielsen.sei.decisionEngine.traits.InputFileProcessor
import com.nielsen.sei.decisionEngine.utils.{AppUtils, DateTime, DateTimeUtil, PatternToPath, S3Util}
import com.typesafe.config.Config
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable

class OmniProcessor (spark: SparkSession, runArgs: RunArguments, s3Util: S3Util) extends InputFileProcessor {
  private var product: String = _
  private var client: String = _
  private var file: String = _
  private var region: String = _

  private var dateTimeObject: DateTime = _
  
  private var inputFileType: String = _
  private var outputFileType: String = _

  private var writeMode: String = _
  private var partitionBy: Seq[String] = _

  private var omniServices: OmniServices = _
  private var omniMetricUtil: OmniMetricUtil = _
  private var transformer: Transformer = _

  private var sparkReadOptions: mutable.Map[String, String] = _
  private var sparkWriteOptions: mutable.Map[String, String] = _

  var countMap: mutable.Map[String, Long] = _

  def process(config: Config): Unit = {
    println("Hello from Omni processor")

    // 1. Initialize the config & objects
    initialize(config, runArgs)

    // 2. Read input data
    val inputData = readData(config)

    // 3. Transformations
    val transformedData = transform(inputData)

    // 4. Write output data
    writeData(config, transformedData)
  }
  
  private def initialize(config: Config, runArgs: RunArguments): Unit = {
    println("\tInitializing the app")
    product = runArgs.product
    client = runArgs.clientName
    file = runArgs.file
    region = runArgs.region

    dateTimeObject = DateTimeUtil.epochToDateAndHour(runArgs.processingHourEpoch)

    inputFileType = config.getString(s"$product.$client.$file.inputFileType")
    outputFileType = config.getString(s"$product.$client.$file.outputFileType")

    val sparkReadOptionsObject = config.getObject(s"$product.$client.$file.sparkOptions.read.options")
    val sparkWriteOptionsObject = config.getObject(s"$product.$client.$file.sparkOptions.write.options")
    writeMode = config.getString(s"$product.$client.$file.sparkOptions.write.mode")
    partitionBy = config.getStringList(s"$product.$client.$file.sparkOptions.write.partitionBy").asScala

    countMap = mutable.Map()

    omniServices = new OmniServices(s3Util, runArgs)
    omniServices.init(config)

    omniMetricUtil = new OmniMetricUtil()
    omniMetricUtil.init(config, runArgs)

    transformer = new Transformer(spark, omniMetricUtil, countMap)

    sparkReadOptions = omniServices.getSparkOptions(sparkReadOptionsObject)
    sparkWriteOptions = omniServices.getSparkOptions(sparkWriteOptionsObject)

    println(s"\tInitialized, isIntegratedConfig: ${omniServices.isIntegratedETLConfig}")
  }
  
  private def readData(config: Config): DataFrame = {
    println("\n\tTask: Read")
    val inputSchemaConfig = omniServices.getETLConfig("inputSchema", "\t\t")

    val inputSchema: StructType = DataType.fromJson(inputSchemaConfig.compactPrint).asInstanceOf[StructType]

    val inputBucket = config.getString(s"$product.$client.$file.inputBucket")
    val inputFilePrefix = config.getString(s"$product.$client.$file.inputFilePrefix")
    val inputPathPattern = config.getString(s"$product.$client.$file.inputPathPattern")
    val inputPartitionPath = PatternToPath.getPatternPath(inputPathPattern, config, runArgs, dateTimeObject)

    val inputPath = s"$inputBucket/$inputFilePrefix/$inputPartitionPath"
    println(s"\t\tReading data from `$inputPath`")

    val data = spark.read
      .schema(inputSchema)
      .options(sparkReadOptions)
      .format(inputFileType.toLowerCase)
      .load(inputPath)

    val rawCount = data.count()
    countMap.put("raw", rawCount)

    data
  }

  private def transform(data: DataFrame): Seq[(String, DataFrame)] = {
    println("\n\tTask: Transform")
    val transformSchemaConfig = omniServices.getETLConfig("transformSchema", "\t\t")

    transformer.applyTransformation(transformSchemaConfig, "output", data)
  }

  private def writeData(config: Config, seqOfDataFrames: Seq[(String, DataFrame)]): Unit = {
    println("\n\t Task: Write")
    seqOfDataFrames.foreach {case (name, dataframe) =>
      println(s"\t\tData Name: $name")
      val schemaConfig = omniServices.getETLConfig(s"${name}Schema", "\t\t\t")

      val schema: StructType = DataType.fromJson(schemaConfig.compactPrint).asInstanceOf[StructType]
      val columns = schema.fieldNames

      val finalData = dataframe.select(columns.head, columns.tail: _*)

      val outputBucket = config.getString(s"$product.$client.$file.${name}Bucket")
      val outputFilePrefix = config.getString(s"$product.$client.$file.${name}FilePrefix")
      val outputPathPattern = config.getString(s"$product.$client.$file.${name}PathPattern")

      val outputPartitionPath = PatternToPath.getPatternPath(outputPathPattern, config, runArgs, dateTimeObject)

      val outputPath = s"$outputBucket/$outputFilePrefix/$outputPartitionPath"
      println(s"\t\t\tWriting data to `$outputPath`")

      var writer = finalData.write
        .mode(writeMode)
        .options(sparkWriteOptions)
        .format(outputFileType.toLowerCase)

      if (partitionBy.nonEmpty) writer = writer.partitionBy(partitionBy:_*)

      writer.save(outputPath)

      val rawCount = countMap.getOrElse("raw", 0L)
      val outputCount = countMap.getOrElse(name, 0L)

      if (!runArgs.isLocalMode) AppUtils.updateCountOnS3SuccessFile(outputPath, rawCount, outputCount, s3Util.getS3Client(runArgs.region), "\t\t\t")
    }
  }
}
