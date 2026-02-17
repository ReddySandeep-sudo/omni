package com.nielsen.sei.decisionEngine.utils


import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{CopyObjectRequest, ObjectMetadata}
import com.nielsen.sei.decisionEngine.data.Constants.HEMOptOut
import com.nielsen.sei.decisionEngine.data.{Constants, SchemaDetails}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import com.typesafe.config.Config



case class DateTime(
                     pDate: String,
                     pYear: String,
                     pMonth: String,
                     pDay: String,
                     pHour: String,
                     pQuarter: String,
                     firstHalfHour: String = "-1",
                     secondHalfHour: String = "-1",
                     minute: String = "1"
                   )

object HemObfuscator{
  def readHEMOptOutJson(sparkSession: SparkSession, HEMOptOutRefrencePath: String): Dataset[HEMOptOut] = {
    println(s"Reading HEMOptOut Files :$HEMOptOutRefrencePath")
    import sparkSession.implicits._
    val HEMOptOutDF = sparkSession.read.json(HEMOptOutRefrencePath).as[HEMOptOut]
    HEMOptOutDF
  }

  def obfuscate(spark:SparkSession,df:DataFrame,config: Config, configKey: String): DataFrame = {

    val hemObfuscationEnabled = config.getBoolean(s"$configKey.hemObfuscationEnabled")

    if (hemObfuscationEnabled && config.hasPath(s"$configKey.hempath") &&
      config.hasPath(s"$configKey.mainDfJoinColumn")
      && config.hasPath(s"$configKey.hemDfJoinColumn")) {

      val dfJoincolumn = config.getString(s"$configKey.mainDfJoinColumn")
      val HemJoincolumn = config.getString(s"$configKey.hemDfJoinColumn")
      val inputLocation = config.getString(s"$configKey.hempath")

      try {
        val hemDf = readHEMOptOutJson(spark, inputLocation)
        val HEMOptOutdf = hemDf.select(HemJoincolumn)


        df.join(HEMOptOutdf, df(dfJoincolumn) === HEMOptOutdf(HemJoincolumn), "left")
          .withColumn(dfJoincolumn, when(col(HemJoincolumn).isNotNull, lit(null)).otherwise(col(dfJoincolumn)))
          .drop(HemJoincolumn)
      } catch {
        case e: Exception => println(s"Error: ${e.getMessage}. Skipping")
          df
      }
    }
    else
    {
      println("HEM obfuscation disabled or config missing. Skipping.")
      df
    }
  }
}

object DateTimeUtil {
  def epochToDateAndHour(epochMillis: String): DateTime = {
    val currentHourEpoch: Long = epochMillis.toLong * 1000
    val date = Constants.timeStamp_f.format(currentHourEpoch)
    val year = Constants.year_f.format(currentHourEpoch)
    val month = Constants.month_f.format(currentHourEpoch)
    val day = Constants.date_f.format(currentHourEpoch)
    val hour = Constants.hour_f.format(currentHourEpoch)
    val firstHalfHour = "%02d".format(hour.toInt * 2)
    val secondHalfHour = "%02d".format(hour.toInt * 2 + 1)
    val minute = Constants.min_f.format(currentHourEpoch)
    val quarter = "0".concat(((minute.toInt / 15) + 1).toString)

    DateTime(date, year, month, day, hour, quarter, firstHalfHour, secondHalfHour, minute)
  }

  def dateToEpoch(): String = {
    import java.time._
    val currentTime: LocalDateTime = LocalDateTime.now()
    val currentTimeInSeconds = currentTime.toEpochSecond(ZoneOffset.UTC)
    String.valueOf(currentTimeInSeconds)
  }
}

object AppUtils{
  def updateCountOnS3SuccessFile(outputFilePath: String, rawCount: Long, processedCount: Long, s3Client: AmazonS3, indent: String = ""): Unit = {
    val s3Details: Array[String] = outputFilePath.replace("s3a://", "").split("/", 2)
    val s3Bucket = s3Details(0)
    val keyName = s3Details(1) + "/_SUCCESS"

    val metadata: ObjectMetadata = s3Client.getObjectMetadata(s3Bucket, keyName)
    metadata.addUserMetadata("raw-count", rawCount.toString)
    metadata.addUserMetadata("processed-count", processedCount.toString)

    val request: CopyObjectRequest = new CopyObjectRequest(s3Bucket, keyName, s3Bucket, keyName)
      .withSourceBucketName(s3Bucket)
      .withSourceKey(keyName)
      .withNewObjectMetadata(metadata)
    s3Client.copyObject(request)

    println(s"${indent}Updated _SUCCESS file with metadata <raw-count: ${rawCount}; processed-count: ${processedCount}>")
  }

  def readData(sparkSession: SparkSession, location: String, inputSchema: StructType, fileType: String, fileExtension:String, withHeaderRow: Boolean): (DataFrame, Row) = {
    if (null != fileType && ("json".equals(fileType.toLowerCase()) || "parquet".equals(fileType.toLowerCase()))) {
      var inputData = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema = if (withHeaderRow) StructType(inputSchema.fields ++ SchemaDetails.metaSchema.fields) else inputSchema)

      try {
         inputData = sparkSession
          .read
          .schema(if (withHeaderRow) StructType(inputSchema.fields ++ SchemaDetails.metaSchema.fields) else inputSchema)
          .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
          .format(fileType.toLowerCase())
          .load(if (null != fileExtension && fileExtension.nonEmpty) s"$location*.$fileExtension" else s"$location*")
      } catch {
        case e: AnalysisException =>
          e.printStackTrace()
      }

      if (withHeaderRow) {
        val metaDF = inputData.limit(1)
        (inputData.exceptAll(metaDF), metaDF.first())
      }
      // $COVERAGE-OFF$
      else {
        val manifest = sparkSession.read.schema(SchemaDetails.manifestFileSchema).json(location + "*_manifest")
        val metaDF = SchemaDetails.metaSchema.fields.foldLeft(manifest)(
          (df, field) => df.withColumn(field.name, col(s"meta.${field.name}"))
        )
        (inputData, metaDF.first())
      }
    }
    else {
      throw new Exception("Invalid Config for filetype")
    }
    // $COVERAGE-ON$
  }
}

