package com.nielsen.sei.decisionEngine.services.omni

import org.apache.spark.sql.functions.{col, explode, expr, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}
import spray.json.DefaultJsonProtocol._
import spray.json.{JsArray, JsBoolean, JsObject, JsString}

import scala.collection.mutable

class Transformer(spark: SparkSession, metricUtil: OmniMetricUtil, countMap: mutable.Map[String, Long]) {
  def applyTransformation(transformConfig: JsObject, dataName: String, data: DataFrame): Seq[(String, DataFrame)] = {
    val transformations = transformConfig.fields("transformations").asInstanceOf[JsArray]

    transformations.elements.foldLeft(Seq((dataName, data))) { (seqOfDataFrames, element) => {
      val transformation = element.asJsObject
      val transformationType = transformation.fields("type").asInstanceOf[JsString].value.trim

      println(s"\t\ttype: $transformationType")

      transformationType match {
        case "split" => seqOfDataFrames.flatMap { case (_, transformingData) => split(transformation, transformingData) }
        case "club" => club(transformation, seqOfDataFrames)
        case _ =>
          seqOfDataFrames.map { case (name, transformingData) =>
            val transformedDF = transformationType match {
              case "filter" => applyFilter(transformation, transformingData)
              case "addColumn" => addColumn(transformation, transformingData)
              case "dropColumn" => dropColumn(transformation, transformingData)
              case "explode" => explodeColumns(transformation, transformingData)
              case "renameColumn" => renameColumn(transformation, transformingData)
              case "executeQuery" => executeQuery(transformation, transformingData)
              case "convertDataType" => convertDataType(transformation, transformingData)
              case "pushMetric" => pushMetric(transformation, transformingData)
              case "dropFirstRow" => dropFirstRow(transformingData)
              case "count" => count(name, transformingData)
              case _ => unknownTransformation(transformationType, transformingData)
            }
            (name, transformedDF)
          }
      }
    }}
  }

  def count(name: String, data: DataFrame): DataFrame = {
    val count = data.count()
    println(s"\t\t\tRecords count `$count` added to countMap with key `$name`")

    countMap.put(name, count)
    data
  }

  def renameColumn(transformation: JsObject, data: DataFrame): DataFrame = {
    val fieldsToRename = transformation.fields("fields").asInstanceOf[JsArray]

    val renamedDf = fieldsToRename.elements.foldLeft(data) { (currentDf, fieldElement) =>
      val fieldObject = fieldElement.asJsObject
      val oldColumnName = fieldObject.fields("currentColumnName").asInstanceOf[JsString].value.trim
      val newColumnName = fieldObject.fields("newColumnName").asInstanceOf[JsString].value.trim

      println(s"\t\t\tRenaming column: $oldColumnName -> $newColumnName")
      currentDf.withColumnRenamed(oldColumnName, newColumnName)
    }

    renamedDf
  }

  def executeQuery(transformation: JsObject, data: DataFrame): DataFrame = {
    val queries = transformation.fields("queries").asInstanceOf[JsArray]

    // Execute each query in sequence
    val finalResult = queries.elements.foldLeft(data) { (currentDf, queryElement) =>
      val queryObject = queryElement.asJsObject
      val query = queryObject.fields("query").asInstanceOf[JsString].value.trim
      println(s"\t\t\tQuery: `$query`")
      currentDf.createOrReplaceTempView("data")

      val result = spark.sql(query)

      spark.catalog.dropTempView("data")

      result
    }

    finalResult
  }

  def pushMetric(transformation: JsObject, data: DataFrame): DataFrame = {
    val datasetName = transformation.fields("datasetName").asInstanceOf[JsString].value.trim
    val metricName = transformation.fields("metricName").asInstanceOf[JsString].value.trim
    val metricMessage = transformation.fields("metricMessage").asInstanceOf[JsString].value.trim

    println(s"\t\t\tFetching number of record from countMap with key `$datasetName`")

    val count = countMap.getOrElse(datasetName, -1L)
    println(s"\t\t\tMetric name: $metricName")
    println(s"\t\t\tRecords count: $count")

    metricUtil.pushToPrometheus(metricName, count, metricMessage)

    data
  }

  def dropFirstRow(data: DataFrame): DataFrame = {
    val headerRow = data.first()
    data.filter(row => row != headerRow)
  }

  // $COVERAGE-OFF$
  def split(transformation: JsObject, data: DataFrame): Seq[(String, DataFrame)] = {
    val datasets = transformation.fields("datasets").asInstanceOf[JsArray]

    datasets.elements.flatMap { dataset =>
      val datasetObj = dataset.asJsObject
      val name = datasetObj.fields("name").asInstanceOf[JsString].value
      val hasTransformation = datasetObj.fields("hasTransformation").asInstanceOf[JsBoolean].value

      val filteredData = applyFilter(datasetObj, data)

      if (hasTransformation) {
        applyTransformation(datasetObj, name, data)
      }
      else {
        Seq((name, filteredData))
      }
    }
  }

  def club(transformation: JsObject, seqOfDataFrames: Seq[(String, DataFrame)]): Seq[(String, DataFrame)] = {
    val dataframes = transformation.fields("datasets").asInstanceOf[JsArray]

    val datasetsToRemove = dataframes.elements.flatMap { clubInfo =>
      clubInfo.asJsObject.fields("datasetsToClub").asInstanceOf[JsArray].elements.map(_.convertTo[String])
    }.toSet

    val clubbedDatasets = dataframes.elements.map { clubInfo =>
      val clubInfoObj = clubInfo.asJsObject
      val name = clubInfoObj.fields("name").convertTo[String]
      val datasetsToClub = clubInfoObj.fields("datasetsToClub").asInstanceOf[JsArray].elements.map(_.convertTo[String])

      val clubbedDF = datasetsToClub.flatMap { datasetName => seqOfDataFrames.find(_._1 == datasetName).map(_._2) }.reduce(_ union _)

      (name, clubbedDF)
    }

    seqOfDataFrames.filter(dataset => !datasetsToRemove.contains(dataset._1)) ++ clubbedDatasets
  }

  def addColumn(transformation: JsObject, data: DataFrame): DataFrame = {
    val fieldsToRename = transformation.fields("fields").asInstanceOf[JsArray]

    val updatedDf = fieldsToRename.elements.foldLeft(data) { (currentDf, fieldElement) =>
      val fieldObject = fieldElement.asJsObject
      val columnToAdd = fieldObject.fields("columnName").asInstanceOf[JsString].value.trim
      val valueType =  fieldObject.fields("valueType").asInstanceOf[JsString].value.trim
      val dataType = fieldObject.fields("dataType").asInstanceOf[JsString].value.trim.toLowerCase

      val newColumn: Column = valueType match {
        case "lit" =>
          val value = fieldObject.fields("value").asInstanceOf[JsString].value.trim
          lit(value)
        case "expression" =>
          val expression = fieldObject.fields("expression").asInstanceOf[JsString].value.trim
          expr(expression)
        case _ =>
          throw new IllegalArgumentException(s"Unsupported valueType: $valueType")
      }

      println(s"\t\t\tAdding column: $columnToAdd, data type: ${dataType}")
      currentDf.withColumn(columnToAdd, newColumn.cast(dataType))
    }

    updatedDf
  }

  def dropColumn(transformation: JsObject, data: DataFrame): DataFrame = {
    val fieldsToRename = transformation.fields("fields").asInstanceOf[JsArray]

    val updatedDf = fieldsToRename.elements.foldLeft(data) { (currentDf, fieldElement) =>
      val fieldObject = fieldElement.asJsObject
      val columnToDrop = fieldObject.fields("columnName").asInstanceOf[JsString].value.trim

      println(s"\t\t\tDropping column: `$columnToDrop`")
      currentDf.drop(columnToDrop)
    }

    updatedDf
  }

  def convertDataType(transformation: JsObject, data: DataFrame): DataFrame = {
    val fields = transformation.fields("fields").asInstanceOf[JsArray]

    val convertedData = fields.elements.foldLeft(data) { (currentDf, fieldElement) =>
      val fieldObject = fieldElement.asJsObject
      val columnName = fieldObject.fields("columnName").asInstanceOf[JsString].value
      val newDataType = fieldObject.fields("newDataType").asInstanceOf[JsString].value.toLowerCase

      val castColumn = newDataType match {
        case "integer" => functions.col(columnName).cast(IntegerType)
        case "long" => functions.col(columnName).cast(LongType)
        case "double" => functions.col(columnName).cast(DoubleType)
        case "float" => functions.col(columnName).cast(FloatType)
        case "boolean" => functions.col(columnName).cast(BooleanType)
        case "string" => functions.col(columnName).cast(StringType)
        case "date" => functions.to_date(functions.col(columnName))
        case "timestamp" => functions.to_timestamp(functions.col(columnName))
        case _ => throw new IllegalArgumentException(s"Unsupported data type '$newDataType' to apply convertDataType transformation")
      }

      println(s"\t\t\tConverting data type for column: $columnName, new data type: $castColumn")
      currentDf.withColumn(columnName, castColumn)
    }

    convertedData
  }

  def applyFilter(transformation: JsObject, data: DataFrame): DataFrame = {
    val filters = transformation.fields("filters").asInstanceOf[JsArray]

    val filterConditions = filters.elements.map { filterElement =>
      val filterObject = filterElement.asJsObject
      val filterType = filterObject.fields("type").asInstanceOf[JsString].value

      filterType match {
        case "expression" =>
          val condition = filterObject.fields("condition").asInstanceOf[JsString].value
          expr(condition)

        case "column" =>
          val columnName = filterObject.fields("columnName").asInstanceOf[JsString].value
          val operator = filterObject.fields("operator").asInstanceOf[JsString].value
          val value = filterObject.fields("value")

          operator match {
            case "<" => col(columnName) < value.convertTo[Double]
            case "<=" => col(columnName) <= value.convertTo[Double]
            case ">" => col(columnName) > value.convertTo[Double]
            case ">=" => col(columnName) >= value.convertTo[Double]
            case "=" | "==" => col(columnName) === value.convertTo[String]
            case "!=" => col(columnName) =!= value.convertTo[String]
            case "in" => col(columnName).isin(value.convertTo[Seq[String]]: _*)
            case "not in" => !col(columnName).isin(value.convertTo[Seq[String]]: _*)
            case "regex" => col(columnName).rlike(value.convertTo[String])
            case "between" =>
              val range = value.convertTo[Seq[Double]]
              col(columnName).between(range.head, range(1))
            case "is null" => col(columnName).isNull
            case "is not null" => col(columnName).isNotNull
            case _ => throw new IllegalArgumentException(s"Unsupported operator: $operator")
          }

        case _ => throw new IllegalArgumentException(s"Unsupported filter type: $filterType")
      }
    }

    val combinedFilter = filterConditions.reduce(_ && _)

    data.filter(combinedFilter)
  }

  def explodeColumns(transformation: JsObject, data: DataFrame): DataFrame = {
    val fields = transformation.fields("fields").asInstanceOf[JsArray]

    val explodedData = fields.elements.foldLeft(data) { (currentDf, fieldElement) =>
      val fieldObject = fieldElement.asJsObject
      val columnToExplode = fieldObject.fields("columnName").asInstanceOf[JsString].value.trim
      val explodedColumnNames = fieldObject.fields("explodedColumnNames").convertTo[List[String]]

      val tempDf = currentDf.withColumn("temp_exploded", explode(col(columnToExplode)))

      val newColumns = explodedColumnNames.zipWithIndex.map { case (colName, _) =>
        col("temp_exploded").getField(colName).as(colName)
      }

      // Add the new columns and drop the temporary and original columns
      tempDf.select(col("*") +: newColumns: _*).drop(columnToExplode, "temp_exploded")
    }

    explodedData
  }

  def unknownTransformation(transformationType: String, data: DataFrame): DataFrame = {
    println(s"No match found for transformation type: $transformationType")
    data
  }
}
