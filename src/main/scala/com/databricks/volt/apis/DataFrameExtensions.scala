package com.databricks.volt.apis

import com.databricks.volt.apis.model.ValidatedDataFrame
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, functions}

object DataFrameExtensions extends Logging {

  implicit class DataFrameImplicits(dataFrame: DataFrame) {

    def filterBadRecordsOn(tableName: String, checksOpt: Option[Seq[String]] = None): DataFrame = {
      val checks = checksOpt.getOrElse(getTableConstraints(tableName))

      val checkWhen = checks.map(c => functions.when(functions.expr(c), null).otherwise(c))
      dataFrame
        .filter(checks.map(s => s"(NOT $s)").mkString(" OR "))
        .withColumn("violations", functions.filter(functions.array(checkWhen :_*), c => c.isNotNull))
    }

    def filterValidRecordsOn(tableName: String, checksOpt: Option[Seq[String]] = None): DataFrame = {
      val checks = checksOpt.getOrElse(getTableConstraints(tableName))

      dataFrame
        .filter(checks.map(s => s"($s)").mkString(" AND "))
    }

    def validateForTable(tableName: String): ValidatedDataFrame = {
      val checkOpt = Some(getTableConstraints(tableName))
      ValidatedDataFrame(
        filterValidRecordsOn(tableName, checksOpt = checkOpt),
        filterBadRecordsOn(tableName, checksOpt=checkOpt)
      )
    }

    private def getTableConstraints(tableName: String) = {
      dataFrame.sparkSession
        .sql(s"DESCRIBE DETAIL $tableName")
        .select("properties")
        .collect()
        .head
        .getMap[String, String](0)
        .filterKeys(k => k.startsWith("delta.constraints."))
        .values
        .toSeq
    }
  }

  // Necessary Python bindings

  def filterBadRecordsOn(dataFrame: DataFrame, tableName: String): DataFrame = DataFrameImplicits(dataFrame)
    .filterBadRecordsOn(tableName)

  def filterValidRecordsOn(dataFrame: DataFrame, tableName: String): DataFrame = DataFrameImplicits(dataFrame)
    .filterValidRecordsOn(tableName)

  def validateForTable(dataFrame: DataFrame, tableName: String): ValidatedDataFrame = DataFrameImplicits(dataFrame)
    .validateForTable(tableName)
}
