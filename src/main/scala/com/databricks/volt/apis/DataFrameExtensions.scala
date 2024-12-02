package com.databricks.volt.apis

import org.apache.spark.sql.{DataFrame, functions}

object DataFrameExtensions {

  implicit class DataFrameImplicits(dataFrame: DataFrame) {

    def filterBadRecordsOn(tableName: String): DataFrame = {
      val checks = getTableConstraints(tableName)

      val checkWhen = checks.map(c => functions.when(functions.expr(c), null).otherwise(c))
      dataFrame
        .filter(checks.map(s => s"(NOT $s)").mkString(" OR "))
        .withColumn("violations", functions.filter(functions.array(checkWhen :_*), c => c.isNotNull))
    }

    def filterValidRecordsOn(tableName: String): DataFrame = {
      val checks = getTableConstraints(tableName)

      dataFrame
        .filter(checks.map(s => s"($s)").mkString(" AND "))
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
}
