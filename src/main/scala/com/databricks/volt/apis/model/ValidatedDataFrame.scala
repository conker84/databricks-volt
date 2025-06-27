package com.databricks.volt.apis.model

import org.apache.spark.sql.DataFrame

case class ValidatedDataFrame(valid: DataFrame, invalid: DataFrame)
