package com.databricks.extension.sql

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec._
import org.scalatest.matchers._

class SQLExtensionsTest extends AnyFlatSpec with should.Matchers {

  it should "work" in {
    val spark = SparkSession
      .builder()
      .appName("Test")
      .master("local[*]")
      .config("spark.sql.extensions", "com.databricks.extensions.sql.SQLExtensions")
      .getOrCreate()


//    spark.sql("create catalog as_catalog_clone DEEP clone as_catalog").show()
//    spark.sql("show tables extended where table_catalog = 'as_catalog'").show()
//    val spark = DatabricksSession.builder().remote().getOrCreate()
//    val df = spark.read.table("samples.nyctaxi.trips")
//    df.limit(5).show()
  }
}