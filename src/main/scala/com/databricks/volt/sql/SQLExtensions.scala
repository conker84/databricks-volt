package com.databricks.volt.sql

import com.databricks.volt.sql.parser.SQLExtensionParser
import org.apache.spark.sql.{SparkSessionExtensions, SparkSessionExtensionsProvider}

class SQLExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(volt: SparkSessionExtensions): Unit = {
    volt.injectParser { (_, parser) => new SQLExtensionParser(parser) }
  }
}
