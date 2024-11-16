package com.databricks.extensions.sql

import com.databricks.extensions.sql.parser.SQLExtensionParser
import org.apache.spark.sql.{SparkSessionExtensions, SparkSessionExtensionsProvider}

class SQLExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { (_, parser) => new SQLExtensionParser(parser) }
  }
}
