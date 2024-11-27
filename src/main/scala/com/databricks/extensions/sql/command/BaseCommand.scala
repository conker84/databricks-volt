package com.databricks.extensions.sql.command

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.jdk.CollectionConverters.seqAsJavaListConverter

abstract class BaseCommand(schema: StructType) extends LeafRunnableCommand {

  override val output: Seq[Attribute] = DataTypeUtils.toAttributes(schema)

  override def canEqual(that: Any): Boolean = {
    this == that
  }

  def runDF(spark: SparkSession): DataFrame = spark.createDataFrame(run(spark).toList.asJava, schema)

}
