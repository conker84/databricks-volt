package com.databricks.extensions.sql.command

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.StructType

import scala.jdk.CollectionConverters.seqAsJavaListConverter

abstract class BaseCommand(schema: StructType) extends LeafRunnableCommand {

  override val output: Seq[Attribute] = schema
    .map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())

  override def canEqual(that: Any): Boolean = {
    this == that
  }

  def runDF(spark: SparkSession): DataFrame = spark.createDataFrame(run(spark).toList.asJava, schema)

}
