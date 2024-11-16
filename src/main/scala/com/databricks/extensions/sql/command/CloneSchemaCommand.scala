package com.databricks.extensions.sql.command

import com.databricks.extensions.sql.command.CloneSchemaCommand.validate
import com.databricks.extensions.sql.command.metadata.SchemaIdentifier
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object CloneSchemaCommand {
  private[command] val schema: StructType = new StructType()
    .add("table_catalog", StringType)
    .add("table_schema", StringType)
    .add("table_name", StringType)
    .add("status", StringType)
    .add("status_message", StringType)

  private[command] def validate(create: Boolean,
                                 replace: Boolean,
                                 ifNotExists: Boolean): Unit = {
    if (!create && ifNotExists) {
      throw new RuntimeException("You can use IF NOT EXISTS only with CREATE")
    }
    if (replace && ifNotExists) {
      throw new RuntimeException("You cannot use IF NOT EXISTS with REPLACE")
    }
  }
}

case class CloneSchemaCommand(
                               cloneType: String,
                               targetEntity: SchemaIdentifier,
                               sourceEntity: SchemaIdentifier,
                               managedLocation: String,
                               create: Boolean,
                               replace: Boolean,
                               ifNotExists: Boolean
                             )
  extends BaseCommand(CloneSchemaCommand.schema) {

  validate(create, replace, ifNotExists)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val source = sourceEntity.toEscapedString
    val target = targetEntity.toEscapedString
    val targetCatalog = targetEntity.catalog.getOrElse("")
    val targetDatabase = targetEntity.schema
    try {
      val sourceTables = sparkSession
        .sql(s"SHOW TABLES IN $source")
        .where("NOT isTemporary")
        .collect()
      if (sourceTables.isEmpty) {
        throw new IllegalArgumentException(s"Source schema $source is empty")
      }
      if (sourceEntity.schema != "default") {
        val managedLocationSQL = if (managedLocation.nonEmpty) {
          s"MANAGED LOCATION '$managedLocation'"
        } else {
          managedLocation
        }
        sparkSession.sql(s"CREATE SCHEMA $target $managedLocationSQL").collect()
      }
      sourceTables
        .map(_.getAs[String]("tableName"))
        .map(tableName =>
          try {
            sparkSession
              .sql(s"""
                      |CREATE TABLE $target.`$tableName`
                      |$cloneType CLONE $source.`$tableName`
                      |""".stripMargin)
              .collect()
            Array[Any](targetCatalog, targetDatabase, tableName, "OK", null)
          } catch {
            case t: Throwable => Array[Any](targetCatalog, targetDatabase, tableName, "ERROR", t.getMessage)
          }
        )
        .map(array => new GenericRowWithSchema(array, schema))
    } catch {
      case t: Throwable => Seq(new GenericRowWithSchema(Array[Any](targetCatalog, targetDatabase, null, "ERROR", t.getMessage), schema))
    }
  }
}
