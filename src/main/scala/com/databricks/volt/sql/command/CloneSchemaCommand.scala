package com.databricks.volt.sql.command

import com.databricks.volt.sql.command.CloneSchemaCommand.validate
import com.databricks.volt.sql.command.metadata.{SchemaIdentifier, TableIdentifier}
import com.databricks.volt.sql.utils.SQLUtils.{cloneMetadata, createOrReplaceObject, createRowWithSchema, filterDeltaTables}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object CloneSchemaCommand {
  private[command] val schema: StructType = new StructType()
    .add("table_catalog", StringType)
    .add("table_schema", StringType)
    .add("table_name", StringType)
    .add("status", StringType)
    .add("status_messages", ArrayType(StringType))

  private[command] def validate(create: Boolean,
                                 replace: Boolean,
                                 ifNotExists: Boolean): Unit = {
    if (!create && ifNotExists) {
      throw new IllegalArgumentException("You can use IF NOT EXISTS only with CREATE")
    }
    if (replace && ifNotExists) {
      throw new IllegalArgumentException("You cannot use IF NOT EXISTS with REPLACE")
    }
  }
}

case class CloneSchemaCommand(
                               cloneType: String,
                               targetEntity: SchemaIdentifier,
                               sourceEntity: SchemaIdentifier,
                               managedLocation: String,
                               ifNotExists: Boolean,
                               isFull: Boolean
                             )
  extends BaseCommand(CloneSchemaCommand.schema) {

  validate(create = true, replace = false, ifNotExists = ifNotExists)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val targetCatalog = targetEntity.catalog.getOrElse("")
    val targetDatabase = targetEntity.schema
    try {
      val resultRows = cloneTables(sparkSession, sourceEntity, targetEntity)
      if (isFull) {
        cloneAllMeta(sparkSession, resultRows)
      } else {
        resultRows
      }
    } catch {
      case t: Throwable => Seq(createRowWithSchema(schema, targetCatalog, targetDatabase, null, "ERROR", Seq(t.getMessage)))
    }
  }

  private def cloneAllMeta(
                            sparkSession: SparkSession,
                            resultRows: Seq[Row]
                          ): Seq[Row] = {

    def cloneRowMeta(row: Row): Row = {
      val tableName = row.getAs[String]("table_name")
      val status = row.getAs[String]("status")
      if (tableName != null || status != "OK") return row
      val sourceTabEntity = TableIdentifier(tableName,
        Some(sourceEntity.schema),
        sourceEntity.catalog
      )
      val targetTabEntity = TableIdentifier(tableName,
        Some(targetEntity.schema),
        targetEntity.catalog
      )
      val errorMsgs: Seq[String] = cloneMetadata(sparkSession, sourceTabEntity, targetTabEntity)
      val statusMessage = row.getAs[Seq[String]]("status_messages")
      val status_messages: Seq[String] = if (statusMessage != null) {
        statusMessage ++ errorMsgs
      } else {
        errorMsgs
      }
      createRowWithSchema(
        schema,
        row.getAs[String]("table_catalog"),
        row.getAs[String]("table_schema"),
        tableName,
        if (status_messages.isEmpty) "OK" else "WITH_ERRORS",
        status_messages
      )
    }

    resultRows.map(cloneRowMeta)
  }

  private def cloneTables(
                           sparkSession: SparkSession,
                           sourceEntity: SchemaIdentifier,
                           targetEntity: SchemaIdentifier,
                         ): Seq[Row] = {
    val source = sourceEntity.toEscapedString
    val target = targetEntity.toEscapedString
    val targetCatalog = targetEntity.catalog.getOrElse("")
    val targetDatabase = targetEntity.schema
    val sourceTables = sparkSession.sql(filterDeltaTables)
      .where(
        s"""table_schema = '${sourceEntity.schema}'
           |AND table_catalog = ${sourceEntity.catalog.map(cat => s"'$cat'").getOrElse("current_catalog()")}
           |""".stripMargin)
      .collect()
    if (sourceTables.isEmpty) {
      throw new IllegalArgumentException(s"Source schema $source is empty")
    }
    val schemaCreatedRow = if (sourceEntity.schema != "default") {
      val managedLocationSQL = if (managedLocation.nonEmpty) {
        s"MANAGED LOCATION '$managedLocation'"
      } else {
        managedLocation
      }
      val createSchema = createOrReplaceObject("SCHEMA", create = true, replace = false, ifNotExists = ifNotExists)
      sparkSession.sql(s"$createSchema $target $managedLocationSQL").collect()
      createRowWithSchema(
        schema,
        targetCatalog,
        targetDatabase,
        null,
        "OK",
        Seq.empty[String]
      )
    } else {
      null
    }
    val rows = sourceTables
      .map(_.getAs[String]("table_name"))
      .map(tableName =>
        try {
          sparkSession
            .sql(
              s"""
                 |CREATE TABLE $target.`$tableName`
                 |$cloneType CLONE $source.`$tableName`
                 |""".stripMargin)
            .collect()
          Array[Any](targetCatalog, targetDatabase, tableName, "OK", Seq.empty[String])
        } catch {
          case t: Throwable => Array[Any](targetCatalog, targetDatabase, tableName, "ERROR", Seq(t.getMessage))
        }
      )
      .map(array => createRowWithSchema(schema, array:_*))
    if (schemaCreatedRow == null) {
      rows
    } else {
      Seq(schemaCreatedRow) ++ rows
    }
  }
}
