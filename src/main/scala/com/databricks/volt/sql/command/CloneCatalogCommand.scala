package com.databricks.volt.sql.command

import com.databricks.volt.sql.command.CloneSchemaCommand.validate
import com.databricks.volt.sql.command.metadata.{CatalogIdentifier, SchemaIdentifier, TableIdentifier}
import com.databricks.volt.sql.utils.SQLUtils.{cloneMetadata, createOrReplaceObject, createRowWithSchema}
import org.apache.spark.sql.{Row, SparkSession}

case class CloneCatalogCommand(
                                cloneType: String,
                                targetEntity: CatalogIdentifier,
                                sourceEntity: CatalogIdentifier,
                                managedLocation: String,
                                ifNotExists: Boolean,
                                isFull: Boolean
                              )
  extends BaseCommand(CloneSchemaCommand.schema) {

  validate(create = true, replace = false, ifNotExists = ifNotExists)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val source = sourceEntity.toEscapedString
    val target = targetEntity.toEscapedString
    try {
      val resultRows = cloneSchemas(sparkSession, source, target)
      if (isFull) {
        cloneAllMeta(sparkSession, resultRows)
      } else {
        resultRows
      }
    } catch {
      case t: Throwable => Seq(createRowWithSchema(schema, target, null, null, "ERROR", t.getMessage))
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
      val tableSchema = row.getAs[String]("table_schema")
      val sourceTabEntity = TableIdentifier(tableName,
        Some(tableSchema),
        Some(sourceEntity.catalog)
      )
      val targetTabEntity = TableIdentifier(
        tableName,
        Some(tableSchema),
        Some(targetEntity.catalog)
      )
      val errorMsgs: Seq[String] = cloneMetadata(sparkSession, sourceTabEntity, targetTabEntity)
      val statusMessages = row.getAs[Seq[String]]("status_messages")
      val status_messages: Seq[String] = if (statusMessages != null) {
        statusMessages ++ errorMsgs
      } else {
        errorMsgs
      }
      createRowWithSchema(schema,
        row.getAs[String]("table_catalog"),
        tableSchema,
        tableName,
        if (status_messages.isEmpty) "OK" else "WITH_ERRORS",
        status_messages
      )
    }

    resultRows.map(cloneRowMeta)
  }

  private def cloneSchemas(sparkSession: SparkSession, source: String, target: String) = {
    val sourceSchemas = sparkSession.sql(s"SHOW SCHEMAS IN $source")
      .where("databaseName NOT IN ('information_schema', '__databricks_internal')")
      .collect()
    if (sourceSchemas.isEmpty) {
      throw new IllegalArgumentException(s"Source catalog $source is empty")
    }
    val managedLocationSQL = if (managedLocation.nonEmpty) {
      s"MANAGED LOCATION '$managedLocation'"
    } else {
      managedLocation
    }
    val createCatalog = createOrReplaceObject("CATALOG", create = true, replace = false, ifNotExists = ifNotExists)
    sparkSession.sql(s"$createCatalog $target $managedLocationSQL".trim).collect()
    val catalogRows = Seq(createRowWithSchema(schema, targetEntity.catalog, null, null, "OK", Seq.empty[String]))
    catalogRows ++ sourceSchemas
      .map(_.getAs[String]("databaseName"))
      .flatMap(databaseName => {
        val fullTarget = SchemaIdentifier(databaseName, Option(targetEntity.catalog))
        val fullSource = SchemaIdentifier(databaseName, Option(sourceEntity.catalog))
        CloneSchemaCommand(cloneType, fullTarget, fullSource, "", ifNotExists = false, isFull = false)
          .run(sparkSession)
      })
  }
}
