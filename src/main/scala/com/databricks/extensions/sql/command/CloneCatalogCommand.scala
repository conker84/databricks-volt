package com.databricks.extensions.sql.command

import com.databricks.extensions.sql.command.CloneSchemaCommand.validate
import com.databricks.extensions.sql.command.metadata.{CatalogIdentifier, SchemaIdentifier}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{Row, SparkSession}

case class CloneCatalogCommand(
                                cloneType: String,
                                targetEntity: CatalogIdentifier,
                                sourceEntity: CatalogIdentifier,
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
    try {
      val sourceSchemas = sparkSession.sql(s"SHOW SCHEMAS IN $source")
        .where("databaseName NOT IN ('information_schema', 'system')")
        .collect()
      if (sourceSchemas.isEmpty) {
        throw new IllegalArgumentException(s"Source catalog $source is empty")
      }
      val managedLocationSQL = if (managedLocation.nonEmpty) {
        s"MANAGED LOCATION '$managedLocation'"
      } else {
        managedLocation
      }
      sparkSession.sql(s"CREATE CATALOG $target $managedLocationSQL".trim)
      sourceSchemas
        .map(_.getAs[String]("databaseName"))
        .flatMap(databaseName => {
          val fullTarget = SchemaIdentifier(databaseName, Option(targetEntity.catalog))
          val fullSource = SchemaIdentifier(databaseName, Option(sourceEntity.catalog))
          CloneSchemaCommand(cloneType, fullTarget, fullSource, "", create = true, replace = false, ifNotExists = false)
            .run(sparkSession)
        })
    } catch {
      case t: Throwable => Seq(new GenericRowWithSchema(Array[Any](target, null, null, "ERROR", t.getMessage), schema))
    }
  }
}
