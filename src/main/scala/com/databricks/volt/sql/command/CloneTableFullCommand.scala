package com.databricks.volt.sql.command

import com.databricks.volt.sql.command.CloneSchemaCommand.validate
import com.databricks.volt.sql.command.metadata.TableIdentifier
import com.databricks.volt.sql.utils.SQLUtils.{cloneMetadata, createOrReplaceObject, createRowWithSchema}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable


case class CloneTableFullCommand(
                                  cloneType: String,
                                  targetEntity: TableIdentifier,
                                  sourceEntity: TableIdentifier,
                                  tableProps: Seq[(String, String)],
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
    val createTable = createOrReplaceObject("TABLE", create = create, replace = replace, ifNotExists = ifNotExists)
    val data = try {
      val cloneCommand = new mutable.MutableList[String]
      cloneCommand += createTable
      cloneCommand += target
      cloneCommand += cloneType
      cloneCommand += "CLONE"
      cloneCommand += source
      if (tableProps.nonEmpty) {
        val props = tableProps
          .map(t => {
            val key = t._1.replaceAll("'", "\\'")
            val value = t._2.replaceAll("'", "\\'")
            s"'$key' = '$value'"
          })
          .mkString("(", ", ", ")")
        cloneCommand += s"TBLPROPERTIES $props"
      }
      if (managedLocation.nonEmpty) {
        cloneCommand += s"MANAGED LOCATION '$managedLocation'"
      }
      sparkSession.sql(cloneCommand.mkString(" ")).collect()
      val errorMsgs = cloneMetadata(sparkSession, sourceEntity, targetEntity)
      Array[Any](
        targetEntity.catalog.orNull,
        targetEntity.schema.orNull,
        targetEntity.table,
        if (errorMsgs.isEmpty) "OK" else "WITH_ERRORS",
        errorMsgs
      )
    } catch {
      case t: Throwable =>
        Array[Any](
          targetEntity.catalog.orNull,
          targetEntity.schema.orNull,
          targetEntity.table,
          "ERROR",
          Seq(t.getMessage)
        )
    }
    Seq(createRowWithSchema(schema, data:_*))

  }
}
