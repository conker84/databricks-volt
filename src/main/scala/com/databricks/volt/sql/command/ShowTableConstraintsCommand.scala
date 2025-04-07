package com.databricks.volt.sql.command
import com.databricks.volt.sql.command.ShowTableConstraintsCommand.{fkPattern, mapper, pkPattern}
import com.databricks.volt.sql.command.metadata.TableIdentifier
import com.databricks.volt.sql.utils.SQLUtils
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import java.util.stream
import scala.jdk.CollectionConverters.asScalaIteratorConverter
import scala.util.matching.Regex

object ShowTableConstraintsCommand {

  private[command] val schema: StructType = new StructType()
    .add("type", StringType) // Enum, PRIMARY KEY, FOREIGN KEY, CHECK
    .add("name", StringType)
    .add("meta", StringType) // Is the JSON string representation

  private val pkPattern: Regex = """PRIMARY KEY\s*\((`[^`]+`(?:\s*,\s*`[^`]+`)*)\)""".r
  private val fkPattern: Regex = (
    """FOREIGN KEY\s*\((`[^`]+`(?:\s*,\s*`[^`]+`)*)\)\s+""" +
      """REFERENCES\s+`(\w+)`\.`(\w+)`\.`(\w+)`\s*""" +
      """\((`[^`]+`(?:\s*,\s*`[^`]+`)*)\)"""
    ).r

  private[command] val mapper: ObjectMapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build()

}


case class ShowTableConstraintsCommand(tableIdentifier: TableIdentifier) extends BaseCommand(ShowTableConstraintsCommand.schema) {

  override def run(sparkSession: SparkSession): Seq[Row] = sparkSession
    .sql(s"DESCRIBE TABLE EXTENDED ${tableIdentifier.toEscapedString}")
    .filter("data_type LIKE 'PRIMARY KEY%' OR data_type LIKE 'FOREIGN KEY%' OR col_name = 'Table Properties'")
    .selectExpr("col_name", "data_type", "substring(data_type, 1, 11) AS type")
    .collectAsList()
    .parallelStream()
    .flatMap((row: Row) => {
      val constrTypeOrProps: String = row.getAs[String]("type")
      val dataType: String = row.getAs[String]("data_type")
      val constrOrPropName: String = row.getAs[String]("col_name") // Constraint type, name, details
      val result: Array[Row] = constrTypeOrProps match {
        case "PRIMARY KEY" =>
          parsePKs(constrTypeOrProps, dataType, constrOrPropName)
        case "FOREIGN KEY" =>
          parseFKs(constrTypeOrProps, dataType, constrOrPropName)
        case _ =>
          parseChecks(constrTypeOrProps)
      }
      stream.Stream.of(result: _*)
    })
    .iterator()
    .asScala
    .asInstanceOf[Iterator[Row]]
    .toSeq

  private def parseChecks(constrTypeOrProps: String): Array[Row] = {
    constrTypeOrProps
      .substring(1, constrTypeOrProps.length - 1)
      .split(",")
      .map(prop => prop.trim().split("=") match {
        case Array(key, value) => (key, value)
      })
      .filter(prop => prop._1.startsWith("delta.constraints."))
      .map(prop => SQLUtils.createRowWithSchema(schema, "CHECK", prop._1.substring("delta.constraints.".length), prop._2))
  }

  private def parseFKs(
                        constrTypeOrProps: String,
                        dataType: String,
                        constrOrPropName: String
                      ): Array[Row] = {
    val meta: String = dataType match {
      case fkPattern(fkColsRaw, refCatalog, refSchema, refTable, refColsRaw) =>
        val fkCols = fkColsRaw.split(",").map(_.trim.stripPrefix("`").stripSuffix("`"))
        val pkCols = refColsRaw.split(",").map(_.trim.stripPrefix("`").stripSuffix("`"))

        if (fkCols.length != pkCols.length) {
          throw new IllegalArgumentException(
            s"Mismatch between FK and PK columns: ${fkCols.mkString(", ")} vs ${pkCols.mkString(", ")}"
          )
        }
        mapper.writeValueAsString(
          Map(
            "columns" -> fkCols,
            "refCatalog" -> refCatalog,
            "refSchema" -> refSchema,
            "refTable" -> refTable,
            "refColumns" -> pkCols,
          )
        )
      case _ =>
        throw new IllegalArgumentException(s"Cannot extract FK columns for table ${tableIdentifier.toEscapedString}")
    }
    Array(SQLUtils.createRowWithSchema(schema, constrTypeOrProps, constrOrPropName, meta))
  }

  private def parsePKs(constrTypeOrProps: String, dataType: String, constrOrPropName: String): Array[Row] = {
    val meta: String = pkPattern.findFirstMatchIn(dataType) match {
      case Some(m) =>
        val rawCols = m.group(1)
        mapper.writeValueAsString(rawCols.split(",").map(_.trim.stripPrefix("`").stripSuffix("`")).toSet)
      case None =>
        throw new IllegalArgumentException(s"Cannot extract PK columns for table ${tableIdentifier.toEscapedString}")
    }
    Array(SQLUtils.createRowWithSchema(schema, constrTypeOrProps, constrOrPropName, meta))
  }
}
