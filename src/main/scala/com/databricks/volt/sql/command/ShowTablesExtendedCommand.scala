package com.databricks.volt.sql.command

import com.databricks.volt.fs.ReadFileSystem
import com.databricks.volt.sql.command.ShowTablesExtendedCommand.{schema, selectColNames, sizeSchema, toGb}
import com.databricks.volt.sql.utils.DeltaMetadata
import com.databricks.volt.sql.utils.SQLUtils.{createRowWithSchema, filterDeltaTables}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.{ArrayType, DoubleType, LongType, MapType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{Column, Row, SparkSession, functions}

import java.net.URI
import java.sql.Timestamp
import scala.jdk.CollectionConverters.asScalaIteratorConverter

object ShowTablesExtendedCommand {
  private[command] val sizeSchema: StructType = new StructType()
    .add("full_size_in_gb", DoubleType)
    .add("full_size_in_bytes", LongType)
    .add("last_snapshot_size_in_gb", DoubleType)
    .add("last_snapshot_size_in_bytes", LongType)
    .add("delta_log_size_in_gb", DoubleType)
    .add("delta_log_size_in_bytes", LongType)
  private[command] val schema: StructType = new StructType()
    .add("table_catalog", StringType)
    .add("table_schema", StringType)
    .add("table_name", StringType)
    .add("table_type", StringType)
    .add("data_source_format", StringType)
    .add("storage_path", StringType)
    .add("created", TimestampType)
    .add("created_by", StringType)
    .add("last_altered_by", StringType)
    .add("last_altered", TimestampType)
    .add("liquid_clustering_cols", ArrayType(StringType))
    .add("properties", MapType.apply(StringType, StringType))
    .add("size", sizeSchema)

  private val toGb = Math.pow(10, 9)

  private val nonPushableCols = Set(
    "liquid_clustering_cols",
    "size",
    "properties"
  )

  private val selectColNames = schema
    .filterNot(f => nonPushableCols.contains(f.name))
    .map(_.name)
    .map(functions.col)

}

case class ShowTablesExtendedCommand(filters: Column)
  extends BaseCommand(schema)  {

  override def run(spark: SparkSession): Seq[Row] = {
    val baseDf = spark.sql(filterDeltaTables)
      .select(selectColNames :_*)

    val list: java.util.List[Row] = try {
      val df = if (filters == null) baseDf else baseDf.where(filters)
      df.collectAsList()
    } catch {
      case _: Throwable =>
        throw new IllegalArgumentException(
          s"""
             |You can filter only for these columns:
             |${selectColNames.map(c => s"- `$c`").mkString("\n")}
             |But you provided the following filtering condition:
             |$filters
             |""".stripMargin)
    }

    val result: Iterator[Row] = list
      .parallelStream()
      .map(row => processRow(spark, row))
      .iterator()
      .asScala
      .asInstanceOf[Iterator[Row]]

    result.toSeq
  }

  private def processRow(spark: SparkSession, row: Row): Row = {
    try {
      val dataSourceFormat = row.getAs[String]("data_source_format")
      val isTableDelta = dataSourceFormat == "DELTA"

      val storagePath = row.getAs[String]("storage_path")
      val files = ReadFileSystem().read(URI.create(storagePath))
      val fullSize: Option[Long] = if (files.isEmpty) None else Option(files.map(_.size).sum)

      val (
        snapshotSize: Option[Long],
        lcCols: Seq[String],
        deltaLogSize: Option[Long],
        props: Map[String, String]
        ) =
        if (isTableDelta) {
          val deltaLogSize = Option(
            files
              .filter(_.path.contains("/_delta_log"))
              .map(_.size)
              .sum
          )
          val metadata = DeltaMetadata.forTable(
            spark,
            TableIdentifier(
              row.getAs[String]("table_name"),
              Option(row.getAs[String]("table_schema")),
              Option(row.getAs[String]("table_catalog"))
            )
          )
          (metadata.snapshotSize, metadata.lcCols, deltaLogSize, metadata.props)
        } else {
          (None, Seq.empty, None, Map.empty)
        }

      val size = createRowWithSchema(
        sizeSchema,
        fullSize.map(_ / toGb).orNull, // full_size_in_gb
        fullSize.orNull, // full_size_in_bytes
        snapshotSize.map(_ / toGb).orNull, // last_snapshot_size_in_gb
        snapshotSize.orNull, // last_snapshot_size_in_bytes
        deltaLogSize.map(_ / toGb).orNull, // delta_log_size_in_gb
        deltaLogSize.orNull // delta_log_size_in_bytes
      )
      createRowWithSchema(
        schema,
        row.getAs[String]("table_catalog"),
        row.getAs[String]("table_schema"),
        row.getAs[String]("table_name"),
        row.getAs[String]("table_type"),
        dataSourceFormat, // data_source_format
        storagePath, // storage_path
        row.getAs[Timestamp]("created"),
        row.getAs[String]("created_by"),
        row.getAs[String]("last_altered_by"),
        row.getAs[String]("last_altered"),
        lcCols, // liquid_clustering_cols
        props, // properties
        size
      )
    } catch {
      case ie: InterruptedException => {
        Thread.currentThread().interrupt()
        throw ie
      }
      case t: Throwable => throw t
    }
  }
}
