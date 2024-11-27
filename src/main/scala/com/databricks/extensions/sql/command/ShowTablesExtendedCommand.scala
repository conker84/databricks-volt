package com.databricks.extensions.sql.command

import com.databricks.extensions.fs.ReadFileSystem
import com.databricks.extensions.sql.command.ShowTablesExtendedCommand.{filterStar, metadataSchema, nonPushableCols, selectColNames, toGb, schema}
import com.databricks.extensions.sql.utils.DeltaMetadataUtils
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, DoubleType, LongType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{Column, Row, SparkSession, functions}

import java.net.URI
import java.sql.Timestamp
import scala.jdk.CollectionConverters.{asScalaIteratorConverter, seqAsJavaListConverter}

object ShowTablesExtendedCommand {
  private val metadataSchema: StructType = new StructType()
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
    .add("last_snapshot_size_in_gb", DoubleType)
    .add("last_snapshot_size_in_bytes", LongType)
    .add("full_size_in_gb", DoubleType)
    .add("full_size_in_bytes", LongType)
    .add("metadata", metadataSchema)

  private val toGb = Math.pow(10, 9)

  private val nonPushableCols = Set(
    "liquid_clustering_cols",
    "last_snapshot_size_in_gb",
    "last_snapshot_size_in_bytes",
    "full_size_in_gb",
    "full_size_in_bytes",
    "metadata"
  )

  private val selectColNames = schema
    .filterNot(f => nonPushableCols.contains(f.name))
    .map(_.name)
    .map(functions.col)

  val filterStar: Column = functions.col("*")
}

case class ShowTablesExtendedCommand(filters: Column)
  extends BaseCommand(schema)  {

  override def run(spark: SparkSession): Seq[Row] = {
    val baseDf = spark.sql(
      """
        |SELECT *
        |FROM system.information_schema.tables
        |WHERE table_type NOT IN ('VIEW', 'FOREIGN')
        |AND table_schema <> 'information_schema'
        |AND table_catalog NOT IN ('system', '__databricks_internal')
        |AND data_source_format NOT IN ('UNKNOWN_DATA_SOURCE_FORMAT', 'DELTASHARING')
        |""".stripMargin)
      .select(selectColNames :_*)

    val (list: java.util.List[Row], hasError: Boolean) = try {
      (baseDf
        .where(filters)
        .collectAsList(), false)
    } catch {
      case _: Throwable =>
        logWarning(
          s"""
            |We cannot push down the filters `$filters` properly.
            |In order to speed up the query execution please check, and exclude, one of the following:
            |${nonPushableCols.map(c => s"- `$c`").mkString("\n")}
            |""".stripMargin)
        (baseDf.collectAsList(), filters != filterStar)
    }

    val result: Iterator[Row] = list
      .parallelStream()
      .map(row => {
        val dataSourceFormat = row.getAs[String]("data_source_format")
        val isTableDelta = dataSourceFormat == "DELTA"

        val storagePath = row.getAs[String]("storage_path")
        val files = ReadFileSystem().read(URI.create(storagePath))
        val fullSize: Option[Long] = if (files.isEmpty) None else Option(files.map(_.size).sum)

        val (snapshotSize: Option[Long], lcCols: Seq[String], deltaLogSize: Option[Long]) =
          if (isTableDelta) {
            val deltaLogSize = Option(
              files
              .filter(_.path.contains("/_delta_log"))
              .map(_.size)
              .sum
            )
            val (snapshotSize, lcCols) = DeltaMetadataUtils.forTable(
              spark,
              TableIdentifier(
                row.getAs[String]("table_name"),
                Option(row.getAs[String]("table_schema")),
                Option(row.getAs[String]("table_catalog"))
              )
            )
            (snapshotSize, lcCols, deltaLogSize)
          } else {
            (None, Seq.empty, None)
          }

        val metadata = new GenericRowWithSchema(
          Array(
            deltaLogSize.map(_ / toGb).orNull,
            deltaLogSize.orNull,
          ),
          metadataSchema)
        val data: Array[Any] = Array(
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
          snapshotSize.map(_ / toGb).orNull, // last_snapshot_size_in_gb
          snapshotSize.orNull, // last_snapshot_size_in_bytes
          fullSize.map(_ / toGb).orNull, // full_size_in_gb
          fullSize.orNull, // full_size_in_bytes
          metadata
        )
        new GenericRowWithSchema(data, schema)
      })
      .iterator()
      .asScala
      .asInstanceOf[Iterator[Row]]

    if (hasError) {
      spark.createDataFrame(result.toList.asJava, schema)
        .where(filters)
        .collect()
        .toSeq
    } else {
      result.toSeq
    }
  }
}
