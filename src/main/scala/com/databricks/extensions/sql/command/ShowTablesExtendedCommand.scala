package com.databricks.extensions.sql.command

import com.databricks.extensions.fs.MultiCloudFSReader
import com.databricks.extensions.sql.command.ShowTablesExtendedCommand.{clusteringColumnInfoClazz, deltaLogClazz, extractLogicalNamesMethod, forTableMethod}
import com.databricks.extensions.sql.parser.ReflectionUtils
import com.databricks.extensions.sql.utils.CatalystUtils
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, DoubleType, LongType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{Row, SparkSession}

import java.net.URI
import java.sql.Timestamp
import scala.jdk.CollectionConverters.{asScalaIteratorConverter, seqAsJavaListConverter}
import scala.util.Try

object ShowTablesExtendedCommand {
  private[command] val schema: StructType = new StructType()
    .add("table_catalog", StringType)
    .add("table_schema", StringType)
    .add("table_name", StringType)
    .add("table_type", StringType)
    .add("storage_path", StringType)
    .add("created", TimestampType)
    .add("created_by", StringType)
    .add("liquid_clustering_cols", ArrayType(StringType))
    .add("last_altered_by", StringType)
    .add("last_altered", TimestampType)
    .add("size_in_gb", DoubleType)
    .add("size_in_bytes", LongType)
    .add("full_size_in_gb", DoubleType)
    .add("full_size_in_bytes", LongType)

  // TODO move this to method handles
  // and add a proper validation for class not present
  private val deltaLogClazz = ReflectionUtils.forName("com.databricks.sql.transaction.tahoe.DeltaLog")
  private val clusteringColumnInfoClazz = ReflectionUtils.forName("com.databricks.sql.io.skipping.liquid.ClusteringColumnInfo")
  private val snapshotClazz = ReflectionUtils.forName("com.databricks.sql.transaction.tahoe.Snapshot")
  private val forTableMethod = if (deltaLogClazz != null)
    deltaLogClazz.getMethod("forTable", classOf[SparkSession], classOf[TableIdentifier])
  else
    null
  private val extractLogicalNamesMethod = if (clusteringColumnInfoClazz != null)
    clusteringColumnInfoClazz.getMethod("extractLogicalNames", snapshotClazz)
  else
    null
}

case class ShowTablesExtendedCommand(plainFiltersOpt: Option[String])
  extends BaseCommand(ShowTablesExtendedCommand.schema)  {

  override def run(spark: SparkSession): Seq[Row] = {
    val plainFilters = plainFiltersOpt.getOrElse("")
    val parsedFilter = CatalystUtils.parsePredicates(spark, plainFiltersOpt.toSeq)
    val filteredCols = CatalystUtils.extractAttributes(parsedFilter)

    val schemaCols = schema.fieldNames.toSet

    val notInSchema = filteredCols.diff(schemaCols)
    if (notInSchema.nonEmpty) {
      throw new RuntimeException(
        s"""
          |The following columns are not available [${notInSchema.mkString(",")}]
          |Table schema is [${schemaCols.mkString(",")}]
          |""".stripMargin)
    }

    val baseDf = spark.sql(
      """
        |SELECT *
        |FROM system.information_schema.tables
        |WHERE table_type NOT IN ('VIEW', 'FOREIGN')
        |AND table_schema <> 'information_schema'
        |AND table_catalog NOT IN ('system', '__databricks_internal')
        |AND data_source_format NOT IN ('UNKNOWN_DATA_SOURCE_FORMAT', 'DELTASHARING')
        |""".stripMargin)

    val (list: java.util.List[Row], hasError: Boolean) = try {
      (baseDf
        .where(plainFilters)
        .collectAsList(), false)
    } catch {
      case _: Throwable => (baseDf.collectAsList(), plainFilters.nonEmpty)
    }

    val result: Iterator[Row] = list
      .parallelStream()
      .map(row => {
        val (size: Option[Long], lcCols: Seq[String]) = try {
          if (row.getAs[String]("data_source_format") == "DELTA") {
            val ret: AnyRef = forTableMethod.invoke(null, spark,
              TableIdentifier(
                row.getAs[String]("table_name"),
                Option(row.getAs[String]("table_schema")),
                Option(row.getAs[String]("table_catalog"))
              )
            )
            val snapshot = ret.getClass
              .getMethod("snapshot")
              .invoke(ret)
            val size = Try(
              snapshot.getClass
                .getMethod("sizeInBytes")
                .invoke(snapshot)
                .asInstanceOf[Long]
            )
              .map(Option.apply)
              .getOrElse(None)
            val lcCols = Try(
              extractLogicalNamesMethod
                .invoke(clusteringColumnInfoClazz, snapshot)
                .asInstanceOf[Seq[String]]
            )
              .getOrElse(Seq.empty[String])
            (size, lcCols)
          } else {
            (None, Seq.empty[String])
          }
        } catch {
          case _: Throwable => (None, Seq.empty[String])
        }

        val storagePath = row.getAs[String]("storage_path")
        val files = new MultiCloudFSReader().read(URI.create(storagePath))
        val fullSize: Option[Long] = if (files.isEmpty) None else Option(files.map(_.size).sum)

        val data: Array[Any] = Array(
          row.getAs[String]("table_catalog"),
          row.getAs[String]("table_schema"),
          row.getAs[String]("table_name"),
          row.getAs[String]("table_type"),
          storagePath, //"storage_path"
          row.getAs[Timestamp]("created"),
          row.getAs[String]("created_by"),
          lcCols, //"liquid_clustering_cols",
          row.getAs[String]("last_altered_by"),
          row.getAs[String]("last_altered"),
          size.map(_ / Math.pow(10, 9)), //"size_in_gb"
          size, //"size_in_bytes"
          fullSize.map(_ / Math.pow(10, 9)), //"full_size_in_gb"
          fullSize, //"full_size_in_bytes"
        )
        new GenericRowWithSchema(data, schema)
      })
      .iterator()
      .asScala
      .asInstanceOf[Iterator[Row]]

    if (hasError) {
      spark.createDataFrame(result.toList.asJava, schema)
        .where(plainFilters)
        .collect()
        .toSeq
    } else {
      result.toSeq
    }
  }
}
