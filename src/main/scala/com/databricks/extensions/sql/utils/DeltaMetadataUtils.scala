package com.databricks.extensions.sql.utils

import com.databricks.extensions.sql.parser.ReflectionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier

import scala.util.Try

object DeltaMetadataUtils {
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

  def forTable(
                spark: SparkSession,
                table: TableIdentifier
              ): (Option[Long], Seq[String]) = Try(forTableMethod
    .invoke(null, spark, table))
    .map(ret => ret.getClass
      .getMethod("snapshot")
      .invoke(ret))
    .map(snapshot => {
      val size = snapshot.getClass
        .getMethod("sizeInBytes")
        .invoke(snapshot)
        .asInstanceOf[Long]
      val lcCols = extractLogicalNamesMethod
        .invoke(clusteringColumnInfoClazz, snapshot)
        .asInstanceOf[Seq[String]]
      (Option(size), lcCols)
    })
    .getOrElse((None, Seq.empty[String]))

}