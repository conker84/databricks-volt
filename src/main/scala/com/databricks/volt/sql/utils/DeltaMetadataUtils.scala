package com.databricks.volt.sql.utils

import com.databricks.volt.sql.parser.ReflectionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier

import java.lang.reflect.Method
import scala.util.Try

object DeltaMetadataUtils {
  // TODO move this to method handles
  private val deltaLogClazz: Try[Class[_]] = ReflectionUtils.forName("com.databricks.sql.transaction.tahoe.DeltaLog")
  private val clusteringColumnInfoClazz: Try[Class[_]] = ReflectionUtils.forName("com.databricks.sql.io.skipping.liquid.ClusteringColumnInfo")
  private val snapshotClazz: Try[Class[_]] = ReflectionUtils.forName("com.databricks.sql.transaction.tahoe.Snapshot")
  private val forTableMethod: Try[Method] = deltaLogClazz
    .map(_.getMethod("forTable", classOf[SparkSession], classOf[TableIdentifier]))
  private val extractLogicalNamesMethod: Try[Method] = clusteringColumnInfoClazz
    .flatMap(ccIc => snapshotClazz.map(sc => ccIc.getMethod("extractLogicalNames", sc)))

  // TODO return a proper error if libraries are not present
  def forTable(
                spark: SparkSession,
                table: TableIdentifier
              ): (Option[Long], Seq[String], Map[String, String]) = forTableMethod
    .map(_.invoke(null, spark, table))
    .map(ret => ret.getClass
      .getMethod("snapshot")
      .invoke(ret))
    .map(snapshot => {
      val size = snapshot.getClass
        .getMethod("sizeInBytes")
        .invoke(snapshot)
        .asInstanceOf[Long]
      val lcCols = extractLogicalNamesMethod
        .map(_.invoke(clusteringColumnInfoClazz, snapshot)
          .asInstanceOf[Seq[String]])
        .getOrElse(Seq.empty[String])
      val props = snapshot.getClass
        .getMethod("getProperties")
        .invoke(snapshot)
        .asInstanceOf[scala.collection.mutable.Map[String, String]]
        .toMap
      (Option(size), lcCols, props)
    })
    .getOrElse((None, Seq.empty[String], Map.empty[String, String]))

}