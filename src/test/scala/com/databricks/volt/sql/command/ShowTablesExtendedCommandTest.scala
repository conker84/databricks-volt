package com.databricks.volt.sql.command

import com.databricks.volt.sql.utils.SQLUtils
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.util.Collections

class ShowTablesExtendedCommandTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  private var spark: SparkSession = _


  override def beforeEach(): Unit =
    spark = mock(classOf[SparkSession])

  test("run command with mock SparkSession using GenericRowWithSchema") {
    // Construct mock data using GenericRowWithSchema
    val mockData: java.util.List[Row] = Collections.singletonList(
      SQLUtils.createRowWithSchema(
        ShowTablesExtendedCommand.schema,
        "catalog1", // table_catalog
        "schema1", // table_schema
        "table1", // table_name
        "MANAGED", // table_type
        "CSV", // data_source_format
        "/path/to/storage1", // storage_path
        Timestamp.valueOf("2024-01-01 12:00:00"), // created
        "user1", // created_by
        "user2", // last_altered_by
        Timestamp.valueOf("2024-01-02 12:00:00"), // last_altered
        Array("col1"), // liquid_clustering_cols
        Map.empty[String, String], // properties,
        SQLUtils.createRowWithSchema(
          ShowTablesExtendedCommand.sizeSchema,
          null, null, null, null, null, null
        )
      )
    )

    // Mock Dataset[Row] and its collectAsList behavior
    val mockDataset = mock(classOf[Dataset[Row]])
    when(mockDataset.schema).thenReturn(ShowTablesExtendedCommand.schema)
    when(mockDataset.select(any[Column])).thenReturn(mockDataset)
    when(mockDataset.collectAsList()).thenReturn(mockData)

    // Mock SQL query
    when(spark.sql(any[String])).thenReturn(mockDataset)

    // Instantiate the command
    val command = ShowTablesExtendedCommand(null)

    // Run the command
    val result = command.run(spark)

    // Validate the result
    result should not be empty
    val first = result(0)
    first.getAs[String]("table_catalog") shouldBe "catalog1"
    first.getAs[String]("table_schema") shouldBe "schema1"
    first.getAs[String]("table_name") shouldBe "table1"
    first.getAs[Map[String, String]]("liquid_clustering_cols") shouldBe Seq.empty
    first.getAs[Map[String, String]]("properties") shouldBe Map.empty
    val row: Row = first.getAs[Row]("size")
    row.getAs[java.lang.Double]("delta_log_size_in_gb") shouldBe null
    row.getAs[java.lang.Long]("delta_log_size_in_bytes") shouldBe null
    row.getAs[java.lang.Double]("full_size_in_gb") shouldBe null
    row.getAs[java.lang.Long]("full_size_in_bytes") shouldBe null
    row.getAs[java.lang.Double]("last_snapshot_size_in_gb") shouldBe null
    row.getAs[java.lang.Long]("last_snapshot_size_in_bytes") shouldBe null

    // Verify interactions
    verify(spark).sql(any[String])
    verify(mockDataset).collectAsList()
  }

}
