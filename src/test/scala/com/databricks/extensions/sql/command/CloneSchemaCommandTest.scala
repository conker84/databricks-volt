package com.databricks.extensions.sql.command

import com.databricks.extensions.sql.command.metadata.SchemaIdentifier
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CloneSchemaCommandTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  private var spark: SparkSession = _
  private val schema = CloneSchemaCommand.schema

  override def beforeEach(): Unit = {
    spark = mock(classOf[SparkSession])
  }

  test("validate method should throw exceptions for invalid options") {
    // Test for invalid create/replace/ifNotExists combinations
    intercept[RuntimeException] {
      CloneSchemaCommand.validate(create = false, replace = false, ifNotExists = true)
    }.getMessage shouldBe "You can use IF NOT EXISTS only with CREATE"

    intercept[RuntimeException] {
      CloneSchemaCommand.validate(create = true, replace = true, ifNotExists = true)
    }.getMessage shouldBe "You cannot use IF NOT EXISTS with REPLACE"
  }

  test("run command with valid inputs") {
    val sourceEntity = SchemaIdentifier("source_schema", Some("source_catalog"))
    val targetEntity = SchemaIdentifier("target_schema", Some("target_catalog"))
    val cloneType = "DEEP"
    val managedLocation = "/path/to/managed"
    val command = CloneSchemaCommand(cloneType, targetEntity, sourceEntity, managedLocation, create = true, replace = false, ifNotExists = false)

    // Mock "SHOW TABLES IN" output
    val rowSchema = new StructType().add("tableName", StringType)
    val mockShowTables = mock(classOf[Dataset[Row]])
    when(mockShowTables.filter(anyString())).thenReturn(mockShowTables)
    when(mockShowTables.where(anyString())).thenReturn(mockShowTables)
    val collectMockResult: Array[Row] = Array(
      new GenericRowWithSchema(Array("table1"), rowSchema),
      new GenericRowWithSchema(Array("table2"), rowSchema)
    )
    when(mockShowTables.collect()).thenReturn(collectMockResult)
    when(spark.sql(contains("SHOW TABLES IN `source_catalog`.`source_schema`"))).thenReturn(mockShowTables)

    // Mock schema creation
    val mockCreateSchema = mock(classOf[Dataset[Row]])
    when(spark.sql(contains("CREATE SCHEMA `target_catalog`.`target_schema` MANAGED LOCATION '/path/to/managed'"))).thenReturn(mockCreateSchema)

    // Mock table cloning
    val mockCloneTable = mock(classOf[Dataset[Row]])
    when(spark.sql(contains("CREATE TABLE"))).thenReturn(mockCloneTable)

    // Execute the command
    val result = command.run(spark)

    // Verify the results
    result should have size 2
    result(0).getAs[String]("status") shouldBe "OK"
    result(1).getAs[String]("status") shouldBe "OK"

    // Verify interactions
    verify(spark).sql(contains("SHOW TABLES IN `source_catalog`.`source_schema`"))
    verify(spark).sql(contains("CREATE SCHEMA `target_catalog`.`target_schema`"))
    verify(spark, times(2)).sql(contains("CREATE TABLE `target_catalog`.`target_schema`"))
  }
}
