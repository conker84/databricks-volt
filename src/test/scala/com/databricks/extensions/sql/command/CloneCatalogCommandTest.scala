package com.databricks.extensions.sql.command

import com.databricks.extensions.sql.command.metadata.{CatalogIdentifier, SchemaIdentifier}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructType}
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CloneCatalogCommandTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  private var sparkSession: SparkSession = _

  private val schema = CloneSchemaCommand.schema

  override def beforeEach(): Unit =
    sparkSession = mock(classOf[SparkSession])

  test("run command with valid source and target catalog") {
    // Mock for CloneCatalog
    val databaseNameSchema = new StructType().add("databaseName", StringType)
    // Mock source schema data
    val mockSourceSchemas: Array[Row] = Array(
      new GenericRowWithSchema(Array("schema1"), databaseNameSchema),
      new GenericRowWithSchema(Array("schema2"), databaseNameSchema)
    )

    val mockDataset = mock(classOf[Dataset[Row]])
    when(mockDataset.filter(anyString())).thenReturn(mockDataset)
    when(mockDataset.where(anyString())).thenReturn(mockDataset)
    when(mockDataset.collect()).thenReturn(mockSourceSchemas)
    when(sparkSession.sql(contains("SHOW SCHEMAS IN"))).thenReturn(mockDataset)

    // Mock the creation of the catalog
    val mockCreateCatalog = mock(classOf[Dataset[Row]])
    when(mockCreateCatalog.collect()).thenReturn(Array.empty[Row])
    when(sparkSession.sql(contains("CREATE CATALOG"))).thenReturn(mockCreateCatalog)

    // Mock CloneSchemaCommand behavior
    val mockCloneSchemaCommand = mock(classOf[CloneSchemaCommand])
    when(mockCloneSchemaCommand.run(any[org.apache.spark.sql.SparkSession]))
      .thenReturn(Seq.empty[Row])

    // Define the source and target catalogs
    val sourceEntity = CatalogIdentifier("source_catalog")
    val targetEntity = CatalogIdentifier("target_catalog")

    // Mock for CloneSchema
    // Mock "SHOW TABLES IN" output
    val rowSchema = new StructType().add("tableName", StringType)
    val mockShowTables = mock(classOf[Dataset[Row]])
    when(mockShowTables.filter(anyString())).thenReturn(mockShowTables)
    when(mockShowTables.where(anyString())).thenReturn(mockShowTables)
    val collectMockResult: Array[Row] =
      Array(new GenericRowWithSchema(Array("table1"), rowSchema), new GenericRowWithSchema(Array("table2"), rowSchema))
    when(mockShowTables.collect()).thenReturn(collectMockResult)
    when(sparkSession.sql(contains("SHOW TABLES IN `source_catalog`.`schema1`"))).thenReturn(mockShowTables)
    when(sparkSession.sql(contains("SHOW TABLES IN `source_catalog`.`schema2`"))).thenReturn(mockShowTables)

    // Mock schema creation
    val mockCreateSchema = mock(classOf[Dataset[Row]])
    when(sparkSession.sql(contains("CREATE SCHEMA `target_catalog`.`schema1`"))).thenReturn(mockCreateSchema)
    when(sparkSession.sql(contains("CREATE SCHEMA `target_catalog`.`schema2`"))).thenReturn(mockCreateSchema)

    // Mock table cloning
    val mockCloneTable = mock(classOf[Dataset[Row]])
    when(sparkSession.sql(contains("CREATE TABLE"))).thenReturn(mockCloneTable)

    // Instantiate and run the command
    val command = CloneCatalogCommand(
      cloneType = "SHALLOW",
      targetEntity = targetEntity,
      sourceEntity = sourceEntity,
      managedLocation = "",
      create = true,
      replace = false,
      ifNotExists = false
    )

    val result = command.run(sparkSession)

    // Assertions
    result should have size 4
    result(0).getAs[String]("status") shouldBe "OK"
    result(1).getAs[String]("status") shouldBe "OK"
    result(2).getAs[String]("status") shouldBe "OK"
    result(3).getAs[String]("status") shouldBe "OK"

    // Verify interactions
    verify(sparkSession).sql(contains("SHOW SCHEMAS IN `source_catalog`"))
    verify(sparkSession).sql(contains("CREATE CATALOG `target_catalog`"))
  }
}
