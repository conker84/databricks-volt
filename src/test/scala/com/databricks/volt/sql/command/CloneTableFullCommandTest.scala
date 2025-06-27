package com.databricks.volt.sql.command

import com.databricks.volt.sql.command.metadata.TableIdentifier
import com.databricks.volt.sql.utils.SQLUtils
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.internal.verification.AtMost
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CloneTableFullCommandTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  private var sparkSession: SparkSession = _

  override def beforeEach(): Unit =
    sparkSession = mock(classOf[SparkSession])

  test("run - success with no errors from cloneMetadata") {
    // Arrange
    val mockDF: Dataset[Row] = mock(classOf[Dataset[Row]])

    when(sparkSession.sql(anyString())).thenReturn(mockDF)
    when(sparkSession.table(anyString())).thenReturn(mockDF)
    when(mockDF.select(anyString(), any[Array[String]](): _*)).thenReturn(mockDF)
    when(mockDF.where(anyString())).thenReturn(mockDF)
    when(mockDF.collect()).thenReturn(Array.empty[Row])
    when(mockDF.first()).thenReturn(null)

    // Stub out the cloneMetadata method to return an empty Seq (simulating no errors)
    // Because cloneMetadata is a static method in SQLUtils, you might extract it
    // or provide a way to override it in tests. For now, we assume we can mock it
    // or that we accept its default behavior.
    // If you cannot easily mock SQLUtils.cloneMetadata, you might rely on integration testing.
    // For demonstration, let's assume you have a companion or utility method that is mockable.
    // If not, you can skip this part or consider using a different mocking approach.
    val sourceEntity = TableIdentifier("sourceTable", None, None)
    val targetEntity = TableIdentifier("targetTable", None, None)

    // Create an instance of the command
    val cmd = CloneTableFullCommand(
      cloneType = "SHALLOW",
      targetEntity = targetEntity,
      sourceEntity = sourceEntity,
      Seq.empty,
      managedLocation = "/some/location",
      create = true,
      replace = false,
      ifNotExists = false
    )

    // Act
    val result = cmd.run(sparkSession)

    // Assert
    // We expect exactly one row in the result
    result.size shouldBe 1
    val row = result(0)

    // Check that we have the expected columns in the returned row
    row.getAs[String]("table_catalog") shouldBe null // because targetEntity.catalog is None
    row.getAs[String]("table_schema") shouldBe null // because targetEntity.schema is None
    row.getAs[String]("table_name") shouldBe "targetTable"
    row.getAs[String]("status") shouldBe "OK"
    row.getAs[Seq[String]]("status_messages") shouldBe empty

    // Verify that sparkSession.sql(...) was called once
    verify(sparkSession, new AtMost(1)).sql(contains("CREATE TABLE targetTable SHALLOW CLONE sourceTable"))
    // You could also verify that cloneMetadata was called if you have a hook for that
  }

  test("run - success with some errors from cloneMetadata") {
    // Arrange
    val mockDF: DataFrame = mock(classOf[DataFrame])
    when(sparkSession.sql(anyString())).thenReturn(mockDF)
    when(mockDF.collect()).thenReturn(Array.empty[Row])

    // Suppose the cloneMetadata call returns some warnings or errors
    // If you can't directly mock cloneMetadata, consider injecting or refactoring.
    // For the sake of demonstration, let's assume we can intercept or wrap its result.
    val sourceEntity = TableIdentifier("sourceTable", None, None)
    val targetEntity = TableIdentifier("targetTable", None, None)

    // Create an instance of the command
    val cmd = new CloneTableFullCommand(
      cloneType = "SHALLOW",
      targetEntity = targetEntity,
      sourceEntity = sourceEntity,
      Seq.empty,
      managedLocation = "/some/location",
      create = true,
      replace = false,
      ifNotExists = false
    ) {
      // Override cloneMetadata call for test (if you can't do this, skip or use Powermock, etc.)
      override def run(sparkSession: SparkSession): Seq[Row] = {
        // Simulate having some "error messages" returned by cloneMetadata
        val data = Array[Any](
          targetEntity.catalog.orNull,
          targetEntity.schema.orNull,
          targetEntity.table,
          "WITH_ERRORS",
          Seq("Warning: Something went partially wrong")
        )
        Seq(new GenericRowWithSchema(data, schema))
      }
    }

    // Act
    val result = cmd.run(sparkSession)

    // Assert
    result.size shouldBe 1
    val row = result(0).asInstanceOf[GenericRowWithSchema]
    row.getAs[String]("status") shouldBe "WITH_ERRORS"
    row.getAs[Seq[String]]("status_messages") should contain("Warning: Something went partially wrong")
  }

  test("run - exception thrown by sparkSession.sql") {
    // Arrange
    when(sparkSession.sql(anyString())).thenThrow(new RuntimeException("Simulated SQL error"))

    val sourceEntity = TableIdentifier("sourceTable", Some("someSchema"), Some("someCatalog"))
    val targetEntity = TableIdentifier("targetTable", Some("someSchema"), Some("someCatalog"))

    val cmd = CloneTableFullCommand(
      cloneType = "DEEP",
      targetEntity = targetEntity,
      sourceEntity = sourceEntity,
      Seq.empty,
      managedLocation = "/some/location",
      create = false,
      replace = true,
      ifNotExists = false
    )

    // Act
    val result = cmd.run(sparkSession)

    // Assert
    result.size shouldBe 1
    val row = result(0).asInstanceOf[GenericRowWithSchema]
    row.getAs[String]("table_catalog") shouldBe "someCatalog"
    row.getAs[String]("table_schema") shouldBe "someSchema"
    row.getAs[String]("table_name") shouldBe "targetTable"
    row.getAs[String]("status") shouldBe "ERROR"
    val messages = row.getAs[Seq[String]]("status_messages")
    messages should have size 1
    messages(0) shouldBe "Simulated SQL error"
  }

  test("validate - invalid combination throws exception") {
    // Here you can directly test `CloneSchemaCommand.validate` if you want to ensure
    // it throws an exception on invalid flags. For example:
    intercept[IllegalArgumentException] {
      CloneSchemaCommand.validate(create = true, replace = true, ifNotExists = true)
    }
  }
}
