package com.databricks.volt.sql.command

import com.databricks.volt.sql.command.metadata.TableIdentifier
import com.databricks.volt.sql.utils.SQLUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.mockito.ArgumentMatchers.{any, anyString, argThat}
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

import scala.jdk.CollectionConverters.seqAsJavaListConverter

object ShowTableConstraintsCommandTest {
  private val schema = new StructType()
    .add("col_name", StringType)
    .add("data_type", StringType)
    .add("type", StringType)

  private val tableIdentifier = TableIdentifier("test_table", Some("test_db"))
}

class ShowTableConstraintsCommandTest extends AnyFunSuite with MockitoSugar {

  test("should extract PRIMARY KEY correctly") {
    val mockDF = mock[DataFrame]
    val emptyDF = mock[DataFrame]
    when(mockDF.filter(anyString())).thenReturn(mockDF)
    when(mockDF.selectExpr(any[Array[String]]() : _*)).thenReturn(mockDF)
    when(mockDF.collectAsList()).thenReturn(Seq(
      SQLUtils.createRowWithSchema(ShowTableConstraintsCommandTest.schema, "foo_pk", "PRIMARY KEY (`pk`)", "PRIMARY KEY"),
    ).asJava)
    when(emptyDF.select(anyString(), any[Array[String]]() : _*)).thenReturn(emptyDF)
    when(emptyDF.collect()).thenReturn(Array(
      SQLUtils.createRowWithSchema(new StructType().add("properties", MapType.apply(StringType, StringType)), Map.empty[String, String])
    ))
    val spark = mock[SparkSession]
    when(spark.sql(argThat((sql: String) => sql != null && sql.startsWith("DESCRIBE TABLE EXTENDED"))))
      .thenReturn(mockDF)
    when(spark.sql(argThat((sql: String) => sql != null && sql.startsWith("DESCRIBE DETAIL"))))
      .thenReturn(emptyDF)

    val command = new ShowTableConstraintsCommand(ShowTableConstraintsCommandTest.tableIdentifier)
    val result = command.run(spark)

    assert(result.size == 1)
    assert(result(0).getAs[String]("type") == "PRIMARY KEY")
    assert(result(0).getAs[String]("name") == "foo_pk")
    assert(result(0).getAs[String]("meta") == "{\"columns\":[\"pk\"],\"rely\":false}")
  }

  test("should extract FOREIGN KEY correctly") {
    val mockDF = mock[DataFrame]
    val emptyDF = mock[DataFrame]
    when(mockDF.filter(anyString())).thenReturn(mockDF)
    when(mockDF.selectExpr(any[Array[String]]() : _*)).thenReturn(mockDF)
    when(mockDF.collectAsList()).thenReturn(Seq(
      SQLUtils.createRowWithSchema(ShowTableConstraintsCommandTest.schema, "foo_fk", "FOREIGN KEY (`fk1`, `fk2`) REFERENCES `as_catalog`.`default`.`tbl` (`pk1`, `pk2`)", "FOREIGN KEY")
    ).asJava)
    when(emptyDF.select(anyString(), any[Array[String]]() : _*)).thenReturn(emptyDF)
    when(emptyDF.collect()).thenReturn(Array(
      SQLUtils.createRowWithSchema(new StructType().add("properties", MapType.apply(StringType, StringType)), Map.empty[String, String])
    ))
    val spark = mock[SparkSession]
    when(spark.sql(argThat((sql: String) => sql != null && sql.startsWith("DESCRIBE TABLE EXTENDED"))))
      .thenReturn(mockDF)
    when(spark.sql(argThat((sql: String) => sql != null && sql.startsWith("DESCRIBE DETAIL"))))
      .thenReturn(emptyDF)

    val command = new ShowTableConstraintsCommand(ShowTableConstraintsCommandTest.tableIdentifier)
    val result = command.run(spark)

    assert(result.size == 1)
    assert(result.head.getAs[String]("type") == "FOREIGN KEY")
    val meta = ShowTableConstraintsCommand.mapper.readValue(result(0).getAs[String]("meta"), classOf[Map[String, Any]])
    assert(meta("columns").asInstanceOf[List[String]] == List("fk1", "fk2"))
    assert(meta("refColumns").asInstanceOf[List[String]] == List("pk1", "pk2"))
    assert(meta("refTable") == "tbl")
  }

  test("should fail on malformed FK") {
    val mockDF = mock[DataFrame]
    val emptyDF = mock[DataFrame]
    when(mockDF.filter(anyString())).thenReturn(mockDF)
    when(mockDF.selectExpr(any[Array[String]]() : _*)).thenReturn(mockDF)
    when(mockDF.collectAsList()).thenReturn(Seq(
      SQLUtils.createRowWithSchema(ShowTableConstraintsCommandTest.schema, "foo_fk", "FOREIGN KEY (`fk1`) REFERENCES `as_catalog`.`default`.`tbl` (`pk1`, `pk2`)", "FOREIGN KEY")
    ).asJava)
    when(emptyDF.select(anyString(), any[Array[String]]() : _*)).thenReturn(emptyDF)
    when(emptyDF.collect()).thenReturn(Array(
      SQLUtils.createRowWithSchema(new StructType().add("properties", MapType.apply(StringType, StringType)), Map.empty[String, String])
    ))
    val spark = mock[SparkSession]
    when(spark.sql(argThat((sql: String) => sql != null && sql.startsWith("DESCRIBE TABLE EXTENDED"))))
      .thenReturn(mockDF)
    when(spark.sql(argThat((sql: String) => sql != null && sql.startsWith("DESCRIBE DETAIL"))))
      .thenReturn(emptyDF)

    val command = new ShowTableConstraintsCommand(ShowTableConstraintsCommandTest.tableIdentifier)
    assertThrows[IllegalArgumentException] {
      command.run(spark)
    }
  }

  test("should return empty on no constraints") {
    val mockDF = mock[DataFrame]
    val emptyDF = mock[DataFrame]
    when(mockDF.filter(anyString())).thenReturn(mockDF)
    when(mockDF.selectExpr(any[Array[String]]() : _*)).thenReturn(mockDF)
    when(mockDF.collectAsList()).thenReturn(Seq.empty[Row].asJava)
    when(emptyDF.select(anyString(), any[Array[String]]() : _*)).thenReturn(emptyDF)
    when(emptyDF.collect()).thenReturn(Array(
      SQLUtils.createRowWithSchema(new StructType().add("properties", MapType.apply(StringType, StringType)), Map.empty[String, String])
    ))
    val spark = mock[SparkSession]
    when(spark.sql(argThat((sql: String) => sql != null && sql.startsWith("DESCRIBE TABLE EXTENDED"))))
      .thenReturn(mockDF)
    when(spark.sql(argThat((sql: String) => sql != null && sql.startsWith("DESCRIBE DETAIL"))))
      .thenReturn(emptyDF)

    val command = new ShowTableConstraintsCommand(ShowTableConstraintsCommandTest.tableIdentifier)
    val result = command.run(spark)
    assert(result.isEmpty)
  }
}
