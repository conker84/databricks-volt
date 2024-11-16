package com.databricks.extensions.sql.utils

import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import scala.annotation.tailrec

object SchemaUtils {

  def assertSchemaEqual(
                         actual: StructType,
                         expected: StructType,
                         ignoreNullable: Boolean = true,
                         ignoreColumnOrder: Boolean = false,
                         ignoreColumnName: Boolean = false
                       ): Unit = {

    def compareSchemasIgnoreNullable(s1: StructType, s2: StructType): Boolean = {
      if (s1.size != s2.size) return false
      s1.zip(s2).forall { case (sf1, sf2) => compareStructFieldsIgnoreNullable(sf1, sf2) }
    }

    def compareStructFieldsIgnoreNullable(actualSF: StructField, expectedSF: StructField): Boolean = {
      if (actualSF == null && expectedSF == null) true
      else if (actualSF == null || expectedSF == null) false
      else {
        actualSF.name == expectedSF.name &&
          compareDataTypesIgnoreNullable(actualSF.dataType, expectedSF.dataType)
      }
    }

    @tailrec
    def compareDataTypesIgnoreNullable(dt1: DataType, dt2: DataType): Boolean = {
      if (dt1.typeName == dt2.typeName) {
        dt1 match {
          case ArrayType(elementType1, _) =>
            compareDataTypesIgnoreNullable(elementType1, dt2.asInstanceOf[ArrayType].elementType)
          case StructType(fields1) =>
            compareSchemasIgnoreNullable(StructType(fields1), dt2.asInstanceOf[StructType])
          case _ => true
        }
      } else {
        false
      }
    }

    val adjustedActual = {
      val reordered = if (ignoreColumnOrder) {
        StructType(actual.sortBy(_.name))
      } else {
        actual
      }
      if (ignoreColumnName) {
        StructType(reordered.zipWithIndex.map {
          case (field, index) =>
            StructField(index.toString, field.dataType, field.nullable)
        })
      } else {
        reordered
      }
    }

    val adjustedExpected = {
      val reordered = if (ignoreColumnOrder) {
        StructType(expected.sortBy(_.name))
      } else {
        expected
      }
      if (ignoreColumnName) {
        StructType(reordered.zipWithIndex.map {
          case (field, index) =>
            StructField(index.toString, field.dataType, field.nullable)
        })
      } else {
        reordered
      }
    }

    val schemasEqual =
      if (ignoreNullable) compareSchemasIgnoreNullable(adjustedActual, adjustedExpected)
      else adjustedActual == adjustedExpected

    if (!schemasEqual) {
      val diff = adjustedActual.toString.split("\n").zipAll(adjustedExpected.toString.split("\n"), "", "")
      val diffMessage = diff.map { case (a, e) => s"- $a\n+ $e" }.mkString("\n")
      throw new IllegalArgumentException(s"Schemas do not match:\n$diffMessage")
    }
  }
}
