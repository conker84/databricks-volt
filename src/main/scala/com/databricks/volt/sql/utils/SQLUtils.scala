package com.databricks.volt.sql.utils

import com.databricks.volt.sql.command.metadata.{CatalogIdentifier, Entity, SchemaIdentifier, TableIdentifier}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Try

/**
 * Utilities for performing SQL metadata operations such as cloning tags, comments, and constraints.
 */
object SQLUtils extends Logging {

  // Constants representing the system information schema tables from which to read tags.
  private val catalogTagsTable = "system.information_schema.catalog_tags"
  private val schemaTagsTable = "system.information_schema.schema_tags"
  private val tableTagsTable = "system.information_schema.table_tags"
  private val columnTagsQuery = """
      |SELECT catalog_name, schema_name, table_name, column_name,
      |  concat_ws(",",
      |    collect_set(format_string("'%s' = '%s'",
      |      replace(tag_name, "'", "\\'"), replace(tag_value, "'", "\\'"))
      |    )
      |  ) AS tags
      |FROM system.information_schema.column_tags
      |GROUP BY ALL
      |""".stripMargin

  // Constants representing the system information schema tables from which to read comments.
  private val catalogCommentsTable: String = "system.information_schema.tables"
  private val schemaCommentsTable: String = "system.information_schema.schemata"
  private val tableCommentsTable: String = "system.information_schema.catalogs"

  /**
   * A base SQL query to retrieve primary key and foreign key constraints from the information schema.
   */
  private val baseSqlPksAndFks: String =
    """
      |SELECT
      |  tc.constraint_name,
      |  tc.table_catalog, tc.table_schema, tc.table_name, collect_list(kcu.column_name) AS column_names,
      |  tc.constraint_type
      |FROM system.information_schema.table_constraints AS tc
      |JOIN system.information_schema.key_column_usage AS kcu
      |  USING (constraint_catalog, constraint_schema, constraint_name)
      |WHERE tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')
      |GROUP BY ALL
      |""".stripMargin

  val filterDeltaTables: String = """
      |SELECT *
      |FROM system.information_schema.tables
      |WHERE table_type NOT IN ('VIEW', 'FOREIGN')
      |AND table_schema <> 'information_schema'
      |AND table_catalog NOT IN ('system', '__databricks_internal')
      |AND data_source_format NOT IN ('UNKNOWN_DATA_SOURCE_FORMAT', 'DELTASHARING')
      |""".stripMargin

  /**
   * Constructs the "CREATE [OR REPLACE] [IF NOT EXISTS]" clause for a given object type (catalog, schema, table).
   *
   * @param objectTypeName Name of the object type (e.g., "CATALOG", "SCHEMA", "TABLE").
   * @param create         Whether to include "CREATE" in the statement.
   * @param replace        Whether to include "OR REPLACE".
   * @param ifNotExists    Whether to include "IF NOT EXISTS".
   * @return A String representing the correct combination of keywords.
   */
  def createOrReplaceObject(
                       objectTypeName: String,
                       create: Boolean,
                       replace: Boolean,
                       ifNotExists: Boolean
                     ): String = {
    // We use a mutable list to build up the clause.
    val createCatalog = new mutable.MutableList[String]

    // Add "CREATE" if requested.
    if (create) {
      createCatalog += "CREATE"
    }

    // Add "OR REPLACE" if requested, ensuring the final phrase is "CREATE OR REPLACE" when both flags are true.
    if (replace) {
      if (create) {
        createCatalog += "OR"
      }
      createCatalog += "REPLACE"
    }

    // Append the object type name (e.g., TABLE, SCHEMA, etc.).
    createCatalog += objectTypeName

    // Add "IF NOT EXISTS" if requested.
    if (ifNotExists) {
      createCatalog += "IF NOT EXISTS"
    }

    // Build and return the final string.
    createCatalog.mkString(" ")
  }

  /**
   * Validates that the source and target entities are of the same type (e.g., both are tables, or both are schemas).
   * Throws an IllegalArgumentException if they differ.
   *
   * @param sourceEntity The entity we are cloning from.
   * @param targetEntity The entity we are cloning to.
   */
  private def validateSourceTargetEntities(sourceEntity: Entity, targetEntity: Entity): Unit = {
    val sourceType = ClassTag(sourceEntity.getClass)
    ClassTag(targetEntity.getClass) match {
      case sourceType => Unit
      case _ => throw new IllegalArgumentException(
        s"Source type is ${sourceEntity.getClass}, target type is ${targetEntity.getClass}"
      )
    }
  }

  /**
   * Clones all tags from the source entity to the target entity.
   *
   * @param spark        The SparkSession used to run SQL queries.
   * @param sourceEntity The entity from which tags will be cloned.
   * @param targetEntity The entity to which tags will be cloned.
   * @return An array of Rows resulting from the final "SET TAGS" operation. Empty if no tags are found.
   */
  def cloneTags(spark: SparkSession, sourceEntity: Entity, targetEntity: Entity): Array[Row] = {
    // Ensure source and target are the same type.
    validateSourceTargetEntities(sourceEntity, targetEntity)

    // Log debug info about the operation.
    logDebug(
      s"""
         |Cloning tags for:
         |- source: $sourceEntity
         |- target: $targetEntity
         |""".stripMargin
    )

    // Build a WHERE clause with only the non-null parts (table, schema, catalog).
    // Identify which system table to query based on the entity type.
    val (systemTable, whereList) = sourceEntity match {
      case TableIdentifier(table, schema, catalog) =>
        (tableTagsTable, Seq(
          s"table_name = '$table'",
          s"schema_name = ${schema.map(sch => s"'$sch'").getOrElse("current_schema()")}",
          s"catalog_name = ${catalog.map(cat => s"'$cat'").getOrElse("current_catalog()")}",
        ))
      case SchemaIdentifier(schema, catalog) =>
        (schemaTagsTable, Seq(
          s"schema_name = '$schema'",
          s"catalog_name = ${catalog.map(cat => s"'$cat'").getOrElse("current_catalog()")}",
        ))
      case CatalogIdentifier(catalog) =>
        (catalogTagsTable, Seq(s"catalog_name = '$catalog'"))
    }

    // Collect the tags from the system table.
    val tags = spark.table(systemTable)
      .where(whereList.mkString(" AND "))
      .select("tag_name", "tag_value")
      .collect()

    // If there are no tags in the source entity, return empty.
    if (tags.nonEmpty) {
      // Build the "SET TAGS" string, e.g. tag_name = 'some_value'
      val tagsAsString = tags
        .map(row => {
          val tagName = row.getAs[String]("tag_name").replaceAll("'", "\\'")
          val tagValue = row.getAs[String]("tag_value").replaceAll("'", "\\'")
          s"'$tagName' = '$tagValue'"
        })
        .mkString("(", ", ", ")")

      // Construct the proper ALTER statement depending on entity type.
      val objectType = targetEntity match {
        case TableIdentifier(_, _, _) => "TABLE"
        case SchemaIdentifier(_, _) => "SCHEMA"
        case CatalogIdentifier(_) => "CATALOG"
      }

      if (objectType == "TABLE") {
        spark.sql(columnTagsQuery)
          .where(whereList.mkString(" "))
          .select("column_name", "tags")
          .collect()
          .foreach(row => {
            val columnName = row.getAs[String]("column_name")
            val tags = row.getAs[String]("tags")
            spark.sql(
              s"""
                 |ALTER TABLE ${targetEntity.toEscapedString}
                 |ALTER COLUM `$columnName`
                 |SET TAGS ($tags)
                 |""".stripMargin)
              .collect()
          })
      }

      // Execute the SQL statement to set the tags on the target entity.
      spark.sql(
          s"""
             |ALTER $objectType ${targetEntity.toEscapedString}
             |SET TAGS $tagsAsString
             |""".stripMargin)
        .collect()
    } else {
      Array.empty[Row]
    }
  }

  /**
   * Clones comments from the source entity to the target entity.
   *
   * @param spark        The SparkSession used to run SQL queries.
   * @param sourceEntity The entity from which the comment will be cloned.
   * @param targetEntity The entity to which the comment will be cloned.
   * @return An array of Rows resulting from the final "COMMENT ON ..." operation. Empty if no comment is found.
   */
  def cloneComments(spark: SparkSession, sourceEntity: Entity, targetEntity: Entity): Array[Row] = {
    // Ensure source and target are the same type.
    validateSourceTargetEntities(sourceEntity, targetEntity)

    // Log debug info about the operation.
    logDebug(
      s"""
         |Cloning comments for:
         |- source: $sourceEntity
         |- target: $targetEntity
         |""".stripMargin
    )

    val (systemTable, whereList) = sourceEntity match {
      case TableIdentifier(table, schema, catalog) =>
        (tableCommentsTable, Seq(
          s"table_name = '$table'",
          s"schema_name = ${schema.map(sch => s"'$sch'").getOrElse("current_schema()")}",
          s"catalog_name = ${catalog.map(cat => s"'$cat'").getOrElse("current_catalog()")}",
        ))
      case SchemaIdentifier(schema, catalog) =>
        (schemaCommentsTable, Seq(
          s"schema_name = '$schema'",
          s"catalog_name = ${catalog.map(cat => s"'$cat'").getOrElse("current_catalog()")}",
        ))
      case CatalogIdentifier(catalog) =>
        (catalogCommentsTable, Seq(s"catalog_name = '$catalog'"))
    }

    // Fetch the first comment row. If there's more than one, only the first is considered.
    val commentRow = spark.table(systemTable)
      .where(whereList.mkString(" AND "))
      .select("comment")
      .first()

    // If there is a comment, build and execute the COMMENT ON ... statement.
    if (commentRow != null) {
      // Escape single quotes in the comment by replacing them with literal periods.
      val comment = commentRow.getAs[String]("comment").replaceAll("'", "\\'")

      val setCommentQuery = targetEntity match {
        case TableIdentifier(_, _, _) =>
          s"COMMENT ON TABLE ${targetEntity.toEscapedString} IS '$comment'"
        case SchemaIdentifier(_, _) =>
          s"COMMENT ON SCHEMA ${targetEntity.toEscapedString} IS '$comment'"
        case CatalogIdentifier(_) =>
          s"COMMENT ON CATALOG ${targetEntity.toEscapedString} IS '$comment'"
      }
      spark.sql(setCommentQuery).collect()
    } else {
      Array.empty[Row]
    }
  }

  /**
   * Clones primary key and foreign key constraints from a source table to a target table.
   * This operation only applies to table entities; an exception is thrown otherwise.
   *
   * @param spark        The SparkSession used to run SQL queries.
   * @param sourceEntity The table from which constraints will be cloned.
   * @param targetEntity The table to which constraints will be cloned.
   * @return An array of Rows containing the results of the ALTER statements. Empty if no constraints are found or applied.
   */
  def cloneConstraints(spark: SparkSession, sourceEntity: Entity, targetEntity: Entity): Array[Row] = {
    // Ensure source and target are the same type.
    validateSourceTargetEntities(sourceEntity, targetEntity)
    logDebug(
      s"""
         |Cloning constraints for:
         |- source: $sourceEntity
         |- target: $targetEntity
         |""".stripMargin
    )

    // Build a WHERE clause with only the non-null parts (table, schema, catalog).
    val whereList = sourceEntity match {
      case TableIdentifier(table, schema, catalog) =>
        Seq(
          s"table_name = '$table'",
          s"schema_name = ${schema.map(sch => s"'$sch'").getOrElse("current_schema()")}",
          s"catalog_name = ${catalog.map(cat => s"'$cat'").getOrElse("current_catalog()")}",
        )
      case _ => throw new IllegalArgumentException("We can clone constraints only for tables")
    }



    // Execute the base query to get PK/FK constraints, filtered by table/catalog/schema name.
    spark.sql(baseSqlPksAndFks)
      .where(whereList.mkString(" AND "))
      .collect()
      .flatMap(row => {
        val constrName = row.getAs[String]("constraint_name")
        val constrColsAsStr = row.getAs[Seq[String]]("column_names").mkString("(", ", ", ")")
        // Check the constraint type (PRIMARY KEY or FOREIGN KEY).
        val alterStatement = if (row.getAs[String]("constraint_type") == "PRIMARY KEY") {
          s"""
             |ALTER TABLE ${targetEntity.toEscapedString}
             |ADD CONSTRAINT $constrName
             |PRIMARY KEY $constrColsAsStr
             |""".stripMargin
        } else {
          s"""
             |ALTER TABLE ${targetEntity.toEscapedString}
             |ADD CONSTRAINT $constrName
             |FOREIGN KEY $constrColsAsStr
             |REFERENCES ${sourceEntity.toEscapedString}
             |""".stripMargin
        }

        // Run each ALTER statement to add constraints to the target table.
        spark.sql(alterStatement).collect()
      })
  }

  /**
   * Clones metadata (tags, comments, and constraints) from a source entity to a target entity.
   * - Tags and comments are cloned for catalogs, schemas, and tables.
   * - Constraints are only cloned for tables.
   *
   * @param spark        The SparkSession used to run SQL queries.
   * @param sourceEntity The entity from which metadata will be cloned.
   * @param targetEntity The entity to which metadata will be cloned.
   * @return A sequence of error messages for any operations that failed; empty if all succeeded.
   */
  def cloneMetadata(spark: SparkSession, sourceEntity: Entity, targetEntity: Entity): Seq[String] = {
    // Attempt to clone tags; capture any exceptions.
    val cloneTagsRes = Try(cloneTags(spark, sourceEntity, targetEntity))
      .failed.toOption.map(_.getMessage)

    // Attempt to clone comments; capture any exceptions.
    val cloneCommentsRes = Try(cloneComments(spark, sourceEntity, targetEntity))
      .failed.toOption.map(_.getMessage)

    // We only attempt to clone constraints if the entity is a table.
    val cloneConstraintsRes = if (sourceEntity.isInstanceOf[TableIdentifier]) {
      Try(cloneConstraints(spark, sourceEntity, targetEntity))
        .failed.toOption.map(_.getMessage)
    } else {
      None
    }

    // Combine error messages for any failures and return them as a sequence.
    Seq(cloneTagsRes, cloneCommentsRes, cloneConstraintsRes)
      .filter(_.nonEmpty)
      .flatten
  }

  def createRowWithSchema(schema: StructType, data: Any*): Row = {
    if (data.isEmpty) throw new IllegalArgumentException("Data must be not empty")
    new GenericRowWithSchema(data.toArray, schema)
  }

}

