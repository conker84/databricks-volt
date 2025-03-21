package com.databricks.volt.apis

import com.databricks.volt.sql.command.metadata.{CatalogIdentifier, SchemaIdentifier}
import com.databricks.volt.sql.command.{CloneCatalogCommand, CloneSchemaCommand, ShowTablesExtendedCommand}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}

object CatalogExtensions {
  implicit class CatalogImplicits(catalog: Catalog) {

    def deepCloneCatalog(
                          targetCatalog: String,
                          managedLocation: String = "",
                          ifNotExists: Boolean = false,
                          isFull: Boolean = false
                        ): DataFrame = {
      val spark = SparkSession.active
      val currentCatalogIdentifier = CatalogIdentifier(catalog.currentCatalog())
      val targetCatalogIdentifier = CatalogIdentifier(targetCatalog)
      CloneCatalogCommand("DEEP", targetCatalogIdentifier, currentCatalogIdentifier, managedLocation,
        ifNotExists, isFull)
        .runDF(spark)
    }

    def shallowCloneCatalog(
                             targetCatalog: String,
                             managedLocation: String = "",
                             ifNotExists: Boolean = false,
                             isFull: Boolean = false
                           ): DataFrame = {
      val spark = SparkSession.active
      val currentCatalogIdentifier = CatalogIdentifier(catalog.currentCatalog())
      val targetCatalogIdentifier = CatalogIdentifier(targetCatalog)
      CloneCatalogCommand("SHALLOW", targetCatalogIdentifier, currentCatalogIdentifier, managedLocation,
        ifNotExists, isFull)
        .runDF(spark)
    }

    def deepCloneSchema(
                         targetSchema: SchemaIdentifier,
                         managedLocation: String = "",
                         ifNotExists: Boolean = false,
                         isFull: Boolean = false
                       ): DataFrame = {
      val spark = SparkSession.active
      val currentSchema = SchemaIdentifier(catalog.currentDatabase, Option(catalog.currentCatalog()))
      CloneSchemaCommand("DEEP", targetSchema, currentSchema, managedLocation,
        ifNotExists, isFull)
        .runDF(spark)
    }

    def shallowCloneSchema(
                            targetSchema: SchemaIdentifier,
                            managedLocation: String = "",
                            ifNotExists: Boolean = false,
                            isFull: Boolean = false
                          ): DataFrame = {
      val spark = SparkSession.active
      val currentSchema = SchemaIdentifier(catalog.currentDatabase, Option(catalog.currentCatalog()))
      CloneSchemaCommand("SHALLOW", targetSchema, currentSchema, managedLocation,
        ifNotExists, isFull)
        .runDF(spark)
    }

    def showTablesExtended(filter: Column): DataFrame = {
      val spark = SparkSession.active
      ShowTablesExtendedCommand(filter).runDF(spark)
    }

    def showTablesExtended(filter: String): DataFrame = {
      val innerFilter = if (StringUtils.isBlank(filter))
        s"table_catalog = '${catalog.currentCatalog()}' AND table_schema = '${catalog.currentDatabase}'"
      else
        filter
      CatalogImplicits(catalog)
        .showTablesExtended(functions.expr(innerFilter))
    }
  }

  // Necessary Python bindings

  def deepCloneCatalog(
                        catalog: Catalog,
                        targetCatalog: String,
                        managedLocation: String = "",
                        ifNotExists: Boolean = false,
                        isFull: Boolean
                      ): DataFrame = CatalogImplicits(catalog)
    .deepCloneCatalog(targetCatalog, managedLocation, ifNotExists, isFull)

  def shallowCloneCatalog(
                           catalog: Catalog,
                           targetCatalog: String,
                           managedLocation: String = "",
                           ifNotExists: Boolean = false,
                           isFull: Boolean = false
                         ): DataFrame = CatalogImplicits(catalog)
    .shallowCloneCatalog(targetCatalog, managedLocation, ifNotExists, isFull)

  def deepCloneSchema(
                       catalog: Catalog,
                       targetSchema: SchemaIdentifier,
                       managedLocation: String = "",
                       ifNotExists: Boolean = false,
                       isFull: Boolean = false
                     ): DataFrame = CatalogImplicits(catalog)
    .deepCloneSchema(targetSchema, managedLocation, ifNotExists, isFull)

  def shallowCloneSchema(
                          catalog: Catalog,
                          targetSchema: SchemaIdentifier,
                          managedLocation: String = "",
                          ifNotExists: Boolean = false,
                          isFull: Boolean = false
                        ): DataFrame = CatalogImplicits(catalog)
    .shallowCloneSchema(targetSchema, managedLocation, ifNotExists, isFull)

  def showTablesExtended(
                          catalog: Catalog,
                          filter: String
                        ): DataFrame = CatalogImplicits(catalog)
    .showTablesExtended(filter)

  def showTablesExtended(
                          catalog: Catalog,
                          filter: Column
                        ): DataFrame = CatalogImplicits(catalog)
    .showTablesExtended(filter)
}

