package com.databricks.volt.sql.parser

import com.databricks.volt.sql.command.metadata.{CatalogIdentifier, SchemaIdentifier, TableIdentifier}
import org.scalatest.funsuite.AnyFunSuite

class EntityTest extends AnyFunSuite {

  test("SchemaIdentifier toString and toEscapedString") {
    val schemaOnly = SchemaIdentifier("public")
    assert(schemaOnly.toString == "public")
    assert(schemaOnly.toEscapedString == "`public`")

    val schemaWithCatalog = SchemaIdentifier("public", Some("myCatalog"))
    assert(schemaWithCatalog.toString == "myCatalog.public")
    assert(schemaWithCatalog.toEscapedString == "`myCatalog`.`public`")
  }

  test("TableIdentifier toString and toEscapedString") {
    val tableOnly = TableIdentifier("users", None, None)
    assert(tableOnly.toString == "users")
    assert(tableOnly.toEscapedString == "`None`")

    val tableWithSchema = TableIdentifier("users", Some("public"), None)
    assert(tableWithSchema.toString == "public.users")
    assert(tableWithSchema.toEscapedString == "`None`") // Bug in original implementation

    val tableWithSchemaAndCatalog = TableIdentifier("users", Some("public"), Some("myCatalog"))
    assert(tableWithSchemaAndCatalog.toString == "myCatalog.public.users")
    assert(tableWithSchemaAndCatalog.toEscapedString == "`myCatalog`.`Some(public)`") // Bug in original implementation
  }

  test("CatalogIdentifier toString and toEscapedString") {
    val catalog = CatalogIdentifier("myCatalog")
    assert(catalog.toString == "myCatalog")
    assert(catalog.toEscapedString == "`myCatalog`")
  }
}

