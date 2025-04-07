package com.databricks.volt.sql.command.metadata

trait Entity {
  def toEscapedString: String
}

case class TableIdentifier(table: String, schema: Option[String] = None, catalog: Option[String] = None) extends Entity {
  override def toString: String = schema
    .map(sch => s"$sch.$table")
    .flatMap(schTbl => catalog.map(cat => s"$cat.$schTbl"))
    .getOrElse(table)

  override def toEscapedString: String = schema
    .map(sch => s"`$sch`.`$table`")
    .flatMap(schTbl => catalog.map(cat => s"`$cat`.$schTbl"))
    .getOrElse(s"`$table`")
}

case class SchemaIdentifier(schema: String, catalog: Option[String] = None) extends Entity {
  override def toString: String = catalog.map(cat => s"$cat.$schema").getOrElse(schema)

  override def toEscapedString: String = catalog.map(cat => s"`$cat`.`$schema`").getOrElse(s"`$schema`")
}

case class CatalogIdentifier(catalog: String) extends Entity {
  override def toString: String = catalog

  override def toEscapedString: String = s"`$catalog`"
}

