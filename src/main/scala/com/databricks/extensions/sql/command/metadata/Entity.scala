package com.databricks.extensions.sql.command.metadata

trait Entity {
  def toEscapedString: String
}

case class SchemaIdentifier(schema: String, catalog: Option[String] = None) extends Entity {
  override def toString: String = catalog.map(cat => s"$cat.$schema").getOrElse(schema)

  override def toEscapedString: String = catalog.map(cat => s"`$cat`.`$schema`").getOrElse(s"`$schema`")
}

case class CatalogIdentifier(catalog: String) extends Entity {
  override def toString: String = catalog

  override def toEscapedString: String = s"`$catalog`"
}

