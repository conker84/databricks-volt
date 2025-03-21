package com.databricks.volt.sql.parser

import com.databricks.volt.sql.command.metadata.{CatalogIdentifier, Entity, SchemaIdentifier, TableIdentifier}
import com.databricks.volt.sql.command.{CloneCatalogCommand, CloneSchemaCommand, CloneTableFullCommand, ShowTablesExtendedCommand}
import com.databricks.volt.sql.parser.SQLParserBaseParser.{CreateCatalogHeaderContext, CreateSchemaHeaderContext, QualifiedNameContext, SingleStatementContext, StringLitContext}
import com.databricks.volt.sql.utils.AntlrUtils
import org.antlr.v4.runtime.Token
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions

import scala.jdk.CollectionConverters.asScalaBufferConverter

case class CloneHeaderMetadata(entity: Entity, create: Boolean, replace: Boolean, ifNotExists: Boolean)

class SQLParserBuilder extends SQLParserBaseBaseVisitor[AnyRef] {

  import org.apache.spark.sql.catalyst.parser.ParserUtils._

  private def visitSchemaIdentifier(ctx: QualifiedNameContext): SchemaIdentifier = withOrigin(ctx) {
    ctx.identifier.asScala match {
      case Seq(db) => SchemaIdentifier(db.getText)
      case Seq(cat, db) => SchemaIdentifier(db.getText, Some(cat.getText))
      case _ => throw new RuntimeException("Illegal Schema Name")
    }
  }

  private def visitCatalogIdentifier(ctx: QualifiedNameContext): CatalogIdentifier = withOrigin(ctx) {
    ctx.identifier.asScala match {
      case Seq(cat) => CatalogIdentifier(cat.getText)
      case _ => throw new RuntimeException("Illegal Schema Name")
    }
  }

  override def visitCloneCatalog(ctx: SQLParserBaseParser.CloneCatalogContext): AnyRef = withOrigin(ctx) {
    val header = visitCloneCatalogHeader(ctx.cloneCatalogHeader()).asInstanceOf[CloneHeaderMetadata]
    val cloneType = if (ctx.DEEP() != null) "DEEP"
      else if (ctx.SHALLOW() != null) "SHALLOW"
      else throw new RuntimeException("Clone Type missing")
    val isFull = ctx.FULL() != null
    val sourceCatalog = visitCatalogIdentifier(ctx.source)
    val location = Option(ctx.location).map(_.getText).getOrElse("")
    CloneCatalogCommand(cloneType, header.entity.asInstanceOf[CatalogIdentifier], sourceCatalog, location,
      ifNotExists = header.ifNotExists, isFull = isFull)
  }

  override def visitCloneSchema(ctx: SQLParserBaseParser.CloneSchemaContext): AnyRef = withOrigin(ctx) {
    val header = visitCloneSchemaHeader(ctx.cloneSchemaHeader()).asInstanceOf[CloneHeaderMetadata]
    val cloneType = if (ctx.DEEP() != null) "DEEP"
      else if (ctx.SHALLOW() != null) "SHALLOW"
      else throw new RuntimeException("Clone Type missing")
    val isFull = ctx.FULL() != null
    val sourceSchema = visitSchemaIdentifier(ctx.source)
    val location = Option(ctx.location).map(_.getText).getOrElse("")
    CloneSchemaCommand(cloneType, header.entity.asInstanceOf[SchemaIdentifier], sourceSchema, location,
      ifNotExists = header.ifNotExists, isFull = isFull)
  }

  override def visitCloneCatalogHeader(ctx: SQLParserBaseParser.CloneCatalogHeaderContext): AnyRef = withOrigin(ctx) {
    val head = ctx.children.asScala.headOption match {
      case Some(value) => value
      case None => throw new RuntimeException("Catalog Header not found")
    }
    head match {
      case createHeader: CreateCatalogHeaderContext =>
        CloneHeaderMetadata(visitCatalogIdentifier(createHeader.catalog), true, false, createHeader.EXISTS() != null)
      case _ => throw new RuntimeException("Catalog Header not found")
    }
  }

  override def visitCloneSchemaHeader(ctx: SQLParserBaseParser.CloneSchemaHeaderContext): AnyRef = withOrigin(ctx) {
    val head = ctx.children.asScala.headOption match {
      case Some(value) => value
      case None => throw new RuntimeException("Catalog Header not found")
    }
    head match {
      case createHeader: CreateSchemaHeaderContext =>
        CloneHeaderMetadata(visitSchemaIdentifier(createHeader.schema), create = true, replace = false, ifNotExists = createHeader.EXISTS() != null)
      case _ => throw new RuntimeException("Schema Header not found")
    }
  }

  override def visitShowTablesExtended(ctx: SQLParserBaseParser.ShowTablesExtendedContext): AnyRef = withOrigin(ctx) {
    val filter = Option(ctx.filters)
      .map(AntlrUtils.extractRawText(_))
      .filterNot(_.isBlank)
      .map(functions.expr)
      .orNull
    ShowTablesExtendedCommand(filter)
  }

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }

  override def visitStringLit(ctx: StringLitContext): Token = {
    if (ctx != null) {
      if (ctx.STRING != null) {
        ctx.STRING.getSymbol
      } else {
        ctx.DOUBLEQUOTED_STRING.getSymbol
      }
    } else {
      null
    }
  }

  override def visitCloneTableFull(ctx: SQLParserBaseParser.CloneTableFullContext): AnyRef = {
    val header = visitCloneTableHeader(ctx.cloneTableHeader()).asInstanceOf[CloneHeaderMetadata]
    val sourceTable = visitTableIdentifier(ctx.source)
    val location = Option(ctx.location).map(_.getText).getOrElse("")
    val props = ctx.tableProps.property()
      .asScala
      .map(p => (p.key.getText, p.value.getText))
    val cloneType = if (ctx.DEEP() != null) "DEEP"
      else if (ctx.SHALLOW() != null) "SHALLOW"
      else throw new RuntimeException("Clone Type missing")
    CloneTableFullCommand(cloneType, header.entity.asInstanceOf[TableIdentifier], sourceTable, props,
      location, header.create, header.replace, header.ifNotExists)
  }

  private def visitTableIdentifier(ctx: QualifiedNameContext): TableIdentifier = withOrigin(ctx) {
    ctx.identifier.asScala match {
      case Seq(table) => TableIdentifier(table.getText)
      case Seq(sch, table) => TableIdentifier(table.getText, Some(sch.getText))
      case Seq(cat, sch, table) => TableIdentifier(table.getText, Some(sch.getText), Some(cat.getText))
      case _ => throw new RuntimeException("Illegal Table Name")
    }
  }

  override def visitCloneTableHeader(ctx: SQLParserBaseParser.CloneTableHeaderContext): AnyRef = {
    val head = ctx.children.asScala.headOption match {
      case Some(value) => value
      case None => throw new RuntimeException("Table Header not found")
    }
    head match {
      case createHeader: CreateCatalogHeaderContext =>
        CloneHeaderMetadata(visitCatalogIdentifier(createHeader.catalog),
          create = true, replace = false, ifNotExists = createHeader.EXISTS() != null)
      case _ => throw new RuntimeException("Table Header not found")
    }
  }

}
