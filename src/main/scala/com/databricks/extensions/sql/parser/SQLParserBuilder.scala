package com.databricks.extensions.sql.parser

import com.databricks.extensions.sql.command.metadata.{CatalogIdentifier, Entity, SchemaIdentifier}
import com.databricks.extensions.sql.command.{CloneCatalogCommand, CloneSchemaCommand, ShowTablesExtendedCommand}
import com.databricks.extensions.sql.parser.SQLParserBaseParser.{CreateCatalogHeaderContext, CreateSchemaHeaderContext, QualifiedNameContext, ReplaceCatalogHeaderContext, ReplaceSchemaHeaderContext, SingleStatementContext, StringLitContext}
import com.databricks.extensions.sql.utils.AntlrUtils
import org.antlr.v4.runtime.Token
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.jdk.CollectionConverters.asScalaBufferConverter
import scala.util.{Failure, Success, Try}

case class CloneHeaderMetadata(entity: Entity, create: Boolean, replace: Boolean, ifNotExists: Boolean)

class SQLParserBuilder extends SQLParserBaseBaseVisitor[AnyRef] {

  import org.apache.spark.sql.catalyst.parser.ParserUtils._

  private def visitSchemaIdentifier(ctx: QualifiedNameContext): SchemaIdentifier = withOrigin(ctx) {
    ctx.identifier.asScala match {
      case Seq(db) => SchemaIdentifier(db.getText, None)
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
    val sourceCatalog = visitCatalogIdentifier(ctx.source)
    val location = Option(ctx.location).map(_.getText).getOrElse("")
    CloneCatalogCommand(cloneType, header.entity.asInstanceOf[CatalogIdentifier], sourceCatalog, location,
      create = header.create, replace = header.replace, ifNotExists = header.ifNotExists)
  }

  override def visitCloneSchema(ctx: SQLParserBaseParser.CloneSchemaContext): AnyRef = withOrigin(ctx) {
    val header = visitCloneSchemaHeader(ctx.cloneSchemaHeader()).asInstanceOf[CloneHeaderMetadata]
    val cloneType = if (ctx.DEEP() != null) "DEEP"
      else if (ctx.SHALLOW() != null) "SHALLOW"
      else throw new RuntimeException("Clone Type missing")
    val sourceSchema = visitSchemaIdentifier(ctx.source)
    val location = Option(ctx.location).map(_.getText).getOrElse("")
    CloneSchemaCommand(cloneType, header.entity.asInstanceOf[SchemaIdentifier], sourceSchema, location,
      create = header.create, replace = header.replace, ifNotExists = header.ifNotExists)
  }

  override def visitCloneCatalogHeader(ctx: SQLParserBaseParser.CloneCatalogHeaderContext): AnyRef = withOrigin(ctx) {
    val head = ctx.children.asScala.headOption match {
      case Some(value) => value
      case None => throw new RuntimeException("Catalog Header not found")
    }
    head match {
      case createHeader: CreateCatalogHeaderContext =>
        CloneHeaderMetadata(visitCatalogIdentifier(createHeader.catalog), true, false, createHeader.EXISTS() != null)
      case replaceHeader: ReplaceCatalogHeaderContext =>
        CloneHeaderMetadata(visitCatalogIdentifier(replaceHeader.catalog), replaceHeader.CREATE() != null, true, false)
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
        CloneHeaderMetadata(visitSchemaIdentifier(createHeader.schema), true, false, createHeader.EXISTS() != null)
      case replaceHeader: ReplaceSchemaHeaderContext =>
        CloneHeaderMetadata(visitSchemaIdentifier(replaceHeader.schema), replaceHeader.CREATE() != null, true, false)
      case _ => throw new RuntimeException("Schema Header not found")
    }
  }

  override def visitShowTablesExtended(ctx: SQLParserBaseParser.ShowTablesExtendedContext): AnyRef = withOrigin(ctx) {
    val stringFilter = Option(ctx.filters).map(AntlrUtils.extractRawText(_))
    ShowTablesExtendedCommand(stringFilter)
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

}
