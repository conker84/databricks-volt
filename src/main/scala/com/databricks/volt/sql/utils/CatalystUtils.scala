package com.databricks.volt.sql.utils

import org.apache.spark.sql.catalyst.expressions.{Attribute, BinaryExpression, Expression, UnaryExpression}

object CatalystUtils {

  private def unfoldExpressions(expr: Expression): Seq[Expression] = {
    val exprs = expr match {
      case BinaryExpression(tuple) => Seq(tuple._1, tuple._2)
      case UnaryExpression(expression) => Seq(expression)
      case _ => Seq.empty
    }
    if (exprs.isEmpty) Seq(expr) else exprs.flatMap(unfoldExpressions)
  }

  def extractAttributes(filters: Seq[Expression]): Set[String] = filters
    .flatMap(unfoldExpressions)
    .filter(_.isInstanceOf[Attribute])
    .map(_.asInstanceOf[Attribute])
    .map(_.name)
    .toSet

}
