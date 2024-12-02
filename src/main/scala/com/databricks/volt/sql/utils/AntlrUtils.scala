package com.databricks.volt.sql.utils

import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.misc.Interval

object AntlrUtils {

  def extractRawText(exprContext: ParserRuleContext): String = {
    // Extract the raw expression which will be parsed later
    exprContext.getStart.getInputStream.getText(new Interval(
      exprContext.getStart.getStartIndex,
      exprContext.getStop.getStopIndex))
  }
}
