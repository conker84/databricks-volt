package com.databricks.volt.sql.parser

import scala.annotation.tailrec

object ReflectionUtils {

  @tailrec
  def forName(className: String*): Class[_] = try {
    if (className.isEmpty) return null
    Class.forName(className(0))
  } catch {
    case cfe: ClassNotFoundException =>
      val list = className.drop(1)
      if (list.nonEmpty) {
        forName(list: _*)
      } else {
        null
      }
  }

}
