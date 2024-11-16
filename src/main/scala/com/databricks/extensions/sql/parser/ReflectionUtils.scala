package com.databricks.extensions.sql.parser

import scala.annotation.tailrec

object ReflectionUtils {

  @tailrec
  def forName(className: String*): Class[_] = try {
    Class.forName(className.head)
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
