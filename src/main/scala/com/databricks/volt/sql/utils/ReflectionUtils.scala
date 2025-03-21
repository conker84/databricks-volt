package com.databricks.volt.sql.utils

import scala.util.Try

object ReflectionUtils {

  @SuppressWarnings(Array("UnsafeTraversableMethods"))
  def forName(className: String*): Try[Class[_]] = className
    .map(s => Try[Class[_]](Class.forName(s)))
    .reduce((a ,b) => a.orElse(b))

}
