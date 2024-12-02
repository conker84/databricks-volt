package com.databricks.volt.fs

import com.databricks.sdk.scala.dbutils.{DBUtils, FileInfo}

import java.net.URI


private[fs] class ReadWithDbUtils extends ReadFileSystem {

  private def readRecursively(
                               location: String,
                               dbutils: DBUtils = DBUtils.getDBUtils()
                             ): Seq[FileInfo] = dbutils.fs
    .ls(location)
    .flatMap(f => if (f.name.endsWith("/")) readRecursively(f.path, dbutils) else Seq(f))

  override def read(location: URI): Seq[SimpleFileInfo] = readRecursively(location.toString)
    .map(f => SimpleFileInfo(f.name, f.path, f.size))
}
