package com.databricks.extensions.fs

import org.apache.spark.internal.Logging

import java.net.URI
import scala.util.{Failure, Success, Try}

case class SimpleFileInfo(name: String, path: String, size: Long)

trait ReadFileSystem extends Logging {
  def read(location: URI): Seq[SimpleFileInfo]
}

object ReadFileSystem {
  def apply(): ReadFileSystem = new ReadFileSystemImpl()
}

private[fs] class ReadFileSystemImpl extends ReadFileSystem {
  def read(location: URI): Seq[SimpleFileInfo] = {
    try {
      val res = Try(new ReadWithDbUtils().read(location)) match {
        case Success(files) => Success(files)
        case Failure(exp) =>
          // add here other schemes s3, gc...
          location.getScheme match {
            case "abfs" | "abfss" => Try(new ReadAzure().read(location))
            case "s3" | "s3a" => Try(new ReadS3().read(location))
            case _ => Failure(new UnsupportedOperationException(s"Unsupported scheme: ${location.getScheme}", exp))
          }
      }
      res match {
        case Success(files) => files
        case Failure(exp) =>
          logError("Cannot read the FS for the following exception", exp)
          Seq.empty[SimpleFileInfo]
      }
    } catch {
      case e: NoClassDefFoundError =>
        logError("Class not found", e)
        Seq.empty[SimpleFileInfo]
    }
  }
}
