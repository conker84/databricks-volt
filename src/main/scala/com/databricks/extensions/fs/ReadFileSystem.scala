package com.databricks.extensions.fs

import org.apache.spark.internal.Logging

import java.net.URI
import scala.util.{Failure, Success, Try}

case class SimpleFileInfo(name: String, path: String, size: Long)

trait ReadFileSystem {
  def read(location: URI): Seq[SimpleFileInfo]
}

class MultiCloudFSReader extends ReadFileSystem with Logging {
  def read(location: URI): Seq[SimpleFileInfo] = {
    val res = Try(new ReadWithDbUtils().read(location)) match {
      case Success(files) => Success(files)
      case Failure(exp) =>
        // add here other schemes s3, gc...
        location.getScheme match {
          case "abfss" => Try(new ReadAzure().read(location))
          case _ => Failure(new UnsupportedOperationException(s"Unsupported scheme: ${location.getScheme}", exp))
        }
    }
    res match {
      case Success(files) => files
      case Failure(exp) =>
        logError("Cannot read the FS for the following exception", exp)
        Seq.empty[SimpleFileInfo]
    }
  }
}
