package com.databricks.volt.fs

import com.azure.identity.ClientSecretCredentialBuilder
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder
import com.databricks.sdk.scala.dbutils.DBUtils

import java.net.URI
import scala.jdk.CollectionConverters.asScalaIteratorConverter


private[fs] class ReadAzure extends ReadFileSystem {

  override def read(location: URI): Seq[SimpleFileInfo] = {
    val dbutils: DBUtils = DBUtils.getDBUtils()
    val accountName = location.getHost.split('.').head
    val containerName = location.getUserInfo
    val blobName = location.getPath

    // Authenticate using ClientSecretCredential
    val credential = new ClientSecretCredentialBuilder()
      .tenantId(dbutils.secrets.get("adls-sp-credentials", "tenant_id"))
      .clientId(dbutils.secrets.get("adls-sp-credentials", "client_id"))
      .clientSecret(dbutils.secrets.get("adls-sp-credentials", "client_secret"))
      .build()

    // Access the container
    val serviceClient = new DataLakeServiceClientBuilder()
      .credential(credential)
      .endpoint(s"https://$accountName.dfs.core.windows.net")
      .buildClient()

    // Access the file system client and directory client
    val fileSystemClient = serviceClient.getFileSystemClient(containerName)
    val directoryClient = fileSystemClient.getDirectoryClient(blobName)

    // List all files in the directory
    directoryClient
      .listPaths(true, false, null, null)
      .iterator()
      .asScala
      .filterNot(_.isDirectory)
      .map(b => SimpleFileInfo(b.getName.split('/').last, b.getName, b.getContentLength))
      .toSeq
  }
}
