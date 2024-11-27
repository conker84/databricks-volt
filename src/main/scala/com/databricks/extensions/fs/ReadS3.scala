package com.databricks.extensions.fs

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, AwsSessionCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetBucketLocationRequest, ListObjectsV2Request, S3Exception, S3Object}
import software.amazon.awssdk.regions.Region
import com.databricks.sdk.scala.dbutils.DBUtils

import java.net.URI
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter


private[fs] class ReadS3 extends ReadFileSystem {

  @tailrec
  private def getBucketRegion(bucketName: String, dbUtils: DBUtils, region: Option[Region] = None): Option[Region] = {
    try {
      val defaultRegionClient = S3Client.builder()
        .credentialsProvider(
          StaticCredentialsProvider.create(
            createCredentials(dbUtils)
          )
        )
      region.foreach(defaultRegionClient.region)
      val request = GetBucketLocationRequest.builder().bucket(bucketName).build()
      val response = defaultRegionClient.build().getBucketLocation(request)
      val regionStr = response.locationConstraintAsString()
      Some(Region.of(regionStr))
    } catch {
      case _: Throwable =>
        logInfo(s"Bucket not found in ${region.map(_.id()).getOrElse("default")}, trying with another")
        // if is not in the default region we scan all the regions
        val newRegionIndex = region
          .map(r => {
            val lastIndex = Region.regions().indexOf(r)
            lastIndex + 1
          })
          .orElse(if (region.isEmpty) Some(0) else None)
        if (newRegionIndex.isDefined) {
          getBucketRegion(bucketName, dbUtils, newRegionIndex.map(Region.regions().get))
        } else {
          logWarning(s"Region not found for bucket $bucketName")
          None
        }
    }
  }

  private def createCredentials(dbUtils: DBUtils) = {
    try {
      AwsSessionCredentials.create(
        dbUtils.secrets.get("aws-s3-credentials", "client_id"),
        dbUtils.secrets.get("aws-s3-credentials", "client_secret"),
        dbUtils.secrets.get("aws-s3-credentials", "session_token"),
      )
    } catch {
      case _: Throwable => AwsBasicCredentials.create(
        dbUtils.secrets.get("aws-s3-credentials", "client_id"),
        dbUtils.secrets.get("aws-s3-credentials", "client_secret"),
      )
    }
  }

  private def listFilesRecursively(s3Client: S3Client, location: URI): List[S3Object] = {
    var continuationToken: Option[String] = None
    var files: List[S3Object] = List.empty[S3Object]

    val bucketName = location.getHost
    val prefix = location.getPath.substring(1)

    do {
      val requestBuilder = ListObjectsV2Request.builder()
        .bucket(bucketName)
        .prefix(prefix)
        .maxKeys(1000)

      continuationToken.foreach(token => requestBuilder.continuationToken(token))

      val request = requestBuilder.build()
      val result = s3Client.listObjectsV2(request)

      // Collect file paths
      val currentBatch = result
        .contents()
        .asScala

      files ++= currentBatch

      // Update continuation token
      continuationToken = Option(result.nextContinuationToken())
    } while (continuationToken.isDefined)
    files
  }

  override def read(location: URI): Seq[SimpleFileInfo] = {
    val dbutils: DBUtils = DBUtils.getDBUtils()
    getBucketRegion(location.getHost, dbutils)
      .map(region => {
        // Create an S3 client with service principal authentication
        val s3Client = S3Client.builder()
          .credentialsProvider(
            StaticCredentialsProvider.create(createCredentials(dbutils))
          )
          .region(region)
          .build()

        // List all files recursively
        listFilesRecursively(s3Client, location)
      })
      .getOrElse(Seq.empty[S3Object])
      .map(s3o => SimpleFileInfo(s3o.key().split('/').last, s3o.key(), s3o.size()))
  }
}
