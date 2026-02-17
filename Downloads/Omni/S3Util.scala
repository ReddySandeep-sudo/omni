package com.nielsen.sei.decisionEngine.utils

import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.google.inject.Inject
import org.apache.spark.internal.Logging

@Inject
class S3Util extends Logging with Serializable {
  def getS3Client(region: String): AmazonS3 = {
    val s3Client = AmazonS3ClientBuilder.standard.withRegion(region).build
    s3Client
  }
}