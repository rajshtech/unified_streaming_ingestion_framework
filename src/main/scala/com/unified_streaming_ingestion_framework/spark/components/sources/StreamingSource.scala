package com.unified_streaming_ingestion_framework.spark.components.sources

import com.unified_streaming_ingestion_framework.spark.components.utils.CommonUtils.{EmailNotification, ShutdownHook}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Streaming Source manages the type of Source to choose from config and calls the respective sink.
 * And this enforce the Source standard methods to call.
 */
abstract class StreamingSource {
  def sourceFormat: String

  def readStreamOptions: Map[String, String]

  def read(spark: SparkSession): DataFrame = {
    spark.readStream.format(sourceFormat).options(readStreamOptions).load()
  }
}

object StreamingSourceBuilder {
  def createSource(envConfig: Config, sourceConfig: Config, sinkConfig: Config, shutdownHook: ShutdownHook, emailNotification: EmailNotification): StreamingSource = {

    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    logger.info(s"Source type: ${sourceConfig.getString("source_type")}")
    val sourceType = sourceConfig.getString("source_type")

    sourceType.toLowerCase match {
      case "kafka" => KafkaStreamSource(sourceConfig, envConfig, sinkConfig, shutdownHook, emailNotification)
//      case "s3" => S3StreamSource(sourceConfig)
      case _ => throw new IllegalArgumentException("Unsupported source type")
    }
  }
}

