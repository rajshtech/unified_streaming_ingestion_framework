package com.unified_streaming_ingestion_framework.spark.components

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

trait FileBasedConfig {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
   * This method reads & parses the app config file
   *
   * @param path - path of the file
   * @return Config - appConfig
   */
  def getConfig(path: String)(implicit spark: SparkSession): Config = {
    Try {
      logger.info(s"ATTEMPTING TO READ CONFIG FILE: $path!")

      val myConfig = spark.read.textFile(path).collect().mkString("\n")
      val appConfig = ConfigFactory.parseString(myConfig)
      appConfig
    } match {
      case Success(appConfig) =>
        logger.info(s"read successful from config path: $path")
        appConfig

      case Failure(exception: Exception) =>
        logger.info(s"Failed to read : $path, Exception -> $exception")
        logger.error("stack trace : " + exception.printStackTrace())
        sys.exit()

    }

  }
}
