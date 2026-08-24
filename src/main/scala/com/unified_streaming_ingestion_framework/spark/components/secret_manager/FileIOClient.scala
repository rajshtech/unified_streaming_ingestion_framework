package com.unified_streaming_ingestion_framework.spark.components.secret_manager

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.io.Source

/**
 * VaultClient class is used to get the secrets from file if the env type is file
 *
 *
 */
class FileIOClient extends SecretsManager {

  val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)
  override def resolveConfigs(envConfig: Config): Config = {
    logger.info("Getting the secrets from File initiated")
    var config = ConfigFactory.empty()
    val fetch_creds = envConfig.getConfigList("fetch_creds").asScala.toList

    fetch_creds.foreach(creds => {
      val path = creds.getString("path")
      val keys = creds.getStringList("keys").asScala.toList
      val configType = creds.getString("type")
      val secrets = getSecrets(path, keys).toMap
      config = addToConfig(configType, secrets, config)
    })
    logger.info("Getting the secrets from File completed")
    config
  }

  private def getSecrets(secretPath: String, keys: List[String]): Map[String, String] = {
    val source = Source.fromFile(secretPath)
    val keyValueMap = scala.collection.mutable.Map.empty[String, String]
    try {
      for (line <- source.getLines() if line.trim.nonEmpty && (line.contains(":") || line.contains("="))) {
        val separator = if (line.contains(":")) ":" else "="
        val separatorIndex = line.indexOf(separator)
        val key = line.substring(0, separatorIndex).trim
        val value = line.substring(separatorIndex + 1).trim.stripPrefix("\"").stripSuffix("\"")
        keyValueMap += (key -> value)
      }
    } finally {
      source.close()
    }
    if (keys.isEmpty) {
      keyValueMap.toMap
    } else {
      keys.map(key => {
        if (key.contains(":")) {
          val arr = key.split(":")
          (arr(0), keyValueMap.getOrElse(arr(1), ""))
        }
        else {
          (key, keyValueMap.getOrElse(key, ""))
        }
      }
      ).toMap
    }

  }
}

object FileIOClient {
  def apply(): FileIOClient = {
    new FileIOClient()
  }
}