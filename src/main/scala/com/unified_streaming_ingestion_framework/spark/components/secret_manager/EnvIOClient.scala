package com.unified_streaming_ingestion_framework.spark.components.secret_manager

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
/**
 * VaultClient class is used to get the secrets from env variables if the env type is env
 *
 */
class EnvIOClient extends SecretsManager {

  val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

  override def resolveConfigs(envConfig: Config): Config = {
    logger.info("Getting the secrets from ENV initiated")
    var config = ConfigFactory.empty()
    val fetch_creds = envConfig.getConfigList("fetch_creds").asScala.toList

    fetch_creds.foreach(creds => {
      val keys = creds.getStringList("keys").asScala.toList
      val configType = creds.getString("type")
      val secrets = getSecrets(keys)
      config = addToConfig(configType, secrets, config)
    })
    logger.info("Getting the secrets from ENV completed")
    config
  }

  private def getSecrets(keys: List[String]): Map[String, String] = {

    keys.map(key => {
      if (key.contains(":")) {
        val arr = key.split(":")
        (arr(0), System.getenv(arr(1)))
      }
      else {
        (key, System.getenv(key))
      }
    }
    ).toMap

  }
}

object EnvIOClient {
  def apply(): EnvIOClient = {
    new EnvIOClient()
  }
}