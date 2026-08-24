package com.unified_streaming_ingestion_framework.spark.components.secret_manager

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

trait SecretsManager {
  def addToConfig(configType: String, secrets: Map[String, Any], resultConfig: Config): Config = {
    var config = resultConfig
    secrets.foreach(rec =>
      config = config.withValue(
        s"${configType}.${rec._1}", ConfigValueFactory.fromAnyRef(rec._2))
    )
    config
  }

  def resolveConfigs(vaultConfig: Config): Config
}