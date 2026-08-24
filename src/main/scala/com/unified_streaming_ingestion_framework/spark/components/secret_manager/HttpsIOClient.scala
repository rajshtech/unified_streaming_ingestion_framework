package com.unified_streaming_ingestion_framework.spark.components.secret_manager

import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

/**
 *  class is used to get the secrets from Rest API if the env type is api
 *
 *  Not implemented yet - this pipeline currently only uses file-based secrets.
 */
class HttpsIOClient extends SecretsManager {

  val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

  override def resolveConfigs(envConfig: Config): Config =
    throw new UnsupportedOperationException("API-based secrets resolution (HttpsIOClient) is not implemented yet")
}

object HttpsIOClient {
  def apply(): HttpsIOClient = {
    new HttpsIOClient()
  }
}
