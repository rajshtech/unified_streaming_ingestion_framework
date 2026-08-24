package com.unified_streaming_ingestion_framework.spark.components.secret_manager

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.vault.authentication.{AppRoleAuthentication, AppRoleAuthenticationOptions}
import org.springframework.vault.client.{ClientHttpRequestFactoryFactory, VaultClients, VaultEndpoint}
import org.springframework.vault.core.VaultTemplate
import org.springframework.vault.support.{ClientOptions, SslConfiguration}

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * VaultClient class is used to get the secrets from vault if the env type is vault
 *
 * @param host
 * @param port
 * @param roleId
 * @param secretId
 * @param nameSpace
 *
 */
class VaultIOClient(
                   host: String,
                   port: Int,
                   roleId: String,
                   secretId: String,
                   nameSpace: String,
                   retries: Int
                 ) extends SecretsManager {

  val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)
  override def resolveConfigs(vaultConfig: Config): Config = {
    logger.info("Getting the secrets from Vault initiated")
    var config = ConfigFactory.empty()
    val fetch_creds = vaultConfig.getConfigList("fetch_creds").asScala.toList
    val vaultTemplate = createVaultInstance()
    fetch_creds.foreach(creds => {
      val path = creds.getString("path")
      val keys = creds.getStringList("keys").asScala.toList
      val configType = creds.getString("type")
      val secrets = getSecrets(path, keys, vaultTemplate)
      config = addToConfig(configType, secrets, config)
    })
    logger.info("Getting the secrets from Vault completed, revoking the active session token.")
    vaultTemplate.destroy()
    config
  }

  private def getSecrets(
                          secretPath: String,
                          keys: List[String],
                          vaultTemplate: VaultTemplate
                        ): Map[String, String] = {
    var attempt = 0
    var lastException: Exception = null

    while (attempt < retries) {
      try {
        val response = vaultTemplate.read(secretPath)
        val secrets = response.getData.get("data").asInstanceOf[
          java.util.LinkedHashMap[String, String]
        ]

        if (keys.isEmpty) {
          return secrets.asScala.toMap
        }
        else {
          return keys.map(key => {
            if (key.contains(":")) {
              val arr = key.split(":")
              (arr(0), secrets.get(arr(1)))
            }
            else {
              (key, secrets.get(key))
            }
          }
          ).toMap
        }
      } catch {
        case e: Exception =>
          lastException = e
          logger.error(s"Vault read failed on attempt ${attempt + 1}: ${e.getMessage}")
          Thread.sleep(5 * attempt)
          attempt += 1
      }
    }

    throw new RuntimeException(
      s"Failed to read secrets from vault after $retries attempts", lastException
    )

  }

  private def vaultInitialize: VaultEndpoint = {
    logger.info("initializing vault")
    val vault = VaultEndpoint.create(host, port)
    vault.setScheme("https")
    vault

  }

  private def createVaultInstance(): VaultTemplate = {
    logger.info("Creating the vault active session token")
    val vault = vaultInitialize
    val options = AppRoleAuthenticationOptions.builder.roleId(
      AppRoleAuthenticationOptions.RoleId.provided(roleId)
    ).secretId(
      AppRoleAuthenticationOptions.SecretId.provided(secretId)
    ).build()

    val restTemplate = VaultClients.createRestTemplate(
      vault,
      ClientHttpRequestFactoryFactory.create(
        new ClientOptions,
        SslConfiguration.unconfigured()
      )
    )
    restTemplate.getInterceptors.add(VaultClients.createNamespaceInterceptor(nameSpace))
    val auth = new AppRoleAuthentication(
      options,
      restTemplate
    )
    val vaultTemplate = new VaultTemplate(vault, auth)
    vaultTemplate
  }
}

object VaultIOClient {
  def apply(vaultConfig: Config): VaultIOClient = {
    val retries = Try(vaultConfig.getInt("retries")).toOption.getOrElse(3)
    new VaultIOClient(
      vaultConfig.getString("url"),
      vaultConfig.getInt("port"),
      vaultConfig.getString("roleId"),
      vaultConfig.getString("secretId"),
      vaultConfig.getString("nameSpace"),
      retries
    )
  }
}