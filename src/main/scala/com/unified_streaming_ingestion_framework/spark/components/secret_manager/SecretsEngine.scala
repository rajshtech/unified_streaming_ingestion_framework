package com.unified_streaming_ingestion_framework.spark.components.secret_manager

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.spark.internal.Logging

object SecretsEngine extends Logging{
  def getConfigSecrets(envConfig: Config, vault_role_id: String, vault_secret_id: String): Config = {

    // Fetches the secrets and configs that are passed through vault & appends to envConfig
    val resolvedVaultConfig: Config = if (envConfig.hasPath("vault")) {
      var vaultConfig = envConfig.getConfig("vault")
      val roleIdConfigValue = ConfigValueFactory.fromAnyRef(vault_role_id)
      val secretIdConfigValue = ConfigValueFactory.fromAnyRef(vault_secret_id)
      vaultConfig = vaultConfig.withValue(
        "roleId", roleIdConfigValue
      ).withValue(
        "secretId", secretIdConfigValue
      )
      val vaultClient = VaultIOClient(vaultConfig)
      vaultClient.resolveConfigs(vaultConfig)
    } else {
      logInfo("The vault secrets are empty")
      ConfigFactory.empty()
    }
    // Fetches the secrets and configs that are passed as env variables & appends to envConfig
    val envSecretConfig: Config = if (envConfig.hasPath("env")) {
      var enviConfig = envConfig.getConfig("env")
      val envSecrets = EnvIOClient()
      val envSecretConfig = envSecrets.resolveConfigs(enviConfig)
      envSecretConfig
    } else {
      logInfo("The ENV secrets are empty")
      ConfigFactory.empty()
    }

    // Fetches the secrets and configs that are passed through file & appends to envConfig
    val fileSecretConfig: Config = if (envConfig.hasPath("file")) {
      var fileConfig = envConfig.getConfig("file")
      val fileSecrets = FileIOClient()
      val fileSecretConfig = fileSecrets.resolveConfigs(fileConfig)
      fileSecretConfig
    } else {
      logInfo("The File secrets are empty")
      ConfigFactory.empty()
    }

    // Fetches the secrets and configs that are passed through api & appends to envConfig
    val apiSecretConfig: Config = if (envConfig.hasPath("api")) {
      var fileConfig = envConfig.getConfig("api")
      val fileSecrets = HttpsIOClient()
      val fileSecretConfig = fileSecrets.resolveConfigs(fileConfig)
      fileSecretConfig
    } else {
      logInfo("The API secrets are empty")
      ConfigFactory.empty()
    }

    val finalConfig: Config = resolvedVaultConfig.withFallback(envSecretConfig).withFallback(fileSecretConfig).withFallback(apiSecretConfig)
    finalConfig.entrySet().forEach(entry => {
      finalConfig.withValue(entry.getKey, ConfigValueFactory.fromAnyRef(entry.getValue))
    })
    finalConfig
  }

}
