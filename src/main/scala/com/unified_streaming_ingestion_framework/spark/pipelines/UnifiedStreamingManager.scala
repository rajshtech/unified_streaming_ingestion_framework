package com.unified_streaming_ingestion_framework.spark.pipelines

import com.unified_streaming_ingestion_framework.spark.components.transformers.{StreamingTransform, StreamingTransformFactory}
import com.unified_streaming_ingestion_framework.spark.components.utils.CommonUtils.{EmailNotification, GracefullyStopStreamingApplication, ShutdownHook, emailBodyMessage, getTopics, initializeShutdownHook, setS3ConfigIfRequired}
import com.unified_streaming_ingestion_framework.spark.components.{ConfigUtil, FileBasedConfig}
import com.unified_streaming_ingestion_framework.spark.components.utils.CommonUtils.{EmailNotification, ShutdownHook, getTopics, initializeShutdownHook}
import com.typesafe.config.Config
import com.unified_streaming_ingestion_framework.spark.components.FileBasedConfig
import com.unified_streaming_ingestion_framework.spark.components.secret_manager.SecretsEngine
import com.unified_streaming_ingestion_framework.spark.components.sinks.StreamingSinkBuilder
import com.unified_streaming_ingestion_framework.spark.components.sources.StreamingSourceBuilder
import com.unified_streaming_ingestion_framework.spark.components.spark_utils.EmailUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

trait UnifiedStreamingManager extends FileBasedConfig {

  implicit protected var spark: SparkSession = _
  protected var runtimeArgs: Map[String, String] = Map.empty

  protected var config: Config = _
  protected var envConfig: Config = _
  protected var sourceConfig: Config = _
  protected var sinkConfig: Config = _
  protected var controlConfig: Config = _
  protected var emailConfig: Config = _

  protected var environment: String = _
  protected var sinkLocation: String = _
  protected var shutdownHook: ShutdownHook = _
  protected var emailNotifier: EmailNotification = _

  protected var kafkaPayloadFormat: String = _
  protected var jsonSchemaInferenceType: String = _

  protected var toAddress: String = _
  protected var fromAddress: String = _
  protected var emailHostName: String = _

  /**
   * Main entry point for StreamLake execution
   */
  def runPipeline(args: Array[String]): Unit = {
    try {
      parseArgs(args)
      validateArgs()
      initializeSpark()
      initializeMonitoring()

      val inputDf = readStream()
      val processedDf = applyTransformations(inputDf)
      writeStream(processedDf)

    } catch {
      case ex: Exception => handleFailure(ex)
    }
  }

  /**
   * Parse runtime arguments in key=value format
   */
  private def parseArgs(args: Array[String]): Unit = {
    runtimeArgs = args
      .map(_.split("=", 2))
      .collect { case Array(k, v) => k -> v }
      .toMap
  }

  /**
   * Validate mandatory runtime parameters
   */
  private def validateArgs(): Unit = {
    val requiredKeys = Seq("config_path", "pipeline_name", "app_name")
    requiredKeys.foreach { key =>
      if (!runtimeArgs.contains(key))
        throw new IllegalArgumentException(s"Missing required argument: $key")
    }
  }

  /**
   * Initialize Spark and load configs
   */
  private def initializeSpark(): Unit = {
    val builder = SparkSession.builder()
      .appName(s"${runtimeArgs("pipeline_name")}-${runtimeArgs("app_name")}")

    runtimeArgs.get("spark_master").foreach { master =>
      builder
        .master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    }

    spark = builder.getOrCreate()
    logger.info(s"Spark version: ${spark.version}")

    loadConfigs()
    setS3ConfigIfRequired(spark, envConfig)
  }

  /**
   * Load all application configurations
   */
  private def loadConfigs(): Unit = {
    config = getConfig(runtimeArgs("config_path"))

    emailConfig = config.getConfig("email")
    envConfig = config.getConfig("envConfig")
    sourceConfig = config.getConfig("source")
    sinkConfig = config.getConfig("target")
    controlConfig = config.getConfig("appControlConfig")

    toAddress = emailConfig.getString("email_to_address")
    fromAddress = emailConfig.getString("email_from_address")
    emailHostName = emailConfig.getString("email_hostname")
    environment = emailConfig.getString("environment")

    kafkaPayloadFormat =
      sourceConfig.getStringOpt("kafka_payload_format").getOrElse("avro")

    jsonSchemaInferenceType =
      sourceConfig.getStringOpt("json_schema_inference_type").getOrElse("infer_as_value")

    envConfig = envConfig.withFallback(
      SecretsEngine.getConfigSecrets(
        envConfig,
        System.getenv("vault_role_id"),
        System.getenv("vault_secret_id")
      )
    )
  }

  /**
   * Initialize shutdown hooks and notifications
   */
  private def initializeMonitoring(): Unit = {
    shutdownHook = initializeShutdownHook(controlConfig)

    emailNotifier = EmailNotification(
      toAddress,
      environment,
      runtimeArgs("pipeline_name"),
      runtimeArgs("app_name"),
      sinkLocation,
      emailHostName,
      fromAddress,
      getTopics(sourceConfig, envConfig).mkString(",")
    )
  }

  /**
   * Read streaming data from source
   */
  protected def readStream(): DataFrame = {
    StreamingSourceBuilder
      .createSource(envConfig, sourceConfig, sinkConfig, shutdownHook, emailNotifier)
      .read(spark)
  }

  /**
   * Apply optional transformation logic
   */
  protected def applyTransformations(df: DataFrame): DataFrame = {
    if (config.hasPath("transform")) {
      StreamingTransformFactory
        .make(config.getConfig("transform"))
        .transform(df)
    } else df
  }

  /**
   * Write stream to sink
   */
  protected def writeStream(df: DataFrame): Unit = {
    val sink = StreamingSinkBuilder.createSink(envConfig, sinkConfig, sourceConfig)
    sinkLocation = sink.getSinkLocation
    sink.write(df).awaitExternalTermination(shutdownHook)
  }

  /**
   * Unified failure handling
   */
  private def handleFailure(ex: Exception): Unit = {
    logger.error("StreamLake pipeline execution failed", ex)

    val topics =
      if (sourceConfig.hasPath("topics"))
        sourceConfig.getStringList("topics").asScala.mkString(",")
      else if (sourceConfig.hasPath("topic_pattern"))
        sourceConfig.getString("topic_pattern")
      else ""

    if (toAddress != null && toAddress.trim.nonEmpty) {
      try {
        EmailUtils.sendHtmlErrorMail(
          toAddress,
          s"${environment.toUpperCase} - StreamLake Pipeline Failure",
          emailBodyMessage(runtimeArgs("app_name"), topics, sinkLocation, ex.getMessage),
          emailHostName,
          fromAddress
        )
      } catch {
        case mailEx: Exception =>
          logger.warn(s"Failed to send failure notification email: ${mailEx.getMessage}")
      }
    } else {
      logger.warn("No email_to_address configured - skipping failure notification email")
    }

    shutdownHook.applicationLock.set(false)
    sys.exit(1)
  }
}
