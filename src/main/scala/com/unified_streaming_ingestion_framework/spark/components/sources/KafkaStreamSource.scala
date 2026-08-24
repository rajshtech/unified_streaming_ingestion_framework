package com.unified_streaming_ingestion_framework.spark.components.sources

import com.unified_streaming_ingestion_framework.spark.components.utils.CommonUtils.{EmailNotification, ShutdownHook, configureStartingOffset, getKafkaParams, getKafkaProps, getReadOrWriteStreamOptions, getSchemaFromJson}
import com.typesafe.config.Config
import com.unified_streaming_ingestion_framework.spark.components.{ConfigUtil, Constants}
import com.unified_streaming_ingestion_framework.spark.components.listeners.KafkaLagListener
import com.unified_streaming_ingestion_framework.spark.components.utils.CommonUtils
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.abris.avro.functions.from_avro
import za.co.absa.abris.config.{AbrisConfig, FromAvroConfig}


class KafkaStreamSource(
                         options: Map[String, String],
                         shutdownHook: ShutdownHook,
                         emailNotification: EmailNotification,
                         sinkConfig: Config,
                         kafkaPayloadFormat: String,
                         jsonSchemaInferenceType: String,
                         jsonSchemaString: String,
                         sourceConfig: Config,
                         envConfig: Config
                       ) extends StreamingSource {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def sourceFormat: String = "kafka"

  override def read(sparkSession: SparkSession): DataFrame = {
    withLagListener(sparkSession)

    var df = super.read(sparkSession)
    df = if (kafkaPayloadFormat.toLowerCase == "json" && jsonSchemaInferenceType.toLowerCase == "infer_as_struct"){
      //Infers the schema from struct type which require to provide in config file.
      val jsonSchema = getSchemaFromJson(jsonSchemaString)
      df.withColumn("recordValue", from_json(col("value").cast("string"), jsonSchema))
    } else if (
      kafkaPayloadFormat.toLowerCase == "json"
        && (jsonSchemaInferenceType.toLowerCase == "infer_as_value"
        || jsonSchemaInferenceType.toLowerCase == "infer_schema_dynamically")
    ){
      //Infers the schema as value string which require to convert the value to string in config file.
      df
    }
    else {
      //This is default format which is AVRO (not required to provide any formats for Kafka in config file.
      var schemaRegistryConfig = Map[String, String]()
      val schemaRegistryEnabled = envConfig.getStringOpt(
        "vault_schema_registry_enabled"
      ).getOrElse("false").toBoolean
      if (schemaRegistryEnabled) {
        logger.info("schema Registry enabled for Kafka Server - reading secrets from Vault")
        schemaRegistryConfig = CommonUtils.getSchemaRegistryConfigs(options, envConfig)
      }

      val avroConfig: FromAvroConfig = AbrisConfig
        .fromConfluentAvro
        .downloadReaderSchemaByLatestVersion
        .andTopicNameStrategy(CommonUtils.getTopics(sourceConfig, envConfig).head)
        .usingSchemaRegistry(schemaRegistryConfig)

      df.withColumn("recordValue", from_avro(col("value"), avroConfig))
    }
    df
  }

  def withLagListener(sparkSession: SparkSession): Unit = {
    val kafkaParams = getKafkaProps(options)
    val metricsLagListener =
      new KafkaLagListener(
        sparkSession.sparkContext.appName,
        getKafkaBrokers,
        shutdownHook,
        kafkaParams,
        emailNotification,
        sinkConfig)
    // Register a query listener to capture progress events
    sparkSession.streams.addListener(metricsLagListener)
  }

  private def getKafkaBrokers: String = readStreamOptions
    .getOrElse("kafka.bootstrap.servers", throw new IllegalArgumentException("No kafka brokers provided"))

  override def readStreamOptions: Map[String, String] = options


}

object KafkaStreamSource {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def apply(
             sourceConfig: Config,
             envConfig: Config,
             sinkConfig: Config,
             shutdownHook: ShutdownHook,
             emailNotification: EmailNotification
           ): KafkaStreamSource = {

    val kafkaConfigs = getKafkaParams(sourceConfig, envConfig)
    val startingOffsetConfigValue = configureStartingOffset(sourceConfig, sinkConfig, envConfig).toString

    val kafkaPayloadFormat = sourceConfig.getStringOpt("kafka_payload_format").getOrElse("avro")
    val jsonSchemaInferenceType = sourceConfig.getStringOpt("json_schema_inference_type").getOrElse("infer_as_value")
    val jsonSchemaString = sourceConfig.getStringOpt("infer_as_struct").getOrElse("")


    var kafkaParams = kafkaConfigs
    val topics = CommonUtils.getTopics(sourceConfig, envConfig).mkString(",")
    // Append min partitions if enabled
    val minPartitionsEnable = sourceConfig.getStringOpt("minPartitions_enabled").getOrElse("false").toBoolean
    if (minPartitionsEnable) {
      kafkaParams = kafkaConfigs ++ Map[String, String](
        "minPartitions" -> Math.min(
          Math.max(
            sourceConfig.getString("minPartitions").toInt,
            Constants.SPARK_KAFKA_MIN_PARTITION),
          Constants.SPARK_KAFKA_MAX_PARTITION).toString
      )
    }

    // Read options for spark
    val readOptionsMap: Map[String, String] = getReadOrWriteStreamOptions(sourceConfig, "read_options").getOrElse(Map.empty[String, String])
    // Append topics
    kafkaParams = kafkaParams ++ Map[String, String](
      "subscribe" -> topics
    ) ++ readOptionsMap

    if (startingOffsetConfigValue.nonEmpty) {
      kafkaParams ++ Map[String, String](
        "startingOffsets" -> startingOffsetConfigValue
      )
    }

    new KafkaStreamSource(
      kafkaParams,
      shutdownHook,
      emailNotification,
      sinkConfig,
      kafkaPayloadFormat,
      jsonSchemaInferenceType,
      jsonSchemaString,
      sourceConfig,
      envConfig
    )

  }
}
