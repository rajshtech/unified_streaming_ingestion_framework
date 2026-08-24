package com.unified_streaming_ingestion_framework.spark.components.utils

import com.unified_streaming_ingestion_framework.spark.components.shutdown_manager.SparkShutdownHook
import com.unified_streaming_ingestion_framework.spark.components.spark_utils.SparkIOUtils._
import com.unified_streaming_ingestion_framework.spark.components.spark_utils.StopFlag
import com.unified_streaming_ingestion_framework.spark.components.sources.KafkaStreamSource.logger
import com.unified_streaming_ingestion_framework.spark.components.utils.CheckpointAutoOffset.{extractOffsetMetadata, getNewStartingOffsets}
import com.typesafe.config.Config
import com.unified_streaming_ingestion_framework.spark.components.ConfigUtil
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.JsonProperties.NULL_VALUE
import org.apache.avro.Schema
import org.apache.hadoop.fs.CommonConfigurationKeysPublic
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{DataType, StructType}
import za.co.absa.abris.config.AbrisConfig
import org.apache.spark.sql.types._
import org.json4s.JsonAST.JArray
import org.json4s.jackson.{Json, JsonMethods}
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import za.co.absa.abris.avro.read.confluent.SchemaManager

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConverters._


object CommonUtils {

  /**
   * This method returns the schema registry configs arguments.
   *
   * @param kafkaConfigs - kafka configuration parameters.
   * @param envConfig    - env configuration that parsed from app config file.
   * @return Map of schema registry params
   */
  def getSchemaRegistryConfigs(kafkaConfigs: Map[String, String], envConfig: Config): Map[String, String] = {
    Map[String, String](
      "schema.registry.url" -> kafkaConfigs("kafka_schema_registry"),
      "basic.auth.credentials.source" -> envConfig.getString("basic_auth_credentials_source"),
      //      "ssl.ca.location" -> "\\projects\\hot_fix\\spark_streaming_framework\\cacert.pem",
      "basic.auth.user.info" -> s"${envConfig.getString("kafka.username")}:${envConfig.getString("kafka.password")}",
      "schema.registry.ssl.truststore.location" -> "//Documents//downlds//server.truststore.jks",
      "schema.registry.ssl.truststore.password" -> ""
    )
  }

  // Not using method as latest schema being pulled by setting up AbrisConfig but keeping just for reference
  def latestVersionSchema(sourceConfig: Config, isKey: Boolean, schemaRegistryConfig: Map[String, String], envConfig: Config): Schema = {
    val topicName = getTopics(sourceConfig, envConfig).head
    val baseURL = schemaRegistryConfig.getOrElse(AbrisConfig.SCHEMA_REGISTRY_URL, null)
    val schemaRegistryConfig1 = schemaRegistryConfig.asJava
    val client = new CachedSchemaRegistryClient(baseURL, 999, schemaRegistryConfig1)
    val subject = if (isKey) s"${topicName}-key" else s"${topicName}-value"
    val schemaMetadata = client.getLatestSchemaMetadata(subject)

    val avroSchema: Schema = new Schema.Parser().parse(schemaMetadata.getSchema)
    avroSchema
  }


  def extractTimeStampFields(avroSchema: Schema): Seq[(String, String)] = {
    avroSchema.getFields.asScala.flatMap { field =>
      val logicalType = Option(field.schema().getProp("logicalType"))
      logicalType match {
        case Some("timestamp-millis") => Some((field.name(), "millis"))
        case Some("timestamp-micros") => Some((field.name(), "micros"))
        case _ => None
      }
    }.toSeq
  }

  def parseDefaultForLogical(field: Field, logicalType: String): AnyRef = {
    import java.time.Instant
    val default = field.defaultVal()
    (logicalType, default) match {
      case ("timestamp-millis", s: String) =>
        java.lang.Long.valueOf(Instant.parse(s).toEpochMilli)
      case ("timestamp-micros", s: String) =>
        java.lang.Long.valueOf(Instant.parse(s).toEpochMilli * 1000)
      case (_, v) =>
        v
    }
  }


  def applyLogicalTypesWithDefaults(
                                     schema: Schema,
                                     sourceConfig: Config
                                   ): Schema = {

    val debeziumFieldsConfig = sourceConfig.getConfig("debezium_field_conversions")
    val connectNameToLogical: Map[String, (String, String)] = debeziumFieldsConfig.root().keySet().asScala
      .filterNot(_ == "default_fields")
      .flatMap { logicalType =>
        val logicalTypeConfig = debeziumFieldsConfig.getConfig(logicalType)
        val baseType = logicalTypeConfig.getString("base_type")
        val types = logicalTypeConfig.getStringList("types").asScala
        types.map(t => t -> (logicalType, baseType))
      }.toMap

    println(s"connectNameToLogical $connectNameToLogical")

    val defaultFields: Set[String] = if (debeziumFieldsConfig.hasPath("default_fields")) {
      debeziumFieldsConfig.getStringList("default_fields").asScala.toSet
    } else Set.empty

    def toAvroType(name: String): Schema.Type = name.toLowerCase match {
      case "long"   => Schema.Type.LONG
      case "int"    => Schema.Type.INT
      case "bytes"  => Schema.Type.BYTES
      case "string" => Schema.Type.STRING
      case other    => throw new IllegalArgumentException(s"Unsupported base type: $other")
    }

    def createLogicalSchema(baseType: String, logicalType: String): Schema = {
      val s = Schema.create(toAvroType(baseType))
      println(f"s is $s")
      s.addProp("logicalType", logicalType)
      println(f"s after addprop is $s")
      s
    }

    def updateSchema(schema: Schema): Schema = {
      schema.getType match {
        case Schema.Type.RECORD =>
          val updatedFields = schema.getFields.asScala.map { field =>

            println(f"field is $field")
            val fieldSchema = field.schema()
            val fieldName = field.name()
            val connectNameOpt = Option(fieldSchema.getProp("connect.name"))
            println(s"connectNameOpt: $connectNameOpt")
            //            val logicalTypeToKafkaConnectName: Map[String, String] = Map(
            //              "timestamp-millis" -> "org.apache.kafka.connect.data.Timestamp",
            //              "timestamp-micros" -> "org.apache.kafka.connect.data.MicroTimestamp",
            //              "decimal"          -> "org.apache.kafka.connect.data.Decimal",
            //              "date"             -> "org.apache.kafka.connect.data.Date",
            //              "time-millis"      -> "org.apache.kafka.connect.data.Time",
            //              "time-micros"      -> "org.apache.kafka.connect.data.MicroTime"
            //            )
            val maybeLogical =
              if (defaultFields.contains(fieldName)) Some(("timestamp-millis", "long"))
              else connectNameOpt.flatMap(connectNameToLogical.get)

            println(s"maybeLogical: $maybeLogical")

            val newSchema = maybeLogical match {
              case Some((logicalType, baseType)) =>
                val logicalSchema = createLogicalSchema(baseType, logicalType)
                println(s"logicalSchema is $logicalSchema")
                val finalSchema =
                  if (field.defaultVal() == null || field.defaultVal().toString == "null" || field.defaultVal() == NULL_VALUE) {
                    Schema.createUnion(List(Schema.create(Schema.Type.NULL), logicalSchema).asJava)
                  } else {
                    fieldSchema.addProp("logicalType", "timestamp-millis")

                    println(s"fieldSchema is $fieldSchema")
                    println(s"logicalSchema is $logicalSchema.")

                    fieldSchema.getObjectProps.asScala.foreach {
                      case (k, v) if k.equals("connect.") => logicalSchema.addProp(k, v)
                      case _ =>
                    }

                    println(s"fieldSchema after is $fieldSchema")

                    //                    logicalTypeToKafkaConnectName.get(logicalType).foreach { kafkaName =>
                    //                      logicalSchema.addProp("connect.name", kafkaName)
                    //                    }
                    fieldSchema
                  }
                // Copy non-connect props

                finalSchema
              case None =>
                updateSchema(fieldSchema)
            }

            val defaultVal = try {
              if (field.defaultVal() != NULL_VALUE && maybeLogical.nonEmpty) {
                println(field.defaultVal())
                parseDefaultForLogical(field = field, logicalType = maybeLogical.get._1)
              } else {
                field.defaultVal()
              }
            } catch {
              case _: Throwable => null
            }

            val updatedField = new Field(fieldName, newSchema, field.doc(), defaultVal)
            //            field.getObjectProps.asScala.foreach {
            //              case (k, v) if !k.startsWith("connect.") => updatedField.addProp(k, v)
            //              case _ =>
            //            }

            updatedField

          }

          Schema.createRecord(
            schema.getName,
            schema.getDoc,
            schema.getNamespace,
            schema.isError,
            updatedFields.asJava
          )

        case Schema.Type.UNION =>
          Schema.createUnion(schema.getTypes.asScala.map(updateSchema).asJava)
        case _ => schema

      }
    }
    updateSchema(schema)

  }

  /*def debeziumFieldOverrideProcess(schema: Schema): Schema = {
    val overrides = Map()
    schema.getType match {
      case Schema.Type.RECORD =>
        val updatedFields = schema.getFields.asScala.map { field =>
          val fieldSchema = field.schema()
          val logicalTypeFromOverride = overrides.get(field.name())

          val isDebeziumTimestamp = fieldSchema.getProp("connect.name") == ""

          val newFieldSchema =
            if (logicalTypeFromOverride.contains("timestamp-millis") || isDebeziumTimestamp) {
              val cleanSchema = Schema.create(Schema.Type.LONG)
              cleanSchema.addProp("logicalType", "timestamp-millis")
              cleanSchema
            } else {
              debeziumFieldOverrideProcess(fieldSchema)
            }

          val newField = new Field(field.name(), newFieldSchema, field.doc(), field.defaultVal())
          field.getObjectProps.asScala.foreach { case (k, v) =>
            if (!k.startsWith("connect.")) newField.addProp(k, v)

          }
          newField
        }
        Schema.createRecord(
          schema.getName,
          schema.getDoc,
          schema.getNamespace,
          schema.isError,
          updatedFields.asJava
        )
      case Schema.Type.UNION =>
        val processedTypes = schema.getTypes.asScala.map(debeziumFieldOverrideProcess)
        Schema.createUnion(processedTypes.asJava)
      case _ =>
        schema
    }
  }*/

  def addLogicalTypes(schema: Schema): Schema = {
    if (schema.getType == Schema.Type.RECORD) {
      val updatedFields = schema.getFields.toArray.map {
        case field: Field =>
          val updatedSchema = addLogicalTypes(field.schema())
          val newField = new Field(field.name(), updatedSchema, field.doc(), field.defaultVal())

          field.getObjectProps.forEach((k,v) => newField.addProp(k, v))
          if (newField.schema().getProp("name") == "starttime") {
            println(s"${newField.schema().getProp("name")}")
          }
          val connectName = field.schema().getProp("connect.name")
          if (connectName == "io.debezium.time.Timestamp") {
            val cleanSchema = Schema.create(Schema.Type.LONG)
            cleanSchema.addProp("logicalType", "timestamp-millis")

            val newField = new Field(field.name(), cleanSchema, field.doc(), field.defaultVal())
            field.getObjectProps.forEach((k,v) => newField.addProp(k, v))

            println(s"debezium timestamp exists for ${field.name()}")
            updatedSchema.addProp("logicalType", "timestamp")
          }
          newField
      }
      Schema.createRecord(schema.getName, schema.getDoc, schema.getNamespace, schema.isError, updatedFields.toList.asJava)
    } else if (schema.getType == Schema.Type.UNION) {
      val newTypes = schema.getTypes.toArray.map { case s: Schema => addLogicalTypes(s)}
      Schema.createUnion(newTypes.toList.asJava)
    } else {
      schema
    }
  }

  /**
   * This method gets the list of topics provided in the app config file.
   *
   * @param config    - either source config or sink config whichever having the topics in app config file.
   * @param envConfig - env configuration that parsed from app config file.
   * @return List of topics
   */
  def getTopics(config: Config, envConfig: Config): List[String] = {
    if (config.hasPath("topics")) {
      config.getStringList("topics").asScala.toList
    }
    else {
      val topicPattern = config.getString("topic_pattern")
      val kafkaOptions = getKafkaParams(config, envConfig)
      val params = mapAsJavaMap(kafkaOptions)
      CommonUtils.getSubscribeTopics(topicPattern, kafkaOptions).toArray().map(_.toString).toList
    }
  }

  /**
   * This method gets the Set of topics provided in the app config file by subscribing the topic pattern.
   *
   * @param subscribePattern - pattern of topic
   * @param options          - The Map of Kafka parameters that includes ssl configs, credentials etc.
   * @return Set of topics
   */
  def getSubscribeTopics(subscribePattern: String, options: Map[String, String]): util.Set[String] = {
    val kafkaParams = getKafkaProps(options)
    val subscribeTopics = new util.HashSet[String]()
    val consumer = new KafkaConsumer[String, String](kafkaParams)
    val allTopics = consumer.listTopics().keySet()
    val regex = subscribePattern.r()
    for (topic <- allTopics.asScala) {
      if (regex.pattern.matcher(topic).matches()) {
        subscribeTopics.add(topic)
      }
    }
    subscribeTopics
  }

  /**
   * This method sets the Map of Kafka parameters to reuse in multiple cases.
   *
   * @param options - The Map of Kafka parameters that includes ssl configs, credentials etc.
   * @return Map of Kafka parameters
   */
  def getKafkaProps(options: Map[String, String]): Map[String, String] = {
    Map[String, String](
      "bootstrap.servers" -> options("kafka.bootstrap.servers"),
      "group.id" -> options("group.id"),
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "security.protocol" -> options("kafka.security.protocol"),
      "sasl.jaas.config" -> options("kafka.sasl.jaas.config"),
      "sasl.mechanism" -> options("kafka.sasl.mechanism"),
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> "false",
      "failOnDataLoss" -> "false"
    )
  }


  /**
   * This method shutdown spark application gracefully if the stop flag is enabled.
   *
   * @param stopFlag        - this sets the stop flag method.
   * @param shutdownThread  - this sets the SparkShutdownHook method.
   * @param applicationLock - atomic boolean
   * @return
   */
  implicit class GracefullyStopStreamingApplication(val self: StreamingQuery) extends AnyVal {
    def awaitExternalTermination(shutdownHook: ShutdownHook): Unit = {
      while (self.isActive && !shutdownHook.shutdownThread.isAlive || self.exception.orNull != null) {
        if (self.exception.orNull != null) {
          logger.info(s"Caught exception - ${self.runId}")
          throw self.exception.get.cause
        }
        if (shutdownHook.stopFlagSet != null && shutdownHook.stopFlagSet.isFlagged() && !shutdownHook.shutdownThread.isAlive) {
          shutdownHook.applicationLock.set(false)
          shutdownHook.shutdownThread.start()
        }
        else {
          Thread.sleep(60000)
        }
      }
    }
  }

  /**
   * This method generates the Map of Kafka parameters that includes ssl configs, credentials etc.
   *
   * @param config    - either source config or sink config whichever having the kafka configs in app config file.
   * @param envConfig - env configuration that parsed from app config file.
   * @return Map of Kafka parameters
   */
  def getKafkaParams(config: Config, envConfig: Config): Map[String, String] = {
    val defaultStartingOffsets = "earliest"
    val defaultFailOnDataLoss = "false"
    val defaultMaxPartitionFetchBytes = "10485760"
    val defaultMaxOffsetsPerTrigger = "100000"
    val defaultIncludeHeaders = "false"

    val kafkaOptions = Map[String, String](
      "kafka.bootstrap.servers" -> envConfig.getString("kafka_configs.kafka_sasl_bootstrap_servers"),
      "startingOffsets" -> config.getStringOpt("startingOffsets").getOrElse(defaultStartingOffsets),
      "failOnDataLoss" -> config.getStringOpt("failOnDataLoss").getOrElse(defaultFailOnDataLoss),
      "kafka.max.partition.fetch.bytes" -> config.getStringOpt("maxPartitionFetchBytes").getOrElse(defaultMaxPartitionFetchBytes),
      "maxOffsetsPerTrigger" -> config.getStringOpt("max_offsets_per_trigger").getOrElse(defaultMaxOffsetsPerTrigger),
      "includeHeaders" -> config.getStringOpt("includeHeaders").getOrElse(defaultIncludeHeaders),
      "group.id" -> config.getString("group_id"),
      "groupIdPrefix" -> config.getString("groupId_prefix")
      //     "kafka.enable.auto.commit" -> "true"
    )
    var kafkaParams = Map.empty[String, String]
    val sslEnabled = config.getStringOpt("kafka_ssl_enabled").getOrElse("false").toBoolean
    if (sslEnabled) {
      logger.info("SSL enabled for Kafka Server - reading secrets from Vault")
      val kafkaCreds = envConfig.getConfig("kafka")
      val sasl_jaas_config: String = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";".format(
        kafkaCreds.getString("username"),
        kafkaCreds.getString("password")
      )
      val sasl_mechanism: String = config.getString("sasl_mechanism")
      val groupId_prefix: String = config.getString("groupId_prefix")
      val security_protocol: String = config.getString("kafka_security_protocol")


      kafkaParams = kafkaOptions ++ Map[String, String](
        "kafka.security.protocol" -> security_protocol,
        "kafka.sasl.jaas.config" -> sasl_jaas_config,
        "kafka.sasl.mechanism" -> sasl_mechanism,
        "groupIdPrefix" -> groupId_prefix

      ) ++ envConfig.getConfig("kafka_configs").entrySet().asScala.map(x => x.getKey -> x.getValue.unwrapped().toString).toMap
    } else {
      logger.info("ssl not enabled for kafka server")
      kafkaParams = kafkaOptions
    }

    // Append topics
    kafkaParams
  }

  /**
   * This method sets the s3 config if s3enabled in app config file.
   *
   * @param spark     - current active spark session
   * @param envConfig - env configuration that parsed from app config file.
   * @return Set of topics
   */
  def setS3ConfigIfRequired(spark: SparkSession, envConfig: Config): Unit = {
    val s3Enabled = envConfig.getStringOpt("s3_enabled").getOrElse("false").toBoolean
    if (s3Enabled) {
      val access_key: String = envConfig.getString("s3.access_key")
      val endpoint: String = envConfig.getString("s3.endpoint")
      val secret_key: String = envConfig.getString("s3.secret_key")

      val conf = spark.sparkContext.hadoopConfiguration

      conf.set("fs.s3a.access.key", access_key)
      conf.set("fs.s3a.secret.key", secret_key)
      conf.set("fs.s3a.endpoint", endpoint)
      conf.set("fs.s3a.path.style.access", "true")
      conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      conf.set("fs.s3a.multiobjectdelete.enable", "false")
      conf.set("fs.s3a.fast.upload", "true")
      conf.set("fs.s3a.connection.timeout", "200000")
      conf.set("fs.s3a.connection.ssl.enabled", "true")


      logger.info("S3 config set up completed")
    }
  }

  /**
   * This method sets the email body message.
   *
   * @param appName      - Application name.
   * @param topics       - topics.
   * @param sinkLocation - sinkLocation.
   * @param Error        - Error.
   * @return String og html message.
   */
  def emailBodyMessage(appName: String, topics: String, sinkLocation: String, Error: String): String = {
    s"""
       |<!DOCTYPE html>
       |<html>
       |<head>
       |  <style>
       |    body {
       |      font-family: 'Arial', sans-serif;
       |    }
       |    table {
       |      width: 80%;
       |      border-collapse: collapse;
       |      margin: 20px 0;
       |    }
       |    th, td {
       |      border: 2px solid black;
       |      padding: 12px;
       |      text-align: left;
       |      word-wrap: break-word;
       |    }
       |    th {
       |      background-color: #ffffff;
       |    }
       |  </style>
       |</head>
       |<body>
       |  <table>
       |  <tr>
       |      <th>JobName</th>
       |      <th>$appName</th>
       |    </tr>
       |    <tr>
       |      <td>Topic</th>
       |      <td>$topics</th>
       |    </tr>
       |    <tr>
       |      <td>Target</td>
       |      <td>$sinkLocation</td>
       |    </tr>
       |    <tr>
       |      <td>Error</td>
       |      <td>${Error}</td>
       |    </tr>
       |  </table>
       |</body>
       |</html>
       |""".stripMargin
  }

  /**
   * This method gets size of partitions for the given list of topics.
   *
   * @param config    - KakfaConfigs
   * @param envConfig - env configuration that parsed from app config file.
   * @param topics    - topics list.
   * @return topics partition map
   */
  def getPartitionSize(config: Config, envConfig: Config, topics: List[String]): Map[String, Int] = {
    val options = getKafkaParams(config, envConfig)
    val kafkaParams = getKafkaProps(options)
    val consumer = new KafkaConsumer[String, String](kafkaParams)
    val topicsPartitionMap = topics.map { topic =>
      val partitions = consumer.partitionsFor(topic)
      val partitionCount = if (partitions != null) partitions.size() else 0
      (topic, partitionCount)
    }.toMap

    consumer.close()
    topicsPartitionMap

  }

  def getReadOrWriteStreamOptions(config: Config, optionType: String): Option[Map[String, String]] = {
    if (config.hasPath(optionType)) {
      val optionsConfig = config.getConfig(optionType)
      Some(optionsConfig.entrySet().asScala.map { entry =>
        entry.getKey -> optionsConfig.getString(entry.getKey)
      }.toMap)
    } else {
      None
    }
  }

  /**
   * This method initialize the starting offset to config file if new topic added to the existing list of topics.
   *
   * @param args -cli arguments
   */
  def configureStartingOffset(sourceConfig: Config, sinkConfig: Config, envConfig: Config): Unit = {

    val spark: SparkSession = SparkSession.getActiveSession.get
    val conf = spark.sparkContext.hadoopConfiguration
    val autoOffsetToggle = sinkConfig.getString("auto_offset_toggle")
    val checkpointPath = sinkConfig.getString("checkpoint_path")

    /**
     * Checking if new topic added and manages the offset metadata to pick the new topic
     */
    if (autoOffsetToggle == "true" && isS3DirectoryNotEmpty(s"$checkpointPath/offsets", conf) && ifFileExists(s"$checkpointPath/commits", conf)) {
      val offsetMetadata = extractOffsetMetadata(sourceConfig, envConfig, checkpointPath, conf)
      val newTopicsList = offsetMetadata("newTopicsList").split(",").map(_.trim).filter((_.nonEmpty)).toList
      logger.info(s"New topics added list: $newTopicsList")
      val latestCheckpointOffset = offsetMetadata("latestCheckpointOffset")
      if (newTopicsList.nonEmpty && newTopicsList != null) {
        val startingOffset = getNewStartingOffsets(sourceConfig, envConfig, newTopicsList, latestCheckpointOffset)
        val format: SimpleDateFormat = new SimpleDateFormat(s"yyyyMMddHHmmss")
        val checkpointDateVersion = format.format(Calendar.getInstance().getTime)

        val checkpointArchivePath = s"${checkpointPath}_archive/checkpoint=$checkpointDateVersion"

        logger.info(s"Copy the checkpoint data to archive location $checkpointArchivePath")
        moveFiles(checkpointPath, checkpointArchivePath)

        logger.info(s"Delete the checkpoint directory $checkpointPath")
        deleteFiles(s"$checkpointPath/", conf)
        logger.info(s"Cleaning up the checkpoint directory $checkpointArchivePath")
        cleanUpLogs(s"${checkpointPath}_archive", conf, 30)
        logger.info(s"The derived value of starting offset is $startingOffset")
        startingOffset
      }
      else {
        logger.info("The New topic is not added, and continuing the process")

      }
    } else {
      logger.info("Checkpoint directory does not exist, processing as initial run")
    }

  }


  /**
   * This method initialize the shutdown process of application whenever the stopFlag file exists.
   * @param appControlConfig - appControlConfig
   */
  def initializeShutdownHook(appControlConfig: Config): ShutdownHook = {
    val spark: SparkSession = SparkSession.getActiveSession.get

    val stopFlag = appControlConfig.getString("stop_flagPath")
    val jobTypePath = appControlConfig.getString("stop_jobType")
    val streamingJob = appControlConfig.getString("stop_applicationName")

    val stopFlagSet = StopFlag(stopFlag, jobTypePath, streamingJob, spark.sparkContext.hadoopConfiguration)
    val applicationLock = new AtomicBoolean(true)
    val shutdownLatch = new CountDownLatch(1)
    val shutdownThread = new SparkShutdownHook(spark, () => applicationLock.get(), shutdownLatch)
    ShutdownHookManager.get().addShutdownHook(
      shutdownThread,
      1000,
      spark.sparkContext.hadoopConfiguration.getTimeDuration(
        CommonConfigurationKeysPublic.SERVICE_SHUTDOWN_TIMEOUT,
        CommonConfigurationKeysPublic.SERVICE_SHUTDOWN_TIMEOUT_DEFAULT,
        ShutdownHookManager.TIME_UNIT_DEFAULT
      ),
      ShutdownHookManager.TIME_UNIT_DEFAULT
    )

    val shutDownHook = ShutdownHook(stopFlagSet, shutdownThread, applicationLock, shutdownLatch)

    shutDownHook

  }

  case class ShutdownHook(
                           stopFlagSet: StopFlag,
                           shutdownThread: SparkShutdownHook,
                           applicationLock: AtomicBoolean,
                           shutdownLatch: CountDownLatch
                         )

  case class EmailNotification(
                                toAddress: String,
                                environment: String,
                                pipeline_name: String,
                                app_name: String,
                                sinkLocation: String,
                                emailHostName: String,
                                fromAddress: String,
                                topics: String
                              )

  /**
   * This method infers the json schema by reading the batch dataframe and returns the dataframe with schema.
   * @param batchDF - batch dataframw
   */
  def inferJsonSchema(batchDF: DataFrame): DataFrame = {
    val spark: SparkSession = SparkSession.getActiveSession.get

    val batchSchema = batchDF.schema
    val metadataFields = batchSchema.fields.filter(_.name != "value")
    val combinedRDD = batchDF.rdd.map { row =>
      val metadataValues = metadataFields.map { field =>
        row.getAs[Any](field.name)
      }
      val jsonValueString = row.getAs[String]("value")
      Row.fromSeq(metadataValues :+ jsonValueString)
    }
    val jsonDF = spark.read.json(combinedRDD.map(row => row.getString(metadataFields.length)))
    val inferredSchema = jsonDF.schema
    val combinedSchema = StructType(
      metadataFields ++ inferredSchema.fields
    )
    val finalRDD = combinedRDD.zip(jsonDF.rdd).map { case (regularRow, jsonRow) =>
      Row.fromSeq(regularRow.toSeq.take(metadataFields.length) ++ jsonRow.toSeq)
    }

    val combinedDF = spark.createDataFrame(finalRDD, combinedSchema)
    combinedDF
  }

  /**
   * This method converts the json schema string to struct type format.
   * @param jsonSchemaString - json schema in string
   */
  def getSchemaFromJson(jsonSchemaString: String): StructType = {
    try {
      DataType.fromJson(jsonSchemaString).asInstanceOf[StructType]
    } catch {
      case e: Throwable => {
        logger.error("Unable to get schema", e)
        throw new RuntimeException(s"Unable to get schema: $jsonSchemaString")
      }
    }
  }
}
