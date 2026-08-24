package com.unified_streaming_ingestion_framework.spark.components.sinks

import com.unified_streaming_ingestion_framework.spark.components.utils.CommonUtils.{getReadOrWriteStreamOptions, inferJsonSchema}
import com.typesafe.config.Config
import com.unified_streaming_ingestion_framework.spark.components.ConfigUtil
import com.unified_streaming_ingestion_framework.spark.components.spark_utils.SparkIOUtils.isPathExists
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.concurrent.duration.SECONDS

/**
 * The below case class is to define the config parameters for Delta lake sink.
 *
 * @param targetPath: delta table path of s3.
 * @param checkpointPath: checkpoint path of s3 for the above delta table path.
 * @param format: The format of target data, default is delta as this is delta lake config.
 * @param triggerSeconds: Frequency of micro-batch, specifies that a new micro-batch should be triggered every x seconds.
 * @param triggerOnce: True or False -  processes all available data in a single trigger and then stops if true else continuous.
 *                      Since Spark 3.4 Trigger once is deprecated - need to migrate this.
 * @param partitionBy: partition columns that need to be partitioned the target data.
 * @param writeOperationType: The delta lake write operation type.
 * @param mergeMatchFields: Fields that are to be provided for merge operation of upsert operation (must provided).
 * @param writeStreamOptions: config of options to Spark while writing.
 * @param kafkaPayloadFormat: Data format of incoming data to write process from Kafka topic, JSON, AVRO(default).
 * @param jsonSchemaInferenceType: For JSON payload, the schema inference type option (here it is automatic pull of schema).
 */
case class DeltalakeConfig(
                            targetPath: String,
                            checkpointPath: String,
                            format: String,
                            triggerSeconds: Long,
                            triggerOnce: String = "false",
                            partitionBy: Option[List[String]],
                            writeOperationType: String,
                            mergeMatchFields: String,
                            writeStreamOptions: Map[String, String],
                            kafkaPayloadFormat: Option[String],
                            jsonSchemaInferenceType: Option[String]
                          )


/**
 * Deltalake Stream sink performs the sink process in insert and upsert operations of delta lake.
 * @param config: config of delta lake config.
 * @param spark: active spark session.
 */

class DeltalakeStreamSink(config: DeltalakeConfig, spark: SparkSession) extends StreamingSink {

  val log: Logger = LoggerFactory.getLogger(this.getClass)

  val deltalakeConfig: DeltalakeConfig = config
  val sourceFormat: String = deltalakeConfig.format
  val targetPath: String = deltalakeConfig.targetPath
  val checkpointPath: String = deltalakeConfig.checkpointPath
  val triggerSeconds: Long = deltalakeConfig.triggerSeconds
  val triggerOnce: String = deltalakeConfig.triggerOnce
  val writeStreamOptions: Map[String, String] = deltalakeConfig.writeStreamOptions
  val writeOperationType: String = deltalakeConfig.writeOperationType
  val mergeMatchFields: String = deltalakeConfig.mergeMatchFields
  val trigger: Trigger = triggerOnce match {
    case "true" => Trigger.AvailableNow()
    case "false" => Trigger.ProcessingTime(triggerSeconds, SECONDS)
  }
  val kafkaPayloadFormat: String = deltalakeConfig.kafkaPayloadFormat.getOrElse("avro")
  val jsonSchemaInferenceType: String = deltalakeConfig.jsonSchemaInferenceType.getOrElse("infer_as_value")


  /**
   * Override write to handle Deltalake specific options
   *
   * @param dataFrame dataFrame to write
   * @return StreamingQuery
   */

  override def write(dataFrame: DataFrame): StreamingQuery = {

    writeOperationType match {
      case "insert" => insert(dataFrame)
      case "upsert" => upsert(dataFrame)
    }

  }

  /**
   * This method performs the insert operation for the given dataframe.
   * @param dataFrame: source dataframe.
   * @return
   */

  def insert(dataFrame: DataFrame): StreamingQuery = {
    if (kafkaPayloadFormat.toLowerCase == "json" && jsonSchemaInferenceType.toLowerCase == "infer_schema_dynamically") {
      log.info("Inferring the JSON schema automatically")
      if (deltalakeConfig.partitionBy.isDefined) {
        val partitionCols = deltalakeConfig.partitionBy.get
        log.info(s"partition Columns provided are $partitionCols")
        log.info(s"The targetPath provided is $targetPath")
        log.info(s"The checkpointPath provided is $checkpointPath")

        dataFrame
          .writeStream
          .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
            val inferredJsonSchemaDF = inferJsonSchema(batchDF)
            inferredJsonSchemaDF
              .write
              .format(sourceFormat)
              .mode("append")
              .partitionBy(partitionCols: _*)
              .options(writeStreamOptions)
              .save(targetPath)
          }
          .option("checkpointLocation", checkpointPath)
          .option("mergeSchema", "true")
          .trigger(trigger)
          .start()
      } else {
        dataFrame
          .writeStream
          .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
            val inferredJsonSchemaDF = inferJsonSchema(batchDF)
            inferredJsonSchemaDF
              .write
              .format(sourceFormat)
              .mode("append")
              .options(writeStreamOptions)
              .save(targetPath)
          }
          .option("checkpointLocation", checkpointPath)
          .option("mergeSchema", "true")
          .trigger(trigger)
          .start()
      }
    }
    else {
      if (deltalakeConfig.partitionBy.isDefined) {
        val partitionCols = deltalakeConfig.partitionBy.get
        log.info(s"partition Columns provided are $partitionCols")
        log.info(s"The targetPath provided is $targetPath")
        log.info(s"The checkpointPath provided is $checkpointPath")
        dataFrame
          .writeStream
          .format(sourceFormat)
          .option("checkpointLocation", checkpointPath)
          .trigger(trigger)
          .partitionBy(partitionCols: _*)
          .outputMode("append")
          .option("path", targetPath)
          .option("mergeSchema", "true")
          .options(writeStreamOptions)
          .start()
      } else {
        dataFrame
          .writeStream
          .format(sourceFormat)
          .option("checkpointLocation", checkpointPath)
          .trigger(trigger)
          .outputMode("append")
          .option("path", targetPath)
          .option("mergeSchema", "true")
          .options(writeStreamOptions)
          .start()
      }
    }
  }

  /**
   * This method performs the upsert operation for the given dataframe.
   * @param df: source dataframe.
   * @return
   */

  def upsert(df: DataFrame): StreamingQuery = {

    import io.delta.tables._

    if ((writeOperationType == "upsert") && !isPathExists(targetPath, spark)) {
      if (deltalakeConfig.partitionBy.isDefined) {
        val partitionCols = deltalakeConfig.partitionBy.get
        log.info(s"partition Columns provided are $partitionCols")
        log.info(s"The targetPath provided is $targetPath")
        log.info(s"The checkpointPath provided is $checkpointPath")
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], df.schema)
          .write
          .format("delta")
          .partitionBy(partitionCols: _*)
          .mode("overwrite")
          .save(targetPath)
      } else {
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], df.schema)
          .write.format("delta")
          .mode("overwrite")
          .save(targetPath)
      }
    }

    val deltaTable = DeltaTable.forPath(spark, targetPath)
    val onCondition = mergeMatchFields.split(",").map(rec => {
      s"s.$rec = t.$rec"
    }
    ).mkString(" and ")
    log.info(s"onCondition delta lake merge is $onCondition")

    // Function to upsert microBatchOutputDF into Delta table using merge
    def upsertToDelta(df: DataFrame, batchId: Long): Unit = {

      val dataFrame: DataFrame = df.dropDuplicates(mergeMatchFields.split(","))

      deltaTable.as("t")
        .merge(
          dataFrame.as("s"),
          onCondition)
        .whenMatched().updateAll()
        .whenNotMatched().insertAll()
        .execute()
    }

    df.writeStream
      .format("delta")
      .foreachBatch(upsertToDelta _)
      .outputMode("update").trigger(trigger)
      .option("checkpointLocation", checkpointPath)
      .options(writeStreamOptions)
      .option("mergeSchema", "true")
      .start()
  }

  override def getSinkLocation: String = deltalakeConfig.targetPath

}

object DeltalakeStreamSink {

  def apply(sourceConfig: Config, sinkConfig: Config, spark: SparkSession): DeltalakeStreamSink = {

    val kafkaPayloadFormat = sourceConfig.getStringOpt("kafka_payload_format")
    val jsonSchemaInferenceType = sourceConfig.getStringOpt("json_schema_inference_type")
    val deltalakePath = sinkConfig.getString("target_path")
    val checkPointPath = sinkConfig.getString("checkpoint_path")
    val triggerSeconds = sinkConfig.getString("trigger_seconds").toLong
    var writeOperationType = sinkConfig.getString("write_operation_type")
    val triggerOnce = sinkConfig.getString("trigger_once")
    val format = "delta"

    val writeStreamOptions: Map[String, String] = getReadOrWriteStreamOptions(
      sinkConfig, "write_options"
    ).getOrElse(
      Map.empty[String, String]
    )

    val mergeMatchFields: String = if (sinkConfig.hasPath("mergeMatchFields")) {
      sinkConfig.getStringList("mergeMatchFields").asScala.mkString(",")
    } else {
      null
    }

    val partitionConfig = sinkConfig.getStringOpt("partitionBy")

    var partitionSpec: Option[List[String]] = None

    if (partitionConfig.isDefined) {
      val partitionCols = partitionConfig.get.split(";")

      partitionSpec = Option(partitionCols.toList.map { spec => spec.trim})
    }

    new DeltalakeStreamSink(
      DeltalakeConfig(
        deltalakePath,
        checkPointPath,
        format,
        triggerSeconds,
        triggerOnce,
        partitionSpec,
        writeOperationType,
        mergeMatchFields,
        writeStreamOptions,
        kafkaPayloadFormat,
        jsonSchemaInferenceType
      ), spark
    )

  }
}
