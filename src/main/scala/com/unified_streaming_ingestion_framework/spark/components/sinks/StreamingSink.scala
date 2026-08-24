package com.unified_streaming_ingestion_framework.spark.components.sinks

import com.typesafe.config.Config
import com.unified_streaming_ingestion_framework.spark.components.transformers.StreamingTransform
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Streaming Sink manages the type of sink to choose from config and calls the respective sink.
 * And this enforce the sink standard methods to call.
 */
abstract class StreamingSink extends TriggerType {
  def sourceFormat: String

  def targetPath: String

  def checkpointPath: String

  def triggerSeconds: Long

  def writeStreamOptions: Map[String, String]

  def write(dataFrame: DataFrame): StreamingQuery = {
    dataFrame
      .writeStream
      .format(sourceFormat)
      .options(writeStreamOptions)
      .option("checkpointLocation", checkpointPath)
      .trigger(getTriggerType(triggerSeconds))
      .start(targetPath)
  }

  def write(dataFrame: DataFrame, transformer: StreamingTransform): StreamingQuery = {
    dataFrame
      .writeStream
      .foreachBatch(batchTransform(transformer))
      .option("checkpointLocation", checkpointPath)
      .trigger(getTriggerType(triggerSeconds))
      .start()
  }

  def batchTransform(transformer: StreamingTransform): (DataFrame, Long) => Unit = {
    (batchDF: DataFrame, _: Long) => {
      //batchDF.persist()
      val xdf = transformer.transform(batchDF)
      batchWrite(xdf)
      //batchDF.unpersist()
    }
  }

  def batchWrite(dataFrame: DataFrame): Unit = {
    dataFrame
      .write
      .format(sourceFormat)
      .mode(saveMode)
      .options(writeStreamOptions)
      .save(targetPath)
  }

  def saveMode: SaveMode = SaveMode.Append

  def getSinkLocation: String
}

object StreamingSinkBuilder {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def createSink(envConfig: Config, sinkConfig: Config, sourceConfig: Config): StreamingSink = {

    val sinkType = sinkConfig.getString("sink_type")
    val spark: SparkSession = SparkSession.getActiveSession.get

    logger.info(s"Sink type: $sinkType")
    sinkType.toLowerCase match {
      case "delta" => DeltalakeStreamSink(sourceConfig, sinkConfig, spark)
      case "s3" | "kafka" | "mssql" | "mysql" | "postgresql" =>
        throw new UnsupportedOperationException(s"Sink type '$sinkType' is not implemented yet")
      case _ => throw new IllegalArgumentException(s"Unknown source type: $sinkType")
    }
  }

}



