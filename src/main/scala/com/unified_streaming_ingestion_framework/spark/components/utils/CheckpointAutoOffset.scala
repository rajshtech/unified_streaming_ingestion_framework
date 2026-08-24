package com.unified_streaming_ingestion_framework.spark.components.utils

import com.typesafe.config.Config
import com.unified_streaming_ingestion_framework.spark.components.spark_utils.SparkIOUtils.getPathFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.json4s.JsonAST.{JField, JInt, JObject}
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

/**
 * Detects topics that were added to the pipeline config after a checkpoint was first created,
 * by comparing the topics in the latest committed Kafka source offsets against the configured topic list.
 */
object CheckpointAutoOffset {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Reads the latest committed Kafka source offsets from the streaming checkpoint's `offsets`
   * directory and returns them alongside the list of configured topics missing from them.
   */
  def extractOffsetMetadata(
                              sourceConfig: Config,
                              envConfig: Config,
                              checkpointPath: String,
                              hadoopConf: Configuration
                            ): Map[String, String] = {
    val offsetsDir = s"$checkpointPath/offsets"
    val fs = getPathFileSystem(offsetsDir, hadoopConf)

    val latestBatchFile = fs.listStatus(new Path(offsetsDir))
      .filter(status => status.isFile && status.getPath.getName.forall(_.isDigit))
      .map(_.getPath)
      .maxBy(_.getName.toLong)

    val stream = fs.open(latestBatchFile)
    val offsetsJson = try {
      // Spark writes each offset log entry as: line0=version, line1=metadata json, line2=offsets json
      Source.fromInputStream(stream).getLines().toList.last
    } finally {
      stream.close()
    }

    val committedTopics = parse(offsetsJson) match {
      case JObject(fields) => fields.map(_._1).toSet
      case _ => Set.empty[String]
    }

    val configuredTopics = CommonUtils.getTopics(sourceConfig, envConfig).toSet
    val newTopicsList = (configuredTopics -- committedTopics).mkString(",")

    Map(
      "newTopicsList" -> newTopicsList,
      "latestCheckpointOffset" -> offsetsJson
    )
  }

  /**
   * Builds a Spark Kafka `startingOffsets` JSON string that keeps the already-committed offsets
   * as-is and adds an `earliest` (-2) starting position for every partition of the new topics.
   */
  def getNewStartingOffsets(
                              sourceConfig: Config,
                              envConfig: Config,
                              newTopicsList: List[String],
                              latestCheckpointOffset: String
                            ): String = {
    val existingFields = parse(latestCheckpointOffset) match {
      case JObject(fields) => fields
      case _ => List.empty[JField]
    }

    val partitionCounts = CommonUtils.getPartitionSize(sourceConfig, envConfig, newTopicsList)

    val newTopicFields: List[JField] = newTopicsList.map { topic =>
      val partitionCount = partitionCounts.getOrElse(topic, 0)
      val partitionOffsets = (0 until partitionCount).map(p => JField(p.toString, JInt(-2))).toList
      JField(topic, JObject(partitionOffsets))
    }

    logger.info(s"Adding starting offsets for new topics: ${newTopicsList.mkString(",")}")
    compact(render(JObject(existingFields ++ newTopicFields)))
  }
}
