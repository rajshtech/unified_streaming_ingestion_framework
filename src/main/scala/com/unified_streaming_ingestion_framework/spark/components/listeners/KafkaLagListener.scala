package com.unified_streaming_ingestion_framework.spark.components.listeners

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.gson.Gson
import com.unified_streaming_ingestion_framework.spark.components.utils.CommonUtils.{EmailNotification, ShutdownHook, emailBodyMessage}
import com.unified_streaming_ingestion_framework.spark.components.spark_utils.EmailUtils
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.slf4j.{Logger, LoggerFactory}

import java.util
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.mutable
import scala.util.control.NonFatal

class KafkaLagListener(
                        appName: String,
                        servers: String,
                        shutdownHook: ShutdownHook,
                        kafkaParams: Map[String, String],
                        emailNotification: EmailNotification,
                        sinkConfig: Config) extends StreamingQueryListener {
  private[this] lazy val _defaultMapper = {
    val m = new ObjectMapper()
    m.registerModule(new DefaultScalaModule)
    m.enable(DeserializationFeature.USE_LONG_FOR_INTS)
    m
  }
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val EARLIEST = "EARLIEST"
  private val LATEST = "LATEST"

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    try {
      val triggerOnce = sinkConfig.getString("trigger_once").toBoolean
      if (shutdownHook.stopFlagSet.isFlagged() && !shutdownHook.shutdownThread.isAlive) {
        shutdownHook.shutdownThread.start()
      }
      val startingOffset = event.progress.sources(0).startOffset

      if (startingOffset != null) {
        logger.info(s"The starting offset is not null and value is: $startingOffset")
        val parseStartingOffset = parseProgress(startingOffset)
        val convertedStartingOffsets = convertStructureOffsetToTopicOffset(parseStartingOffset)
        val endOffset = event.progress.sources(0).endOffset
        val parsedEndOffset = convertStructureOffsetToTopicOffset(parseProgress(endOffset))
        val logOffset = parseOffsetRanges(convertedStartingOffsets, parsedEndOffset)
        commitSyncTopicOffset(mapAsJavaMap(kafkaParams), StreamingTopicOffset(logOffset.values.toArray))
      }
      else {
        logger.info(s"The starting offset is $startingOffset")

      }
      if ((shutdownHook.shutdownLatch.getCount == 0 || triggerOnce) && shutdownHook.applicationLock.get()) {
        shutdownHook.applicationLock.set(false)
        logger.info("Released the lock")
      }
    }
    catch {
      case e: Throwable =>
        shutdownHook.applicationLock.set(false)
        logger.error(s"Error ${event.progress.runId}: ${e.getMessage}")
        e.printStackTrace()
        EmailUtils.sendHtmlErrorMail(
          emailNotification.toAddress,
          s"${emailNotification.environment.toUpperCase} - Streaming Framework: ${emailNotification.pipeline_name} " +
            s"failed for application ${emailNotification.app_name}",
          emailBodyMessage(
            emailNotification.app_name,
            emailNotification.topics,
            emailNotification.sinkLocation,
            e.toString + "<br>" + e.getStackTrace.take(20).mkString("<br>")),
          emailNotification.emailHostName,
          emailNotification.fromAddress
        )
        if (!shutdownHook.shutdownThread.isAlive)
          shutdownHook.shutdownThread.start()
    }
  }

  def convertStructureOffsetToTopicOffset(offsetStruct: Map[String, Map[String, Long]]): Map[TopicPartition, Long] = {
    try {
      val formattedOffsets = offsetStruct.flatMap {
        case (topic, partOffsets) =>
          partOffsets.map {
            case (part, offset) =>
              //logger.info(s"parsed offset: ${offsetStruct.toString()}")
              new TopicPartition(topic, part.toInt) -> offset
          }
      }
      //logger.info(s"formatted offsets: ${formattedOffsets.toString}")
      formattedOffsets
    } catch {
      case NonFatal(_) =>
        throw new IllegalArgumentException(offsetStruct.toString())
    }
  }

  def commitSyncTopicOffset(param: java.util.Map[String, Object], offset: StreamingTopicOffset): Unit = {
    try {
      val consumer = new KafkaConsumer[String, String](param)
      val offsetMap = new util.HashMap[TopicPartition, OffsetAndMetadata]()
      offset.offsetRanges.foreach(x => offsetMap.put(new TopicPartition(x.topic, x.partition), new OffsetAndMetadata(x.untilOffset)))
      consumer.commitSync(offsetMap)
      logger.info("Offset is synchronized to Kafka")
    }
    catch {
      case e: Throwable => logger.warn(s"Unable to sync offset with Kafka: ${e.getMessage}")
    }
  }

  def parseOffsetRanges(startingOffset: scala.collection.Map[TopicPartition, Long], endingOffset: scala.collection.Map[TopicPartition, Long]): mutable.HashMap[String, OffsetRange] = {
    val offsetRangeMap: mutable.HashMap[String, OffsetRange] = mutable.HashMap[String, OffsetRange]()
    endingOffset.foreach(x => {
      val topicPartition = x._1.asInstanceOf[TopicPartition]
      val topic = topicPartition.topic()
      val partition = topicPartition.partition()
      val untilOffset = x._2
      val fromOffset = startingOffset.getOrElse(topicPartition, untilOffset)
      val updateOffset = OffsetRange(topic, partition, fromOffset, untilOffset)
      offsetRangeMap.put(s"${topic}-${partition}", updateOffset)
    })
    logger.info(s"offsetRangeMap is ${offsetRangeMap.toString()}")
    offsetRangeMap

  }

  private def parseProgress(progressString: String): Map[String, Map[String, Long]] = {
    val parsedOffsets = _defaultMapper.readValue(progressString, classOf[Map[String, Map[String, Long]]])
    parsedOffsets
  }

  /*  *
     * Get kafka (earliest, latest) offsets for each topic, as a map.
     * @param topicNames
     * @return*/
  def getKafkaOffsets(
                       topicNames: List[String]): Map[String, Map[String, List[PartitionOffset]]] = {
    val consumer = new KafkaConsumer[String, String](kafkaParams)

    val offsetMap = topicNames.map({ topicName =>
      logger.info(s" getKafkaOffsets - TopicName : ${topicName}")
      val latest = KafkaUtil.getLatestOffsets(consumer, topicName)
      logger.info(s"latest is ${latest.toString()}")
      val earliest = KafkaUtil.getEarliestKafkaOffsets(consumer, topicName)
      logger.info(s"earliest is ${earliest.toString()}")
      (topicName -> Map(EARLIEST -> earliest, LATEST -> latest))
    }).toMap

    consumer.close()
    offsetMap
  }

  // The below method is to compute lag & print to logs, keeping for reference
  def computeTopicLag(
                       progressOffsets: Map[String, Long],
                       kafkaEarliestOffsets: List[PartitionOffset],
                       kafkaLatestOffsets: List[PartitionOffset]): Unit = {

    val offsetsToGo = getOffsetDiff(progressOffsets, kafkaLatestOffsets)
    logger.info(s"offsetsToGo is ${offsetsToGo.toString()}")
    val offsetsProcessed = -1 * getOffsetDiff(progressOffsets, kafkaEarliestOffsets).toDouble
    logger.info(s"offsetsProcessed is ${offsetsProcessed.toString()}")
    val percentLag = 100 * (offsetsToGo.toDouble / (offsetsProcessed + offsetsToGo.toDouble))
    logger.info(s"percentLag is ${percentLag.toString()}")
    //source.updateTotalLag(offsetsToGo)
    //source.updatePercentLag(percentLag)

  }

  private def getOffsetDiff(
                             progressOffsets: Map[String, Long],
                             kafkaOffsets: List[PartitionOffset]): Long = {
    val kafkaOffsetsMap: Map[String, Long] = kafkaOffsets
      .map(partition => (partition.topicPartition.partition.toString -> partition.offset))
      .toMap

    assert(
      kafkaOffsetsMap.keySet == progressOffsets.keySet,
      s"kafkaOffset keys: ${kafkaOffsetsMap.keySet} do not match checkpointOffsets ${progressOffsets.keySet}")

    kafkaOffsetsMap
      .keySet
      .map(partition => kafkaOffsetsMap(partition) - progressOffsets(partition))
      .sum
  }

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    logger.info("Query terminated")
    val triggerOnce = sinkConfig.getString("trigger_once").toBoolean
    if ((shutdownHook.shutdownLatch.getCount == 0 || triggerOnce) && shutdownHook.applicationLock.get()) {
      shutdownHook.applicationLock.set(false)
      logger.info("Released the lock")
    }
  }

  case class OffsetRange(topic: String, partition: Int, fromOffset: Long, untilOffset: Long) {
    override def toString: String = {
      s"OffsetRange(topic: '$topic', partition: $partition, range: [$fromOffset -> $untilOffset])"
    }
  }

  case class StreamingTopicOffset(offsetRanges: Array[OffsetRange]) {
    override def toString(): String = {
      new Gson().toJson(this, classOf[StreamingTopicOffset])
    }

    def parseFromStartingOffset(): mutable.HashMap[TopicPartition, Long] = {
      val fromOffsets = mutable.HashMap[TopicPartition, Long]()
      if (offsetRanges != null)
        this.offsetRanges.foreach(x => fromOffsets.put(new TopicPartition(x.topic, x.partition), x.fromOffset))
      fromOffsets
    }

    /**
     *
     * @return
     */
    def parseFromEndingOffset(): mutable.HashMap[TopicPartition, Long] = {
      val fromOffsets = mutable.HashMap[TopicPartition, Long]()
      if (offsetRanges != null)
        this.offsetRanges.foreach(x => fromOffsets.put(new TopicPartition(x.topic, x.partition), x.untilOffset))
      fromOffsets
    }
  }
}