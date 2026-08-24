package com.unified_streaming_ingestion_framework.spark.components.listeners

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

case class PartitionOffset(topicPartition: TopicPartition, offset: Long)

object KafkaUtil {

  def getLatestOffsets(consumer: KafkaConsumer[String, String], topic: String): List[PartitionOffset] = {
    val partitions = consumer.partitionsFor(topic).asScala.map(p => new TopicPartition(topic, p.partition())).toList
    consumer.endOffsets(partitions.asJava).asScala.map {
      case (tp, offset) => PartitionOffset(tp, offset.toLong)
    }.toList
  }

  def getEarliestKafkaOffsets(consumer: KafkaConsumer[String, String], topic: String): List[PartitionOffset] = {
    val partitions = consumer.partitionsFor(topic).asScala.map(p => new TopicPartition(topic, p.partition())).toList
    consumer.beginningOffsets(partitions.asJava).asScala.map {
      case (tp, offset) => PartitionOffset(tp, offset.toLong)
    }.toList
  }
}
