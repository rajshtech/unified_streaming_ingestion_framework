package com.unified_streaming_ingestion_framework.spark.components.shutdown_manager

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.CountDownLatch

class SparkShutdownHook(spark: SparkSession, lock:() => Boolean, stopLatch: CountDownLatch) extends Thread {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  override def run(): Unit = {
    try {
      stopLatch.countDown()
      logger.debug("Shutdown hook was triggered")
      if (spark != null) {
        //      Waiting for application to finish
        while(lock()){}
        logger.debug("Shutting down the job...")
        val activeStreams = spark.streams.active
        if(activeStreams != null && activeStreams.length > 0) {
          activeStreams.foreach(stream => {
            stream.stop()
            stream.awaitTermination()
            logger.warn(s"${spark.sparkContext.appName} - ${stream.runId} was stopped")
          })
        }
        spark.stop()
        logger.warn(s"${spark.sparkContext.appName} was stopped")
      }
    }
    catch {
      case e: IllegalStateException if e.getMessage.contains("Cannot call methods on a stopped SparkContext") =>
        logger.warn("Spark was already stopped, no methods can be called on it.")
      case e: Exception =>
        logger.warn(
          s"Spark was already stopped: ${e.getMessage}")
    }
  }
}



