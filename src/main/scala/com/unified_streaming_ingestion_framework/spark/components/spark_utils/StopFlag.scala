package com.unified_streaming_ingestion_framework.spark.components.spark_utils

import com.unified_streaming_ingestion_framework.spark.components.Constants
import com.unified_streaming_ingestion_framework.spark.components.spark_utils.SparkIOUtils.{applicationSeparator, ifFileExists}
import org.apache.hadoop.conf.Configuration
import org.slf4j.{Logger, LoggerFactory}

/***
 * Won't allow job to run if there's stop flag
 *
 * @param flagPath
 * @param jobType
 * @param applicationName
 */
case class StopFlag(
                     flagPath:String,
                     jobType:String,
                     applicationName:String,
                     hadoopConfiguration: Configuration
                   ) {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def isFlagged(): Boolean = {
    val globalStopFlag = Array(flagPath,Constants.STOP_FLAG).mkString(applicationSeparator(flagPath))
    val jobTypeStopFlag = Array(flagPath,jobType,Constants.STOP_FLAG).mkString(applicationSeparator(flagPath))
    val applicationStopFlag = Array(flagPath,applicationName,Constants.STOP_FLAG).mkString(applicationSeparator(flagPath))
    ifFileExists(globalStopFlag, hadoopConfiguration) == true || ifFileExists(jobTypeStopFlag, hadoopConfiguration) == true || ifFileExists(applicationStopFlag, hadoopConfiguration) == true
  }

}
