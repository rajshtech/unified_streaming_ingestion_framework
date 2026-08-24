
package com.unified_streaming_ingestion_framework.spark.components.spark_utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


object SparkIOUtils {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private def logInfo(msg: String): Unit = logger.info(msg)

  private def logDebug(msg: String): Unit = logger.debug(msg)

  private def logWarning(msg: String): Unit = logger.warn(msg)

  private def logInfo(msg: String, cause: Throwable): Unit = logger.info(msg, cause)

  def getPathFileSystem(path: String, hadoopConfiguration: Configuration = null): FileSystem = {
    var oPath = path
    //for window local machine
    if (!oPath.startsWith("hdfs") && !oPath.startsWith("s3")) {
      oPath = s"file://${path.replace("\\", "/")}"
    }
    val filePath = new org.apache.hadoop.fs.Path(oPath)
    if (hadoopConfiguration == null)
      filePath.getFileSystem(new Configuration())
    else
      filePath.getFileSystem(hadoopConfiguration)
  }

  /**
   * Check if path is available or not, if flag checkSuccess, check if _SUCCESS is there.
   * If not, clean up path so that path can be used later
   *
   * @param path
   * @param checkSuccess
   * @return
   */
  def ifFileExists(path: String, hadoopConfiguration: Configuration = null, checkSuccess: Boolean = false): Boolean = {
    var checkPath = path
    if (checkSuccess)
      checkPath = s"$path${applicationSeparator(path)}_SUCCESS"
    val fs = getPathFileSystem(path, hadoopConfiguration)
    if (fs.exists(new Path(checkPath))) {
      logDebug(s"$path is exists")
      true
    }
    else {
      logDebug(s"$path is not exists")
      //cleanup Path for dataFrame
      if (checkSuccess)
        deleteFiles(path, hadoopConfiguration)
      false
    }
  }

  /**
   * Fuction to delete all data in a provided path
   *
   * @param path
   */
  def deleteFiles(path: String, hadoopConfiguration: Configuration = null): Unit = {
    val fs = getPathFileSystem(path, hadoopConfiguration)
    val outPutPath = new Path(path)
    if (fs.exists(outPutPath)) {
      try {
        fs.delete(outPutPath, true)
        logDebug(s"$path is successfully clean")
      }
      catch {
        case e: Throwable => logInfo(s"$path can't be deleted. Please check.", e.getCause)
      }
    }
  }

  def applicationSeparator(path: String = null): String = {
    if (!path.startsWith("hdfs") && !path.startsWith("s3")) {
      File.separator
    }
    else
      "/"
  }

  //  Clean up all logs older than n days
  def cleanUpLogs(path: String, hadoopConfiguration: Configuration = null, olderThanDay: Int = 30): Unit = {
    val currentTime = LocalDateTime.now().minusDays(olderThanDay)
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd000000")
    val acceptedTime = currentTime.format(formatter)
    val fs = getPathFileSystem(path, hadoopConfiguration)
    val hPath = new Path(path)
    val allFiles = fs.listStatus(hPath).sortWith((x, y) => x.getPath.toString.compareTo(y.getPath.toString) < 0)
    val toDelete = allFiles.filter(_.getPath.toString.split("=", -1).last.compareTo(acceptedTime) < 0).dropRight(1)
    logInfo(s"Clean up logs > ${olderThanDay} day(s) - ${toDelete.length}")
    for (file <- toDelete) {
      if (!fs.delete(file.getPath, true)) {
        logWarning(s"Unable to delete ${file.getPath.toString}")
      }
      else
        logDebug(s"Deleted ${file.getPath.toString}")
    }
  }

  /**
   * Fuction to delete all data in a provided path
   */
  def moveFiles(src: String, dest: String, hadoopConfiguration: Configuration = null): Unit = {
    val fs = getPathFileSystem(src, hadoopConfiguration)
    val srcPath = new Path(src)
    val destPath = new Path(dest)
    if (fs.exists(srcPath)) {
      try {
        FileUtil.copy(fs, srcPath, fs, destPath, true, fs.getConf)
        logDebug(s"$src is successfully moved to $dest")
      }
      catch {
        case e: Throwable => logInfo(s"$src can't be moved. Please check.", e.getCause)
      }
    }
  }

  /**
   * This method checks if the s3 directory is empty or not.
   *
   * @param dirPath    - dirPath
   * @param hadoopConf - Configuration.
   * @return Boolean
   */
  def isS3DirectoryNotEmpty(dirPath: String, hadoopConf: Configuration): Boolean = {
    val fs = getPathFileSystem(dirPath, hadoopConf)
    val path = new Path(dirPath)

    if (fs.exists(path)) {
      val fileStatus: Array[FileStatus] = fs.listStatus(path)
      val filesOnly = fileStatus.filter(!_.isDirectory)
      filesOnly.nonEmpty
    } else {
      false
    }
  }

  /**
   * This method checks the given s3 path exists.
   *
   * @param path  - s3 path.
   * @param spark - spark.
   * @return Boolean
   */
  def isPathExists(path: String, spark: SparkSession): Boolean = {
    val fsPath = new Path(path)
    val fs = fsPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.exists(fsPath)
  }
}
