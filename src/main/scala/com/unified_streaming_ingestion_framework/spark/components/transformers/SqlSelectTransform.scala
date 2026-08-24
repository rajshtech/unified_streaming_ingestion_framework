package com.unified_streaming_ingestion_framework.spark.components.transformers

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters.asScalaBufferConverter

class SqlSelectTransform(selectExprs: List[String]) extends StreamingTransform {
  override def key: String = SqlSelectTransform.KEY

  override def transform(dataFrame: DataFrame): DataFrame = dataFrame.selectExpr(selectExprs: _*)
}

object SqlSelectTransform {
  val KEY = "select"

  def apply(config: Config): SqlSelectTransform =
    new SqlSelectTransform(config.getStringList("selectExpr").asScala.toList)
}
