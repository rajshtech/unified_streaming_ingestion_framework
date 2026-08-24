package com.unified_streaming_ingestion_framework.spark.components.transformers

import com.typesafe.config.Config
import com.unified_streaming_ingestion_framework.spark.components.utils.CommonUtils.getReadOrWriteStreamOptions
import org.apache.spark.sql.DataFrame

/**
 * Applies one or more derived-column expressions on top of the existing columns, e.g.
 * "batchid": "date_format(timestamp,'yyyyMMdd') as batchid"
 */
class CustomTransform(customExprs: Map[String, String]) extends StreamingTransform {
  override def key: String = CustomTransform.KEY

  override def transform(dataFrame: DataFrame): DataFrame =
    customExprs.values.foldLeft(dataFrame)((df, expr) => df.selectExpr("*", expr))
}

object CustomTransform {
  val KEY = "custom"

  def apply(config: Config): CustomTransform =
    new CustomTransform(getReadOrWriteStreamOptions(config, "custom_transform").getOrElse(Map.empty))
}
