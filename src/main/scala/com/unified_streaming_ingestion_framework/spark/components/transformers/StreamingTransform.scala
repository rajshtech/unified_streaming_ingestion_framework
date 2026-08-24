package com.unified_streaming_ingestion_framework.spark.components.transformers

import org.apache.spark.sql.DataFrame

trait StreamingTransform {
  def key: String

  def transform(dataFrame: DataFrame): DataFrame
}
