package com.unified_streaming_ingestion_framework.spark.pipelines

object StreamingPipelineController extends UnifiedStreamingManager {
  def main(args: Array[String]): Unit = {
    runPipeline(args)
  }
}
