package com.unified_streaming_ingestion_framework.spark.components.sinks

import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration.SECONDS

trait TriggerType {
  /**
   * Spark structured streaming has four different types of triggers that define the timing of the streaming data processing.
   *
   * 1. Unspecified(default - equivalent to setting triggerSeconds to 0): Stream the micro-batch as soon as it has completed processing.
   *
   * 2. Fixed interval micro-batches: Query will be executed at user-specified intervals.
   *
   * 3. One-time micro-batch: Query will process all the available data and then stop on its own.
   *
   * 4. Continuous with fixed checkpoint interval (Experimental and not handled here): The query will be executed in the new low-latency, continuous processing mode.
   *
   * @param triggerSeconds
   * @return Trigger
   */
  def getTriggerType(triggerSeconds: Long) = {
    if (triggerSeconds == -1L) Trigger.Once()
    else Trigger.ProcessingTime(triggerSeconds, SECONDS)
  }

}

