package com.unified_streaming_ingestion_framework.spark.components.transformers

import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

case class UnknownTransformTypeException(msg: String) extends Exception(msg)

object StreamingTransformFactory {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)


  def make(config: Config): StreamingTransform = {
    val _type: String = config.getString("type").toLowerCase
    require(_type.nonEmpty)
    _type match {
      case ChainTransform.KEY => ChainTransform(config)
      case SqlSelectTransform.KEY => SqlSelectTransform(config)
      case CustomTransform.KEY => CustomTransform(config)
      case _ => throw UnknownTransformTypeException(s"Unidentified Transform Configuration: ${_type}")
    }
  }
}
