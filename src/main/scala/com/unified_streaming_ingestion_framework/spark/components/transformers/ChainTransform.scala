package com.unified_streaming_ingestion_framework.spark.components.transformers

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.asScalaBufferConverter

//TODO Transformations can be moved to common utils repo in future
class ChainTransform(chain: List[StreamingTransform]) extends StreamingTransform {
  override def key: String = ChainTransform.KEY

  override def transform(dataFrame: DataFrame): DataFrame = {
    chain.foldLeft(dataFrame)((a: DataFrame, b: StreamingTransform) => b.transform(a))
  }
}

object ChainTransform {
  val KEY = "chain"

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def apply(config: Config): ChainTransform = {
    val chainTransforms: List[StreamingTransform] = config.getConfigList("chain").asScala.toList
      .foldLeft(List[StreamingTransform]())(
        (a: List[StreamingTransform], c: Config) => {
          val transformer: StreamingTransform = StreamingTransformFactory.make(c)
          logger.info(s"Adding transformer with key: [${transformer.key}] to the chain.")
          a :+ transformer
        }
      )
    ChainTransform(chainTransforms)
  }

  def apply(chain: List[StreamingTransform]): ChainTransform = new ChainTransform(chain)
}
