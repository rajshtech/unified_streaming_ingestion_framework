package com.unified_streaming_ingestion_framework.spark

import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.io.Source
import scala.util.{Failure, Success, Try}

package object components {

  /**
   * load configuration from config file and return a typesafe config object
   *
   * @param configFilePath configuration path to read HOCON(JSON) config
   * @return application configuration
   */

  def loadConfigFile(configFilePath: String): Config = {
    val strBuffer = Source.fromFile(configFilePath)
    val strConfig = strBuffer.mkString

    val parseConfig = ConfigFactory.parseString(strConfig).resolve()

    strBuffer.close()

    parseConfig
  }


  implicit class ConfigUtil(config: Config) {
    def getIntOpt(key: String): Option[Int] = getOpt(key, config.getInt)

    def getLongOpt(key: String): Option[Long] = getOpt(key, config.getLong)

    def getListOpt(key: String): Option[Seq[String]] = getOpt(key, k => config.getStringList(k).asScala)

    def getSchemaFromConfig(key: String): List[StructField] = {
      config.getConfigListOpt(key).getOrElse(Seq.empty[Config]).toList.map {
        s: Config => StructField(s.getString("name"), str2Type(s.getString("type")), s.getStringOpt("nullable").getOrElse("true").toBoolean)
      }
    }

    def getStringOpt(key: String): Option[String] = getOpt(key, config.getString)

    def getConfigListOpt(key: String): Option[Seq[Config]] = getOpt(key, k => config.getConfigList(k).asScala)

    def getConfigAsMap(key: String): Map[String, String] = {
      config
        .getConfigOpt(key).getOrElse(ConfigFactory.empty()).entrySet()
        .map(e => e.getKey -> e.getValue.unwrapped().toString)
        .toMap
    }

    def getConfigOpt(key: String): Option[Config] = getOpt(key, config.getConfig)
  }

  def str2Type(str: String): DataType = {
    str.toLowerCase match {
      case "string" => StringType
      case "date" => DateType
      case "integer" => IntegerType
      case "long" => LongType
      case "timestamp" => TimestampType
      case "boolean" => BooleanType
      case "float" => FloatType
    }
  }

  private def getOpt[T](key: String, fn: String => T) = Try(fn(key)) match {
    case Success(t) => Option(t)
    case Failure(e) => e match {
      case t: ConfigException.Missing => None
      case t => throw t
    }
  }

}
