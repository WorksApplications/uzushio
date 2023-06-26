package com.worksap.nlp.uzushio.cleaning

import com.typesafe.config.ConfigObject
import org.apache.spark.sql.Dataset

/** Transforms given spark dataset. */
trait Transformer extends scala.Serializable {
  def transform(ds: Dataset[Seq[String]]): Dataset[Seq[String]]

  override def toString(): String = s"${this.getClass.getSimpleName}"
}

/** Trait to instanciate transformer based on config file.
  *
  * Every Transformers should have a companion object with this trait.
  */
trait FromConfig {
  def fromConfig(conf: ConfigObject): Transformer

  /** Wrapper class for easy config value access. */
  implicit class ConfigObjectWrapper(val conf: ConfigObject) {
    def getAs[T](key: String): Option[T] =
      Option(conf.get(key)).map(_.unwrapped.asInstanceOf[T])

    def getOrElseAs[T](key: String, default: T): T =
      conf.getAs[T](key).getOrElse(default)
  }
}

/** Transformer that does nothing. */
class Identity extends Transformer {
  override def transform(ds: Dataset[Seq[String]]): Dataset[Seq[String]] = ds
}

object Identity extends FromConfig {
  override def fromConfig(conf: ConfigObject): Identity = new Identity
}
