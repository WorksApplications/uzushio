package org.sample.corpus.cleaning

import com.typesafe.config.ConfigObject

trait FromConfig {
  def fromConfig(conf: ConfigObject): Transformer
}

trait FieldSettable {
  def setFields(map: Map[String, Any]) = {
    for ((k, v) <- map) setField(k, v)
  }

  def setField(key: String, value: Any) = {
    this.getClass.getDeclaredFields.find(_.getName == key) match {
      case Some(field) => {
        field.setAccessible(true)
        field.set(this, value)
      }
      case None => throw new IllegalArgumentException(s"No field named ${key}")
    }
  }
}
