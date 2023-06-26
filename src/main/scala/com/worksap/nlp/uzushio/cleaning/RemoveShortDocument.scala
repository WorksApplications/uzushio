package com.worksap.nlp.uzushio.cleaning

import com.typesafe.config.ConfigObject

/** Filters documents that are too short.
  *
  * @constructor
  *   create a new filter.
  * @param min
  *   the minimum number of sentences a document should contain
  */
class RemoveShortDocument(min: Int = 5)
    extends DocumentFilter
    with FieldSettable[RemoveShortDocument] {
  override def isFiltered(doc: Seq[String]): Boolean = {
    min <= doc.map(_.split("\n").length).reduce(_ + _)
  }

  override def toString(): String = s"${this.getClass.getSimpleName}(${min})"
}

object RemoveShortDocument extends FromConfig {
  override def fromConfig(conf: ConfigObject): RemoveShortDocument = {
    val args = Map[String, Option[Any]]("min" -> conf.getAs[Int]("min"))
      .collect { case (k, Some(v)) => k -> v }

    new RemoveShortDocument().setFields(args)
  }
}
