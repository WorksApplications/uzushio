package org.sample.corpus.cleaning

import com.typesafe.config.ConfigObject

/** Filters sentences that are too short or too long.
  *
  * @constructor
  *   create a new filter.
  * @param min
  *   the minimum number of characters a sentence should contain
  * @param max
  *   the maximum number of characters a sentence should contain
  */
class FilterBySentenceLength(min: Int = 10, max: Int = 200)
    extends SentenceFilter
    with FieldSettable[FilterBySentenceLength] {
  override def isFiltered(sent: String): Boolean = {
    min <= sent.length && sent.length <= max
  }

  override def toString(): String = {
    s"${this.getClass.getSimpleName}(${min}, ${max})"
  }
}

object FilterBySentenceLength extends FromConfig {
  override def fromConfig(conf: ConfigObject): FilterBySentenceLength = {
    val args = Map[String, Option[Any]](
      "min" -> Option(conf.get("min")).map(_.unwrapped.asInstanceOf[Int]),
      "max" -> Option(conf.get("max")).map(_.unwrapped.asInstanceOf[Int])
    ).collect { case (k, Some(v)) => k -> v }

    new FilterBySentenceLength().setFields(args)
  }
}
