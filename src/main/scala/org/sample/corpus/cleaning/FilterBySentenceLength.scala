package org.sample.corpus.cleaning

import com.typesafe.config.ConfigObject

trait FromConf {
  def apply(conf: ConfigObject): Transformer
}

/** Filters sentences that are too short or too long.
  *
  * @constructor
  *   create a new filter.
  * @param min
  *   the minimum number of characters a sentence should contain
  * @param max
  *   the maximum number of characters a sentence should contain
  */
class FilterBySentenceLength(
    min: Int = FilterBySentenceLength.defaultMin,
    max: Int = FilterBySentenceLength.defaultMax
) extends SentenceFilter {
  override def isFiltered(sent: String): Boolean = {
    min <= sent.length && sent.length <= max
  }

  def companion() = FilterBySentenceLength

  override def toString(): String = {
    s"${this.getClass.getSimpleName}(${min}, ${max})"
  }
}

object FilterBySentenceLength extends FromConf {
  val defaultMin = 10
  val defaultMax = 200

  def apply(conf: ConfigObject) = {
    new FilterBySentenceLength(
      min = Option(conf.get("min"))
        .map(_.unwrapped.asInstanceOf[Int])
        .getOrElse(defaultMin),
      max = Option(conf.get("max"))
        .map(_.unwrapped.asInstanceOf[Int])
        .getOrElse(defaultMax)
    )
  }
}
