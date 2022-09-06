package org.sample.corpus.cleaning

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
    extends SentenceFilter {
  override def isFiltered(sent: String): Boolean = {
    min <= sent.length && sent.length <= max
  }
}
