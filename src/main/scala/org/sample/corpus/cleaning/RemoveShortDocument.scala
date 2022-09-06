package org.sample.corpus.cleaning

/** Filters documents that are too short.
  *
  * @constructor
  *   create a new filter.
  * @param min
  *   the minimum number of sentences a document should contain
  */
class RemoveShortDocument(min: Int = 5) extends DocumentFilter {
  override def isFiltered(doc: Seq[String]): Boolean = {
    min <= doc.map(_.split("\n").length).reduce(_ + _)
  }
}
