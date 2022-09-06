package org.sample.corpus.cleaning

import org.apache.spark.sql.Dataset

abstract class Filter extends scala.Serializable {
  /* Filters out some part of dataset that suffice the condition.
   *
   * Filtering can be per document, sentence, or word etc.
   */
  def filter(ds: Dataset[Seq[String]]): Dataset[Seq[String]]
}

/** Do nothing. */
class IdentityFilter extends Filter {
  override def filter(ds: Dataset[Seq[String]]): Dataset[Seq[String]] = ds
}

/* Filter with multiple filters combined.
 *
 *  @constructor create a new filter from given filters.
 *  @param filters filters to combine
 */
class SequenceFilter(filters: Seq[Filter]) extends Filter {
  def filter(ds: Dataset[Seq[String]]): Dataset[Seq[String]] = {
    filters.foldLeft(ds)((ds, f) => f.filter(ds))
  }
}

/* Filters documents with specific condition. */
abstract class DocumentFilter extends Filter {
  /* Determines if the document should be kept or not. */
  def isFiltered(doc: Seq[String]): Boolean

  override def filter(ds: Dataset[Seq[String]]): Dataset[Seq[String]] = {
    ds.filter(isFiltered(_))
  }
}

/* DocumentFilter with multiple filters combined.
 *
 *  @constructor create a new filter from given filters.
 *  @param filters filters to combine
 */
class SequenceDocumentFilter(filters: Seq[DocumentFilter])
    extends DocumentFilter {
  override def isFiltered(doc: Seq[String]): Boolean = {
    // return true if all filters return ture
    filters.foldLeft(true)((b, f) => b && f.isFiltered(doc))
  }
}

/* Filters sentences with specific condition. */
abstract class SentenceFilter extends Filter {
  /* Determines if the sentence should be kept or not. */
  def isFiltered(sent: String): Boolean

  override def filter(ds: Dataset[Seq[String]]): Dataset[Seq[String]] = {
    import ds.sparkSession.implicits._
    ds.map(_.filter(isFiltered)).filter(_.length > 0)
  }
}

/* SentenceFilter with multiple filters combined.
 *
 *  @constructor create a new filter from given filters.
 *  @param filters filters to combine
 */
class SequenceSentenceFilter(filters: Seq[SentenceFilter])
    extends SentenceFilter {
  override def isFiltered(sent: String): Boolean = {
    // return true if all filters return ture
    filters.foldLeft(true)((b, f) => b && f.isFiltered(sent))
  }
}
