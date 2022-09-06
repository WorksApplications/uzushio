package org.sample.corpus.cleaning

import org.apache.spark.sql.Dataset

/** Filters documents with specific condition. */
abstract class DocumentFilter extends Transformer {

  /** Determines if the document should be kept or not. */
  def isFiltered(doc: Seq[String]): Boolean

  override def transform(ds: Dataset[Seq[String]]): Dataset[Seq[String]] = {
    ds.filter(isFiltered(_))
  }
}

/** Filters sentences with specific condition. */
abstract class SentenceFilter extends Transformer {

  /** Determines if the sentence should be kept or not. */
  def isFiltered(sent: String): Boolean

  override def transform(ds: Dataset[Seq[String]]): Dataset[Seq[String]] = {
    import ds.sparkSession.implicits._
    ds.map(_.filter(isFiltered)).filter(_.length > 0)
  }
}
