package com.worksap.nlp.uzushio.cleaning

import org.apache.spark.sql.Dataset

/** Normalizes document in document-wise way. */
abstract class DocumentNormalizer extends Transformer {
  def normalizeDocument(doc: Seq[String]): Seq[String]

  override def transform(ds: Dataset[Seq[String]]): Dataset[Seq[String]] = {
    import ds.sparkSession.implicits._
    ds.map(doc => normalizeDocument(doc))
  }
}

/** Normalizes document in sentence-wise way. */
abstract class SentenceNormalizer extends DocumentNormalizer {
  def normalizeSentence(sent: String): String

  override def normalizeDocument(doc: Seq[String]): Seq[String] = {
    doc.map(sent => normalizeSentence(sent))
  }
}
