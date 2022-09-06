package org.sample.corpus.cleaning

import org.apache.spark.sql.Dataset

abstract class Normalizer extends scala.Serializable {
  /* Normalizes document in the dataset. */
  def normalize(ds: Dataset[Seq[String]]): Dataset[Seq[String]]
}

/** Do nothing. */
class IdentityNormalizer extends Normalizer {
  override def normalize(ds: Dataset[Seq[String]]): Dataset[Seq[String]] = ds
}

/* Normalizer with multiple ones combined.
 *
 *  @constructor create a new normalizer from given ones.
 *  @param normalizers normalizers to combine
 */
class SequenceNormalizer(normalizers: Seq[Normalizer]) extends Normalizer {
  override def normalize(ds: Dataset[Seq[String]]): Dataset[Seq[String]] = {
    normalizers.foldLeft(ds)((ds, n) => n.normalize(ds))
  }
}

/* Normalizes document in document-wise way. */
abstract class DocumentNormalizer extends Normalizer {
  def normalizeDocument(doc: Seq[String]): Seq[String]

  override def normalize(ds: Dataset[Seq[String]]): Dataset[Seq[String]] = {
    import ds.sparkSession.implicits._
    ds.map(doc => normalizeDocument(doc))
  }
}

/* DocumentNormalizer with multiple ones combined.
 *
 *  @constructor create a new normalizer from given ones.
 *  @param normalizers normalizers to combine
 */
class SequenceDocumentNormalizer(normalizers: Seq[DocumentNormalizer])
    extends DocumentNormalizer {
  override def normalizeDocument(doc: Seq[String]): Seq[String] = {
    normalizers.foldLeft(doc)((doc, n) => n.normalizeDocument(doc))
  }
}

/* Normalizes document in sentence-wise way. */
abstract class SentenceNormalizer extends DocumentNormalizer {
  def normalizeSentence(sent: String): String

  override def normalizeDocument(doc: Seq[String]): Seq[String] = {
    doc.map(sent => normalizeSentence(sent))
  }
}

/* SentenceNormalizer with multiple ones combined.
 *
 *  @constructor create a new normalizer from given ones.
 *  @param normalizers normalizers to combine
 */
class SequenceSentenceNormalizer(normalizers: Seq[SentenceNormalizer])
    extends SentenceNormalizer {
  override def normalizeSentence(sent: String): String = {
    normalizers.foldLeft(sent)((sent, n) => n.normalizeSentence(sent))
  }
}
