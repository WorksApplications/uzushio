package org.sample.corpus.cleaning

import org.apache.spark.sql.Dataset

abstract class Normalizer extends scala.Serializable {
  /* Normalizes document in the dataset. */
  def normalize(ds: Dataset[Seq[String]]): Dataset[Seq[String]]
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

/* Concat too short sentences to the previous sentence. */
class ConcatShortSentenceNormalizer(concatThr: Int = 2)
    extends DocumentNormalizer {
  override def normalizeDocument(doc: Seq[String]): Seq[String] = {
    if (doc.length <= 1) {
      doc
    } else {
      val shortSentIdx = doc.zipWithIndex
        .map(z => { if (z._1.length <= concatThr) z._2 else -1 })
        .filter(_ > 0) // keep first sentence as is regardless of its length

      val appended = shortSentIdx.reverse.foldLeft(doc)((d, i) =>
        d.updated(i - 1, d(i - 1) + d(i))
      )

      for (i <- 0 until appended.length if (!shortSentIdx.contains(i)))
        yield appended(i)
    }
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

/* Removes non-printable characters and excess whitespaces. */
class WhitespaceNormalizer extends SentenceNormalizer {
  // todo: this may not be a good way to rm non-printable chars.
  val nonPrintablePattern = """\p{C}""".r
  val continuousWhitespacePattern = """\s+""".r

  override def normalizeSentence(sent: String): String = {
    val rmNP = nonPrintablePattern.replaceAllIn(sent, "")
    continuousWhitespacePattern.replaceAllIn(rmNP, " ")
  }
}

/* Removes citation markers.*/
class CitationNormalizer extends SentenceNormalizer {
  val citationPattern =
    """\[\d+?\]|\[要.+?\]|\{\{+[^{}]+?\}\}+|\[(要出典|リンク切れ|.+?\?)\]""".r

  override def normalizeSentence(sent: String): String = {
    citationPattern.replaceAllIn(sent, "")
  }
}
