package org.sample.corpus.cleaning

import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Files}

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
        .filter(_ > 0) // keep first sentence regardless of its length

      val appended = shortSentIdx.reverse.foldLeft(doc)((d, i) =>
        d.updated(i - 1, d(i - 1) + d(i))
      )

      for (i <- 0 until appended.length if (!shortSentIdx.contains(i)))
        yield appended(i)
    }
  }
}

/* Removes given substrings from documents.
 *
 * If perSentence is true, remove if it starts/ends at newline.
 */
class RemoveSubstring(substrs: Set[String], perSentence: Boolean = false)
    extends DocumentNormalizer {
  val substrPattern = perSentence match {
    case false => { s"""(${substrs.mkString("|")})""".r }
    case true  => { s"""(?m)(^${substrs.mkString("$|^")}$$)""".r }
  }

  override def normalizeDocument(doc: Seq[String]): Seq[String] = {
    val fullDoc = doc.mkString("\n")
    val removed = substrPattern.replaceAllIn(fullDoc, "")
    removed.split("\n").filter(_.length > 0).toSeq
  }
}

object RemoveSubstring {
  def fromFile(
      substrFile: Path,
      delim: String = "\n\n",
      perSentence: Boolean = false
  ): RemoveSubstring = {
    val fullstr =
      new String(Files.readAllBytes(substrFile), StandardCharsets.UTF_8)
    new RemoveSubstring(
      fullstr.split(delim).map(_.trim).filter(_.nonEmpty).toSet,
      perSentence
    )
  }
}

/* Deduplicate same sentences repeating many times.
 */
class DeduplicateRepeatingSentences(minRep: Int = 2)
    extends DocumentNormalizer {
  override def normalizeDocument(doc: Seq[String]): Seq[String] = {
    var (i, j) = (0, 0)
    var indices: Seq[Int] = Vector()
    while (i < doc.length) {
      j = i + 1
      while ((j < doc.length) && (doc(i) == doc(j))) { j += 1 }

      if (i + minRep <= j) { indices :+= i }
      else { indices ++= i until j }
      i = j
    }
    for (i <- indices) yield doc(i)
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

/** Removes non-printable characters.
  *
  * Following python's str.isprintable, remove unicode general-category "Other"
  * or "Separator" except space. We also keep surrogate code points (that are
  * not in python).
  *
  * @param keepWS
  *   If true, keep whitespaces other than space (" "), including \u3000. This
  *   is not python compatible behaviour.
  */
class CharacterNormalizer(keepWS: Boolean = false) extends SentenceNormalizer {
  val nonPrintablePattern =
    if (keepWS) """[\p{gc=C}\p{gc=Z}&&[^\s　\p{gc=Cs}]]""".r
    else """[\p{gc=C}\p{gc=Z}&&[^ \p{gc=Cs}]]""".r

  override def normalizeSentence(sent: String): String = {
    nonPrintablePattern.replaceAllIn(sent, "")
  }
}

/* Removes excess whitespaces. */
class WhitespaceNormalizer extends SentenceNormalizer {
  val continuousWhitespacePattern = """[\s　]+""".r

  override def normalizeSentence(sent: String): String = {
    continuousWhitespacePattern.replaceAllIn(sent, " ")
  }
}

/* Removes citation markers (from Wikipedia). */
class CitationNormalizer extends SentenceNormalizer {
  val citationPattern =
    """\[\d+?\]|\[要.+?\]|\{\{+[^{}]+?\}\}+|\[(要出典|リンク切れ|.+?\?)\]""".r

  override def normalizeSentence(sent: String): String = {
    citationPattern.replaceAllIn(sent, "")
  }
}
