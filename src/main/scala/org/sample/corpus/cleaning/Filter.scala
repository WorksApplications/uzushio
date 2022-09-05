package org.sample.corpus.cleaning

import collection.JavaConverters._
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Files}
import com.worksap.nlp.sudachi.Tokenizer

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.monotonically_increasing_id

import org.sample.corpus.Sudachi

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

/** Deduplicate elements of sequences, keeping seq order. */
class DedupElemFilter extends Filter {
  override def filter(ds: Dataset[Seq[String]]): Dataset[Seq[String]] = {
    import ds.sparkSession.implicits._

    // add indices: (doc_id, elem_id, txt)
    val indexed = ds
      .withColumn("did", monotonically_increasing_id)
      .flatMap(r =>
        r.getSeq[String](0).zipWithIndex.map(z => (r.getLong(1), z._2, z._1))
      )
    // drop duplicate paragraphs
    val dedup = indexed.dropDuplicates("_3")
    // reconstruct documents
    dedup
      .groupByKey(_._1)
      .mapGroups((k, itr) => itr.toSeq.sortBy(_._2).map(_._3))
  }
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

/** Filters sentences that are duplicate (over documents).
  *
  * @param nDup
  *   Sentences that appears equal or more than this number will be filtered.
  */
class DuplicateSentenceFilter(nDup: Int = 2) extends Filter {
  def filter(ds: Dataset[Seq[String]]): Dataset[Seq[String]] = {
    import ds.sparkSession.implicits._

    val count = ds.flatMap(doc => doc).groupBy("value").count()
    val dupSentSet =
      count.filter(s"count >= ${nDup}").map(r => r.getString(0)).collect.toSet
    val firstFlag = scala.collection.mutable.Set(dupSentSet.toSeq: _*)

    ds.map(doc =>
      doc.filter(sent => {
        if (!dupSentSet.contains(sent)) { true }
        else {
          val isFirst = firstFlag.contains(sent)
          firstFlag -= sent
          isFirst
        }
      })
    )
  }
}

/* Filters documents that contain one of the specified words.
 *
 *  @constructor create a new filter with ng-word list.
 *  @param ngwords the set of words which should not appear in the filtered documents
 */
class NgWordFilter(ngwords: Set[String]) extends Filter {
  val ngwordPattern = s"""(${ngwords.mkString("|")})""".r
  val mode = Tokenizer.SplitMode.C

  def containsNgword(tok: Tokenizer, doc: Seq[String]): Boolean = {
    for (sent <- doc) {
      val matchIter = ngwordPattern.findAllMatchIn(sent)
      val (matches, forSize) = matchIter.duplicate

      if (forSize.size != 0) {
        try {
          val morphmes = tok.tokenize(sent).asScala
          val morphBegins = morphmes.map(_.begin()).toSet
          val morphEnds = morphmes.map(_.end()).toSet

          for (m <- matches) {
            if (morphBegins.contains(m.start) && morphEnds.contains(m.end)) {
              return true
            }
          }
        } catch {
          case err: Exception => println(s"$sent")
        }
      }
    }
    false
  }

  def filter(ds: Dataset[Seq[String]]): Dataset[Seq[String]] = {
    import ds.sparkSession.implicits._

    if (ngwords.size == 0) { ds }
    else {
      ds.mapPartitions(iter => {
        // setup sudachi tokenizer per partition
        val tok = Sudachi.setupSudachiTokenizer()
        iter.filter(doc => !containsNgword(tok, doc))
      })
    }
  }
}

object NgWordFilter {
  def fromFile(ngwordsFile: Path): NgWordFilter = {
    val fullstr =
      new String(Files.readAllBytes(ngwordsFile), StandardCharsets.UTF_8)
    new NgWordFilter(fullstr.split("\n").map(_.trim).filter(_.nonEmpty).toSet)
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

/* Filters documents that contain script. */
class ScriptFilter extends DocumentFilter {
  val curlyBracketsPattern = """[\{|\}]""".r

  def isFilteredSent(sent: String): Boolean = {
    curlyBracketsPattern.findFirstIn(sent).isEmpty
  }

  override def isFiltered(doc: Seq[String]): Boolean = {
    doc.foldLeft(true)((b, sent) => b && isFilteredSent(sent))
  }
}

/* Filters documents that are too short.
 *
 *  @constructor create a new filter.
 *  @param min the minimum number of sentences a document should contain
 */
class ShortDocumentFilter(min: Int = 5) extends DocumentFilter {
  override def isFiltered(doc: Seq[String]): Boolean = {
    min <= doc.map(_.split("\n").length).reduce(_ + _)
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

/* Filters sentences that contain email address. */
class EmailFilter extends SentenceFilter {
  val emailPattern = """[\w\d_-]+@[\w\d_-]+\.[\w\d._-]+""".r

  override def isFiltered(sent: String): Boolean = {
    emailPattern.findFirstIn(sent).isEmpty
  }
}

/* Filters sentences that contain URL. */
class UrlFilter extends SentenceFilter {
  val urlPattern = """(https?|sftp?)://[\w/:%#\$&\?\(\)~\.=\+\-]+""".r

  override def isFiltered(sent: String): Boolean = {
    urlPattern.findFirstIn(sent).isEmpty
  }
}

/* Filters sentences that are too short or too long.
 *
 *  @constructor create a new filter.
 *  @param min the minimum number of characters a sentence should contain
 *  @param max the maximum number of characters a sentence should contain
 */
class SentenceLengthFilter(min: Int = 10, max: Int = 200)
    extends SentenceFilter {
  override def isFiltered(sent: String): Boolean = {
    min <= sent.length && sent.length <= max
  }
}

/** Filters non Japanese document based on the type of characters.
  *
  * Default threshold follows nwc-toolkit:text-filter.
  *
  * @param kanaRate
  *   texts with hiragana/katakana less than this are filtered.
  * @param jpRate
  *   texts with kana/kanji less than this are filtered.
  */
class CharacterBaseJapaneseFilter(kanaRate: Double = 0.05, jpRate: Double = 0.7)
    extends SentenceFilter {
  val kanaPattern = """\p{InHiragana}|\p{InKatakana}""".r
  val jpCharPattern =
    """\p{InHiragana}|\p{InKatakana}|\p{InCJKUnifiedIdeographs}""".r

  override def isFiltered(sent: String): Boolean = {
    val kanaCount = kanaPattern.findAllIn(sent).length.toDouble
    val jpCount = jpCharPattern.findAllIn(sent).length.toDouble
    val charCount = sent.length.toDouble

    (kanaCount / charCount) > kanaRate && (jpCount / charCount) > jpRate
  }
}
