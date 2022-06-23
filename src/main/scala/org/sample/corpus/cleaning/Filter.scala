package org.sample.corpus.cleaning

import collection.JavaConverters._
import java.nio.file.Path
import scala.io.Source
import com.worksap.nlp.sudachi.Tokenizer

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._

import org.sample.corpus.Sudachi

abstract class Filter extends scala.Serializable {
  /* Filters documents in the dataset.
   *
   * Filtering can be per document, sentence, or word etc.
   */
  def filter(ds: Dataset[Seq[String]]): Dataset[Seq[String]]
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
  def fromFile(ngwords_file: Path): NgWordFilter = {
    val src = Source.fromFile(ngwords_file.toFile, "utf-8")
    try {
      val ngwords = src.getLines().map(_.trim).filter(_.nonEmpty).toSet
      new NgWordFilter(ngwords)
    } finally {
      src.close
    }
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
    min <= doc.length
  }
}

/* Filters sentences with specific condition. */
abstract class SentenceFilter extends Filter {
  /* Determines if the sentence should be kept or not. */
  def isFiltered(sent: String): Boolean

  override def filter(ds: Dataset[Seq[String]]): Dataset[Seq[String]] = {
    import ds.sparkSession.implicits._
    ds.map(_.filter(isFiltered))
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
