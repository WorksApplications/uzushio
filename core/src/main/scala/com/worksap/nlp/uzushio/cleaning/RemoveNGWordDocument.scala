package com.worksap.nlp.uzushio.cleaning

import com.typesafe.config.ConfigObject
import com.worksap.nlp.sudachi.Tokenizer
import com.worksap.nlp.uzushio.Sudachi

import collection.JavaConverters._
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import org.apache.spark.sql.Dataset

import scala.io.Source

/** Filters documents that contain one of the specified words.
  *
  * @constructor
  *   create a new filter with ng-word list.
  * @param ngwords
  *   the set of words which should not appear in the filtered documents
  */
class RemoveNGWordDocument(ngwords: Set[String]) extends Transformer {
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

  override def transform(ds: Dataset[Seq[String]]): Dataset[Seq[String]] = {
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

  override def toString(): String = s"${this.getClass.getSimpleName}(#word=${ngwords.size})"
}

object RemoveNGWordDocument extends FromConfig {
  val defaultPath = "ng_words.txt"

  def fromFile(ngwordsFile: Path): RemoveNGWordDocument = {
    val fullstr = new String(Files.readAllBytes(ngwordsFile), StandardCharsets.UTF_8)
    new RemoveNGWordDocument(
      fullstr.split("\n").map(_.trim).filter(_.nonEmpty).toSet
    )
  }

  override def fromConfig(conf: ConfigObject): RemoveNGWordDocument = {
    val pathStr = conf.getOrElseAs[String]("path", defaultPath)

    val filepath = Paths.get(pathStr)
    if (filepath.toFile.exists) {
      fromFile(filepath)
    } else {
      val fullstr = Source.fromResource(pathStr).mkString
      new RemoveNGWordDocument(
        fullstr.split("\n").map(_.trim).filter(_.nonEmpty).toSet
      )
    }
  }
}
