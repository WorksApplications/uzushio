package org.sample.corpus

import org.apache.spark.sql.{SparkSession, Dataset}

import collection.JavaConverters._
import com.worksap.nlp.sudachi.{DictionaryFactory, Tokenizer}

object Sudachi {
  def parseSplitMode(mode: String): Tokenizer.SplitMode = {
    // Parse SplitMode from a string.
    mode.capitalize match {
      case "A" => Tokenizer.SplitMode.A
      case "B" => Tokenizer.SplitMode.B
      case _   => Tokenizer.SplitMode.C
    }
  }

  def setupSudachiTokenizer(): Tokenizer = {
    // create Tokenizer instance
    // system_core.dict must be in cwd.
    new DictionaryFactory().create().create()
  }

  def tokenize(
      tok: Tokenizer,
      mode: Tokenizer.SplitMode,
      sent: String
  ): String = {
    // tokenize a sentence and return surfaces using given tokenizer and split mode
    val morphemes = tok.tokenize(mode, sent).asScala
    morphemes.map(_.surface()).mkString(" ")
  }

  def tokenizeDocuments(
      spark: SparkSession,
      documents: Dataset[String],
      modeStr: String = "C"
  ): Dataset[String] = {
    // split each sentences in each documents into tokens using sudachi.
    import spark.implicits._

    val mode = parseSplitMode(modeStr)

    documents.mapPartitions(iter => {
      // setup tokenizer per partition
      val tok = setupSudachiTokenizer()
      iter.map(doc => {
        doc
          .split("\n")
          .map(tokenize(tok, mode, _))
          .mkString("\n")
      })
    })
  }
}
