package org.sample.corpus

import java.nio.file.{Path, Paths}
import org.rogach.scallop.ScallopConf

import org.apache.spark.sql.{SparkSession, Dataset, DataFrame, Row}
import org.apache.spark.sql.functions._

import org.sample.corpus.cleaning._

object CorpusCleaner {
  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[Path]](required = true)
    val output = opt[Path](default = Some(Paths.get("./out")))

    val ngwords =
      opt[Path](default = Some(Paths.get("./resources/ng_words.txt")))
    verify()
  }

  def run(spark: SparkSession, conf: Conf): Unit = {
    import spark.implicits._

    val raw = DocumentIO.loadRawDocuments(spark, conf.input())
    val data = raw.as[String].map(docStr => docStr.split("\n").toSeq)

    val (normalizer, filter) = setupChitraPreprocess(conf.ngwords.toOption)

    val cleansed = filter
      .filter(normalizer.normalize(data))
      .map(doc => doc.mkString("\n"))
      .toDF

    DocumentIO.saveRawDocuments(
      cleansed,
      conf.output(),
      docCol = cleansed.columns(0)
    )
  }

  /* setup cleaner equivalent to chitra pretraining preprocess */
  def setupChitraPreprocess(
      ngwordFile: Option[Path] = None
  ): (Normalizer, Filter) = {
    (
      new SequenceDocumentNormalizer(
        Seq(
          new SequenceSentenceNormalizer(
            Seq(
              new RemoveWikipediaCitation,
              new NormalizeCharacter,
              new NormalizeWhitespace
            )
          ),
          new ConcatShortSentence
        )
      ),
      new SequenceFilter(
        Seq(
          new SequenceSentenceFilter(
            Seq(
              new RemoveEmail,
              new RemoveURL,
              new FilterBySentenceLength
            )
          ),
          new SequenceDocumentFilter(
            Seq(
              new RemoveShortDocument,
              new RemoveScriptDocument
            )
          )
        ) ++ option2seq(ngwordFile).map(RemoveNGWordDocument.fromFile(_))
      )
    )
  }

  def option2seq[T](opt: Option[T]): Seq[T] = {
    opt match {
      case Some(t) => { Seq(t) }
      case None    => { Seq() }
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val spark =
      SparkSession.builder().appName(this.getClass.getSimpleName).getOrCreate()

    try { run(spark, conf) }
    finally { spark.stop() }
  }
}
