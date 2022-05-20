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

    val ngwords = opt[Path]()
    verify()
  }

  def run(spark: SparkSession, conf: Conf): Unit = {
    import spark.implicits._

    val raw = DocumentIO.loadRawDocuments(spark, conf.input())
    val data = raw.as[String].map(docStr => docStr.split("\n").toSeq)

    val normaliser = setupFullNormalizer()
    val filter = setupFullFilter(conf.ngwords.toOption)

    val cleansed = filter
      .filter(normaliser.normalize(data))
      .map(doc => doc.mkString("\n"))
      .toDF

    DocumentIO.saveRawDocuments(
      cleansed,
      conf.output(),
      docCol = cleansed.columns(0)
    )
  }

  def setupFullNormalizer(): Normalizer = {
    new SequenceDocumentNormalizer(
      Seq(
        new SequenceSentenceNormalizer(
          Seq(
            new CitationNormalizer,
            new CharacterNormalizer,
            new WhitespaceNormalizer
          )
        ),
        new ConcatShortSentenceNormalizer
      )
    )
  }

  def setupFullFilter(ngwordFile: Option[Path] = None): Filter = {
    new SequenceFilter(
      Seq(
        new SequenceSentenceFilter(
          Seq(
            new EmailFilter,
            new UrlFilter,
            new SentenceLengthFilter
          )
        ),
        new SequenceDocumentFilter(
          Seq(
            new ShortDocumentFilter,
            new ScriptFilter
          )
        )
      ) ++ {
        ngwordFile match {
          case Some(p) => { Seq(NgWordFilter.fromFile(p)) }
          case None    => { Seq() }
        }
      }
    )
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val spark =
      SparkSession.builder().appName("CorpusCleaner").getOrCreate()

    try { run(spark, conf) }
    finally { spark.stop() }
  }
}
