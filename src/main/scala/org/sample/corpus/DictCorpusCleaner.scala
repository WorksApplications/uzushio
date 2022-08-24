package org.sample.corpus

import java.nio.file.{Path, Paths}
import org.rogach.scallop.ScallopConf

import org.apache.spark.sql.{SparkSession, Dataset, DataFrame, Row}
import org.apache.spark.sql.functions._

import org.sample.corpus.cleaning._

object DictCorpusCleaner {
  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[Path]](required = true)
    val output = opt[Path](default = Some(Paths.get("./out")))

    verify()
  }

  def run(spark: SparkSession, conf: Conf): Unit = {
    import spark.implicits._

    // cleaning
    // Dataset[String (document)]
    val raw = DocumentIO.loadRawDocuments(spark, conf.input())

    // cleaning
    // Dataset[String (document)] -> Dataset[Seq[String]]
    val data = raw.as[String].map(docStr => docStr.split("\n").toSeq)
    val (normalizer, filter) = setupDictCorpusPreprocess()
    val cleaned = filter.filter(normalizer.normalize(data))

    // Dataset[Seq[String]] -> Dataset[String (document)]
    val result = cleaned.map(_.mkString("\n")).toDF

    // write
    DocumentIO.saveRawDocuments(
      result,
      conf.output(),
      docCol = result.columns(0),
      sep = "\n"
    )
  }

  /* setup cleaner equivalent to chitra pretraining preprocess */
  def setupDictCorpusPreprocess(): (Normalizer, Filter) = {
    (
      new SequenceDocumentNormalizer(
        Seq(
          new SequenceSentenceNormalizer(
            Seq(
              new CharacterNormalizer(keepWS = true),
              new WhitespaceNormalizer
            )
          )
        )
      ),
      new DuplicateSentenceFilter()
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
