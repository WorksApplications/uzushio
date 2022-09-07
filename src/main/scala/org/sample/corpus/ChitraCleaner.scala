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

    // Dataset[String (document)]
    val raw = DocumentIO.loadRawDocuments(spark, conf.input())

    // cleaning
    // Dataset[String (document)] -> Dataset[Seq[String]]
    val data = raw.as[String].map(_.split("\n").toSeq)
    val pipeline = setupChitraPreprocess(conf.ngwords.toOption)
    val cleansed = pipeline.transform(data)

    // Dataset[Seq[String]] -> Dataset[String (document)]
    val result = cleansed.map(_.mkString("\n")).toDF

    DocumentIO.saveRawDocuments(
      result,
      conf.output(),
      docCol = result.columns(0)
    )
  }

  /* setup cleaner equivalent to chitra pretraining preprocess */
  def setupChitraPreprocess(
      ngwordFile: Option[Path] = None
  ): Pipeline = {
    new Pipeline(
      Seq(
        new RemoveWikipediaCitation,
        new NormalizeCharacter,
        new NormalizeWhitespace,
        new ConcatShortSentence,
        new RemoveEmail,
        new RemoveURL,
        new FilterBySentenceLength,
        new RemoveShortDocument,
        new RemoveScriptDocument,
        ngwordFile
          .map(RemoveNGWordDocument.fromFile(_))
          .getOrElse(new IdentityTransformer)
      )
    )
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val spark =
      SparkSession.builder().appName(this.getClass.getSimpleName).getOrCreate()

    try { run(spark, conf) }
    finally { spark.stop() }
  }
}
