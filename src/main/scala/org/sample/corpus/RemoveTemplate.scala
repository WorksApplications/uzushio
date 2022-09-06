package org.sample.corpus

import java.nio.file.{Path, Paths}
import org.rogach.scallop.ScallopConf

import org.apache.spark.sql.{SparkSession, Dataset, DataFrame, Row}
import org.apache.spark.sql.functions._

import org.sample.corpus.cleaning._

object RemoveTemplate {
  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[Path]](required = true)
    val output = opt[Path](default = Some(Paths.get("./out")))

    val minRepeat = opt[Int](default = Some(2))
    val substrs =
      opt[Path](default = Some(Paths.get("./resources/template_sentences.txt")))
    val perSentence = opt[Boolean]()
    verify()
  }

  def run(spark: SparkSession, conf: Conf): Unit = {
    import spark.implicits._

    // Dataset[String (document)]
    val raw = DocumentIO.loadRawDocuments(spark, conf.input())
    val uniq = raw.distinct

    // cleaning
    // Dataset[String (document)] -> Dataset[Seq[String]]
    val docs = uniq.as[String].map(_.split("\n").toSeq)
    val pipeline = setupRemoveTemplate(
      conf.minRepeat(),
      conf.substrs.toOption,
      conf.perSentence()
    )
    val cleansed = pipeline.transform(docs)

    // Dataset[Seq[String]] -> Dataset[String (document)]
    val result = cleansed.map(doc => doc.mkString("\n")).toDF

    DocumentIO.saveRawDocuments(
      result,
      conf.output(),
      docCol = result.columns(0)
    )
  }

  /* setup cleaner to remove specific substrings */
  def setupRemoveTemplate(
      minRep: Int,
      substrFile: Option[Path],
      perSentence: Boolean
  ): Pipeline = {
    new Pipeline(
      Seq(
        new DeduplicateRepeatingSentence(minRep),
        substrFile
          .map(RemoveSubstring.fromFile(_, perSentence = perSentence))
          .getOrElse(new IdentityTransformer),
        new RemoveShortDocument
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
