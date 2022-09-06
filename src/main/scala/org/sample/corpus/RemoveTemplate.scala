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

    val raw = DocumentIO.loadRawDocuments(spark, conf.input())

    val uniq = raw.distinct

    val docs = uniq.as[String].map(docStr => docStr.split("\n").toSeq)
    val (normalizer, filter) = setupRemoveTemplate(
      conf.minRepeat(),
      conf.substrs.toOption,
      conf.perSentence()
    )

    val cleansed = filter
      .filter(normalizer.normalize(docs))
      .map(doc => doc.mkString("\n"))
      .toDF

    DocumentIO.saveRawDocuments(
      cleansed,
      conf.output(),
      docCol = cleansed.columns(0)
    )
  }

  /* setup cleaner to remove specific substrings */
  def setupRemoveTemplate(
      minRep: Int = 2,
      substrFile: Option[Path] = None,
      perSentence: Boolean
  ): (Normalizer, Filter) = {
    (
      new SequenceDocumentNormalizer(
        Seq(
          new DeduplicateRepeatingSentence(minRep)
        ) ++ option2seq(substrFile).map(
          RemoveSubstring.fromFile(_, perSentence = perSentence)
        )
      ),
      new RemoveShortDocument
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
