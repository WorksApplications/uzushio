package org.sample.corpus

import java.nio.file.{Path, Paths}
import org.rogach.scallop.ScallopConf

import org.apache.spark.sql.{SparkSession, Dataset, DataFrame, Row}
import org.apache.spark.sql.functions._

import org.sample.corpus.cleaning._

object WarcCorpusCleaner {
  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[Path]](required = true)
    val inputFormat = opt[String](default = Some("parquet"))
    val output = opt[Path](default = Some(Paths.get("./out")))

    val documentDelim = opt[String](default = Some("\n\n\n"))
    val paragraphDelim = opt[String](default = Some("\n\n"))

    verify()
  }

  def run(spark: SparkSession, conf: Conf): Unit = {
    import spark.implicits._

    // each documents are separated into paragraphs (not sentences)
    val pDelim = conf.paragraphDelim()
    val data: Dataset[Seq[String]] = conf.inputFormat() match {
      case "parquet" => {
        val inputPaths = DocumentIO.formatPathList(conf.input())
        spark.read
          .format(conf.inputFormat())
          .load(inputPaths.map(_.toString): _*)
          .select("document") // ignore meta data
          .map(_.getString(0).split(pDelim).toSeq)
      }
      case "txt" | "text" => {
        DocumentIO
          .loadRawDocuments(
            spark,
            conf.input(),
            sep = conf.documentDelim()
          )
          .map(_.getString(0).split(pDelim).toSeq)
      }
      case _ => {
        throw new scala.NotImplementedError(
          s"input format ${conf.inputFormat()} is not supported."
        )
      }
    }

    // cleaning
    // Dataset[String (document)] -> Dataset[Seq[String]]
    val pipeline = setupWarcPreprocess()
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
  def setupWarcPreprocess(
      ngwordFile: Option[Path] = None
  ): Pipeline = {
    new Pipeline(
      Seq(
        new FilterJapaneseBasedOnCharacter,
        new DeduplicateElement,
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
