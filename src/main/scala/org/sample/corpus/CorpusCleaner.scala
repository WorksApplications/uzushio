package org.sample.corpus

import java.nio.file.{Path, Paths}
import org.rogach.scallop.ScallopConf
import org.apache.log4j.LogManager

import org.apache.spark.sql.{SparkSession, Dataset, DataFrame, Row}
import org.apache.spark.sql.functions._

import org.sample.corpus.cleaning._

object CorpusCleaner {
  @transient lazy val logger = LogManager.getLogger(this.getClass.getSimpleName)

  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[Path]](required = true, descr = "List of input files.")
    val inputFormat = opt[String](descr =
      "The format of input files (text/parquet). Neccessary when directory is provided as input."
    )
    val config = opt[String](
      required = true,
      default = Some("chitra"),
      descr = "Name or Path to the config file."
    )
    val output = opt[Path](default = Some(Paths.get("./out")))
    val outputFormat = opt[String](
      default = Some("text"),
      descr = "The format of output files (text/parquet)."
    )

    /** Delimiter of documents and paragraphs in input/output. */
    val delimParagraph = opt[String](default = Some("\n\n"))
    val delimDocument = opt[String](default = Some("\n\n\n"))
    val delimParagraphOut = opt[String](default = Some("\n"))
    val delimDocumentOut = opt[String](default = Some("\n\n"))

    val documentColumn = opt[String](
      default = Some("document"),
      descr = "Name of the document column (for parquet)."
    )

    verify()
  }

  val availableFormat = Set("text", "txt", "parquet")

  def detectInputFormat(
      input: List[Path],
      inputFormat: Option[String]
  ): String = {
    val ext = inputFormat.getOrElse(input(0).toString.split("\\.").last)
    if (!availableFormat.contains(ext)) {
      throw new java.lang.RuntimeException(
        s"Failed during input format detection: ${ext}."
      )
    }
    ext
  }

  /** load documents as a seq of paragraphs. */
  def loadInput(spark: SparkSession, conf: Conf): Dataset[Seq[String]] = {
    import spark.implicits._

    val fmt = detectInputFormat(conf.input(), conf.inputFormat.toOption)
    val inputPaths = DocumentIO.formatPathList(conf.input()).map(_.toString)
    val docCol = conf.documentColumn()

    val rawdf = fmt match {
      case "parquet" => {
        spark.read
          .load(inputPaths: _*)
          .select(docCol) // TODO: keep other columns
      }
      case "text" | "txt" => {
        spark.read
          .option("lineSep", conf.delimDocument())
          .text(inputPaths: _*)
          .withColumnRenamed("value", docCol)
      }
    }
    val delim = conf.delimParagraph()
    rawdf.as[String].map(_.split(delim).toSeq)
  }

  def run(spark: SparkSession, conf: Conf): Unit = {
    import spark.implicits._

    // Dataset[Seq[String (paragraph)]]
    val data = loadInput(spark, conf)

    // setup pipeline and apply
    // TODO: keep original non-document column
    val pipeline = Pipeline.from(conf.config())
    logger.info(s"pipeline: ${pipeline}")
    val processed = pipeline.transform(data)

    // write
    // TODO: parquet output
    val delim = conf.delimParagraphOut()
    processed
      .map(_.mkString(delim))
      .write
      .option("lineSep", conf.delimDocumentOut())
      .text(conf.output().toString)
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val spark =
      SparkSession.builder().appName(this.getClass.getSimpleName).getOrCreate()

    try { run(spark, conf) }
    finally { spark.stop() }
  }
}
