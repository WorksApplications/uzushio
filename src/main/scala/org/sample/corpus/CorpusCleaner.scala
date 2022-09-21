package org.sample.corpus

import java.nio.file.{Path, Paths}
import org.rogach.scallop.ScallopConf
import org.apache.log4j.LogManager

import org.apache.spark.sql.{SparkSession, Dataset, DataFrame, Row}
import org.apache.spark.sql.functions._

import org.sample.corpus.cleaning._

object CorpusCleaner {
  @transient lazy val logger = LogManager.getLogger(this.getClass.getSimpleName)

  /* config from CLI. */
  private class CLIConf(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[Path]](required = true, descr = "List of input files.")
    val output = opt[Path](default = Some(Paths.get("./out")))
    val config = opt[String](
      required = true,
      default = Some("chitra"),
      descr = "Name or Path to the config file."
    )

    val inputFormat = opt[String](descr =
      "Input file format (text/parquet). " +
        "If not given, try to estimate from the extension of the first file."
    )
    val inputDocDelim =
      opt[String](descr = "Delimiter of documents (text)")
    val inputDocCol =
      opt[String](descr = "Name of the document column (parquet).")

    val outputFormat = opt[String](descr = "Output file format (text/parquet).")
    val outputDocDelim =
      opt[String](descr = "Delimiter of documents (text).")
    val outputDocCol =
      opt[String](descr = "Name of the document column (parquet).")
    val outputElemDelim =
      opt[String](descr = "Delimiter of elements e.g. paragraph, sentence.")

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

  /** load documents as a seq of full text. */
  def loadInput(spark: SparkSession, conf: CLIConf): Dataset[Seq[String]] = {
    import spark.implicits._

    val fmt = detectInputFormat(conf.input(), conf.inputFormat.toOption)
    val inputPaths = DocumentIO.formatPathList(conf.input()).map(_.toString)
    val docCol = conf.inputDocCol()

    val rawdf = fmt match {
      case "parquet" => {
        spark.read
          .load(inputPaths: _*)
          .select(docCol) // TODO: keep other columns
      }
      case "text" | "txt" => {
        spark.read
          .option("lineSep", conf.inputDocDelim())
          .text(inputPaths: _*)
          .withColumnRenamed("value", docCol)
      }
    }
    rawdf.as[String].map(Seq(_))
  }

  def run(spark: SparkSession, conf: CLIConf): Unit = {
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
    val delim = conf.outputElemDelim()
    processed
      .map(_.mkString(delim))
      .write
      .option("lineSep", conf.outputDocDelim())
      .text(conf.output().toString)
  }

  def main(args: Array[String]): Unit = {
    val conf = new CLIConf(args)
    val spark =
      SparkSession.builder().appName(this.getClass.getSimpleName).getOrCreate()

    try { run(spark, conf) }
    finally { spark.stop() }
  }
}
