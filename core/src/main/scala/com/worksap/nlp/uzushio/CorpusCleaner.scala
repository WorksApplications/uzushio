package com.worksap.nlp.uzushio

import java.nio.file.{Files, Path, Paths}
import org.rogach.scallop.ScallopConf
import com.typesafe.config.{Config, ConfigFactory}
import com.worksap.nlp.uzushio.cleaning.Pipeline
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object CorpusCleaner {
  @transient lazy val logger = LogManager.getLogger(this.getClass.getSimpleName)

  /* Config from CLI. */
  private class CLIConf(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[Path]](required = true, descr = "List of input files.")
    val output = opt[Path](default = Some(Paths.get("./out")))
    val config = opt[String](
      default = Some("chitra"),
      descr = "Name or Path to the config file."
    )

    val inputFormat = opt[String](descr = "Input file format (text/parquet).")
    val inputDelimiter =
      opt[String](descr = "Delimiter of documents (text)")
    val inputColumn =
      opt[String](descr = "Name of the document column (parquet).")

    val outputFormat = opt[String](descr = "Output file format (text/parquet).")
    val outputDelimiter =
      opt[String](descr = "Delimiter of documents (text).")
    val outputColumn =
      opt[String](descr = "Name of the document column (parquet).")
    val outputElementDelimiter =
      opt[String](descr = "Delimiter of elements e.g. paragraph, sentence.")

    verify()
  }

  /* Config that cliconf and configfile merged.
   *
   * Prefers cli > config file > default value.
   * See `core/src/main/resources/reference.conf` for the default values.
   */
  private class Conf(val cliconf: CLIConf) {
    // args only from cli
    val input = cliconf.input()
    val output = cliconf.output()

    // load config file
    val fileconf = loadConfig(cliconf.config())
    fileconf.checkValid(ConfigFactory.defaultReference())

    // args
    val inputFormat: String = cliconf.inputFormat.toOption.getOrElse(
      Option(fileconf.getString("input.format")).get
    )
    val inputColumn: String = cliconf.inputColumn.toOption.getOrElse(
      Option(fileconf.getString("input.column")).get
    )
    val inputDelimiter: String = cliconf.inputDelimiter.toOption.getOrElse(
      Option(fileconf.getString("input.delimiter")).get
    )
    val outputFormat: String = cliconf.outputFormat.toOption.getOrElse(
      Option(fileconf.getString("output.format")).get
    )
    val outputColumn: String = cliconf.outputColumn.toOption.getOrElse(
      Option(fileconf.getString("output.column")).get
    )
    val outputDelimiter: String = cliconf.outputDelimiter.toOption.getOrElse(
      Option(fileconf.getString("output.delimiter")).get
    )
    val outputElementDelimiter: String =
      cliconf.outputElementDelimiter.toOption.getOrElse(
        Option(fileconf.getString("output.elementDelimiter")).get
      )
  }

  // todo: check if there is a way to get this list
  private val providedSettings =
    Set("chitra", "sudachiDictCorpus", "rmTemplate", "warc")

  private def loadConfig(nameOrPath: String): Config = {
    if (providedSettings.contains(nameOrPath)) {
      ConfigFactory.load(nameOrPath)
    } else {
      val path = Paths.get(nameOrPath)
      if (!Files.exists(path)) {
        throw new java.nio.file.NoSuchFileException(path.toString)
      }
      ConfigFactory.parseFile(path.toFile).withFallback(ConfigFactory.load())
    }
  }

  /** load documents as a seq of full text. */
  def loadInput(spark: SparkSession, conf: Conf): Dataset[Seq[String]] = {
    import spark.implicits._

    val inputPaths = DocumentIO.formatPathList(conf.input).map(_.toString)
    val docCol = conf.inputColumn

    val rawdf = conf.inputFormat match {
      case "parquet" => {
        spark.read
          .load(inputPaths: _*)
          .select(docCol) // TODO: keep other columns
      }
      case "text" | "txt" => {
        spark.read
          .option("lineSep", conf.inputDelimiter)
          .text(inputPaths: _*)
          .withColumnRenamed("value", docCol)
      }
    }
    rawdf.as[String].map(Seq(_))
  }

  /** save documents in the specified format. */
  def saveOutput(df: Dataset[Seq[String]], conf: Conf): Unit = {
    import df.sparkSession.implicits._
    val delim = conf.outputElementDelimiter
    val dfout = df.map(_.mkString(delim))

    conf.outputFormat match {
      case "parquet" => {
        dfout.toDF(conf.outputColumn).write.save(conf.output.toString)
      }
      case "text" | "txt" => {
        dfout.write
          .option("lineSep", conf.outputDelimiter)
          .text(conf.output.toString)
      }
    }
  }

  def run(spark: SparkSession, conf: Conf): Unit = {
    import spark.implicits._

    // Dataset[Seq[String (paragraph)]]
    val data = loadInput(spark, conf)

    // setup pipeline and apply
    // TODO: keep original non-document column
    val pipeline = Pipeline.fromConfig(conf.fileconf)
    logger.info(s"pipeline: ${pipeline}")
    val processed = pipeline.transform(data)

    // write
    saveOutput(processed, conf)
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(new CLIConf(args))
    val spark =
      SparkSession.builder().appName(this.getClass.getSimpleName).getOrCreate()

    try { run(spark, conf) }
    finally { spark.stop() }
  }
}
