package org.sample.corpus

import java.nio.file.{Path, Paths}
import org.rogach.scallop.ScallopConf

import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}
import org.apache.spark.sql.functions.{expr, monotonically_increasing_id}

object DocumentIO {
  val idxCol = "documentId"
  val docCol = "document"

  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    // args `--input ./hoge.md ./*.txt` will be parsed like
    //   List(./hoge.md, ./fuga.txt, ./piyo.txt)
    val input = opt[List[Path]](required = true)
    val output = opt[Path](default = Some(Paths.get("./out")))
    verify()
  }

  def run(spark: SparkSession, conf: Conf): Unit = {
    val docs = loadRawDocuments(spark, conf.input())
    val docWithIdx = addIndex(docs)
    saveIndexedDocuments(spark, docWithIdx, idxCol, docCol, conf.output())
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val spark = SparkSession.builder().appName("DocumentIO").getOrCreate()

    try { run(spark, conf) }
    finally { spark.stop() }
  }

  def addIndex(
      dataframe: Dataset[_],
      idxColName: String = idxCol
  ): DataFrame = {
    // add index column
    dataframe.withColumn(idxColName, monotonically_increasing_id)
  }

  def formatPathList(paths: Seq[Path]): Seq[Path] = {
    paths.distinct.sorted
  }

  def saveRawDocuments(
      spark: SparkSession,
      documents: Dataset[String],
      output: Path,
      sep: String = "\n\n\n"
  ): Unit = {
    documents.write.option("lineSep", sep).text(output.toString)
  }

  def loadRawDocuments(
      spark: SparkSession,
      input: Seq[Path],
      sep: String = "\n\n\n"
  ): Dataset[String] = {
    // load document data.
    //
    // Assumes each input file contains multiple documents,
    // and they are separated by `sep` (by default two empty lines).
    import spark.implicits._

    val paths = formatPathList(input).map(_.toString)
    spark.read
      .option("lineSep", sep)
      .textFile(paths: _*)
      .filter(_.trim != "")
      .select(expr(s"value as ${docCol}"))
      .as[String]
  }

  def saveIndexedDocuments(
      spark: SparkSession,
      dataframe: DataFrame,
      idxColName: String,
      docColName: String,
      output: Path,
      format: String = "parquet"
  ): Unit = {
    import spark.implicits._

    val data = dataframe
      .select(
        expr(s"${idxColName} as ${idxCol}"),
        expr(s"${docColName} as ${docCol}")
      )
      .as[(Long, String)]

    data.write.format(format).save(output.toString)
  }

  def loadIndexedDocuments(
      spark: SparkSession,
      input: Seq[Path],
      format: String = "parquet"
  ): Dataset[(Long, String)] = {
    // Assume the schema of files is same to `saveIndexedDocuments` output
    import spark.implicits._

    val paths = formatPathList(input).map(_.toString)
    spark.read.format(format).load(paths: _*).as[(Long, String)]
  }
}
