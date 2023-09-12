package com.worksap.nlp.uzushio

import java.nio.file.{Path, Paths}
import org.rogach.scallop.ScallopConf

import org.apache.spark.sql.{SparkSession, DataFrame}
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
    saveIndexedDocuments(docWithIdx, conf.output())
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val spark = SparkSession.builder().appName("DocumentIO").getOrCreate()

    try { run(spark, conf) }
    finally { spark.stop() }
  }

  def addIndex(
      dataframe: DataFrame,
      idxColName: String = idxCol
  ): DataFrame = {
    // add index column
    dataframe.withColumn(idxColName, monotonically_increasing_id)
  }

  def formatPathList(paths: Seq[Path]): Seq[Path] = {
    // align list to fix the order of file load (todo: check if necessary)
    paths.distinct.sorted
  }

  def saveRawDocuments(
      documents: DataFrame,
      output: Path,
      docCol: String = docCol,
      sep: String = "\n\n"
  ): Unit = {
    documents.select(docCol).write.option("lineSep", sep).text(output.toString)
  }

  def loadRawDocuments(
      spark: SparkSession,
      input: Seq[Path],
      sep: String = "\n\n"
  ): DataFrame = {
    // load document data.
    //
    // Assumes each input file contains multiple documents,
    // and they are separated by `sep` (by default two empty lines).
    val paths = formatPathList(input).map(_.toString)
    spark.read
      .option("lineSep", sep)
      .text(paths: _*)
      .filter(r => r.getAs[String](0).trim != "")
      .select(expr(s"value as ${docCol}"))
  }

  def saveIndexedDocuments(
      dataframe: DataFrame,
      output: Path,
      idxColName: String = idxCol,
      docColName: String = docCol,
      format: String = "parquet"
  ): Unit = {
    val data = dataframe.select(
      expr(s"${idxColName} as ${idxCol}"),
      expr(s"${docColName} as ${docCol}")
    )

    data.write.format(format).save(output.toString)
  }

  def loadIndexedDocuments(
      spark: SparkSession,
      input: Seq[Path],
      format: String = "parquet"
  ): DataFrame = {
    // Assume the schema of files is same to `saveIndexedDocuments` output
    val paths = formatPathList(input).map(_.toString)
    spark.read.format(format).load(paths: _*)
  }
}
