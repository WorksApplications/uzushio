package org.sample.corpus

import java.nio.file.{Path, Paths}
import org.rogach.scallop.ScallopConf

import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}
import org.apache.spark.sql.functions.{expr}
import org.apache.spark.ml.{Pipeline}
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.linalg.{Vector, Vectors}

object TfIdfVectorizer {
  val featureCol = "feature"

  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[Path]](required = true)
    val output = opt[Path](default = Some(Paths.get("./out")))

    val numTfFeature = opt[Int](default = Some(1000))
    val mode = opt[String](default = Some("c"))
    verify()
  }

  def run(spark: SparkSession, conf: Conf): Unit = {
    val indexedDoc = DocumentIO.loadIndexedDocuments(spark, conf.input())

    val featureColName = "idf"
    val pipeline = setupPipeline(
      conf.mode(),
      nTfFeature = conf.numTfFeature(),
      DocumentIO.docCol,
      featureColName
    )

    val model = pipeline.fit(indexedDoc)
    // sudachiTokenizer transformer is not writable now
    // model.write.overwrite().save(conf.output().toString + "/model")

    val processed = model.transform(indexedDoc)

    saveFeatureVector(
      spark,
      processed,
      DocumentIO.idxCol,
      featureColName,
      conf.output()
    )
  }

  def setupPipeline(
      mode: String = "C",
      nTfFeature: Int = 1000,
      inputCol: String = "document",
      outputCol: String = "idf"
  ) = {
    val tokenizer =
      new SudachiTokenizer()
        .setInputCol(inputCol)
        .setOutputCol("tokens")
        .setSplitMode(mode)
    val hashingTF = new HashingTF()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("tf")
      .setNumFeatures(nTfFeature)
    val idf =
      new IDF().setInputCol(hashingTF.getOutputCol).setOutputCol(outputCol)

    new Pipeline().setStages(Array(tokenizer, hashingTF, idf))
  }

  def saveFeatureVector(
      spark: SparkSession,
      dataframe: DataFrame,
      idxColName: String,
      featureColName: String,
      output: Path,
      format: String = "parquet"
  ): Unit = {
    val data = dataframe
      .select(
        expr(s"${idxColName} as ${DocumentIO.idxCol}"),
        expr(s"${featureColName} as ${featureCol}")
      )

    data.write.format(format).save(output.toString)
  }

  def loadFeatureVector(
      spark: SparkSession,
      input: Seq[Path],
      format: String = "parquet"
  ): DataFrame = {
    val paths = DocumentIO.formatPathList(input).map(_.toString)
    spark.read.format(format).load(paths: _*)
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val spark = SparkSession.builder().appName("TfIdf").getOrCreate()

    try { run(spark, conf) }
    finally { spark.stop() }
  }
}
