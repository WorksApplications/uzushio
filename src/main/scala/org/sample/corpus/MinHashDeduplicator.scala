package org.sample.corpus

import java.nio.file.{Path, Paths}
import org.rogach.scallop.ScallopConf

import org.apache.spark.sql.{SparkSession, Dataset, DataFrame, Row}
import org.apache.spark.sql.functions._

import org.apache.spark.ml.{Pipeline}
import org.apache.spark.ml.feature.NGram
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.feature.MinHashLSH

import org.apache.spark.graphx.{Graph, Edge}

object MinHashDeduplicator {
  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[Path]](required = true)
    val output = opt[Path](default = Some(Paths.get("./out")))

    val mode = opt[String](default = Some("C"))
    val ngram = opt[Int](default = Some(5))
    val minDF = opt[Int](default = Some(2))
    val numFeatures = opt[Int](default = Some(1000))
    val numTables = opt[Int](default = Some(5))

    val joinThr = opt[Double](default = Some(0.2))
    verify()
  }

  val featureCol = "feature"
  val ccCol = "cc"
  val didCol = DocumentIO.idxCol
  val distCol = "JaccardDistance"

  def run(spark: SparkSession, conf: Conf): Unit = {
    import spark.implicits._

    val documents =
      DocumentIO.addIndex(DocumentIO.loadRawDocuments(spark, conf.input()))

    // preprocess
    val pipe = setupPipeline(
      mode = conf.mode(),
      numGrams = conf.ngram(),
      minDF = conf.minDF(),
      numFeatures = conf.numFeatures()
    )
    val model_pipe = pipe.fit(documents)
    val preprocessed = model_pipe.transform(documents)

    // list similar pairs
    val mh = new MinHashLSH()
      .setNumHashTables(conf.numTables())
      .setInputCol(featureCol)
      .setOutputCol("hashes")
    val model_mh = mh.fit(preprocessed)

    val docPairs =
      model_mh
        .approxSimilarityJoin(
          preprocessed,
          preprocessed,
          conf.joinThr(),
          distCol
        )
        // rm self join and dup pair
        .filter(col(s"datasetA.${didCol}") < col(s"datasetB.${didCol}"))

    // construct graph and clc connected components
    val g = Graph.fromEdges(
      docPairs
        .select(expr(s"datasetA.${didCol}"), expr(s"datasetB.${didCol}"))
        .rdd
        .map(row => Edge(row.getLong(0), row.getLong(1), "e")),
      "g"
    )
    val cc = g.connectedComponents().vertices.toDF(didCol, ccCol)

    // rm duplicated documents
    val joined = documents.join(cc, Seq(didCol), "left")
    val dedup =
      joined.filter((isnull(col(ccCol))) || (col(ccCol) === col(didCol)))
    val dup =
      joined.filter((!isnull(col(ccCol))) && (col(ccCol) !== col(didCol)))

    DocumentIO.saveRawDocuments(dedup, conf.output().resolve("dedup"))
    DocumentIO.saveRawDocuments(dup, conf.output().resolve("dup"))
  }

  /* Returns a spark.ml pipeline for the preprocess.
   *
   * converts documents into n-gram occurrence flag vector
   */
  def setupPipeline(
      mode: String = "C",
      numGrams: Int = 5,
      minDF: Int = 2,
      numFeatures: Int = 1000,
      inputCol: String = DocumentIO.docCol,
      outputCol: String = featureCol
  ) = {
    val tok = new SudachiTokenizer()
      .setInputCol(inputCol)
      .setOutputCol("tokens")
      .setSplitMode(mode)
    val ngram =
      new NGram().setInputCol("tokens").setOutputCol("ngrams").setN(numGrams)
    val count = new CountVectorizer()
      .setInputCol("ngrams")
      .setOutputCol(outputCol)
      .setMinDF(minDF)
      .setVocabSize(numFeatures)
      .setBinary(true)

    new Pipeline().setStages(Array(tok, ngram, count))
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val spark =
      SparkSession.builder().appName("MinHashDeduplicator").getOrCreate()

    try { run(spark, conf) }
    finally { spark.stop() }
  }
}
