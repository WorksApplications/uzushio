package org.sample.corpus

import java.nio.file.{Path, Paths}
import org.rogach.scallop.ScallopConf

import org.apache.spark.sql.{SparkSession, Dataset, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.feature.{NGram, CountVectorizer, MinHashLSH}
import org.apache.spark.graphx.{Graph, Edge}

object MinHashDeduplicator {
  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[Path]](required = true)
    val output = opt[Path](default = Some(Paths.get("./out")))
    val saveStats = opt[Boolean]()

    val mode = opt[String](default = Some("C"))
    val ngram = opt[Int](default = Some(5))
    val numTables = opt[Int](default = Some(5))
    val joinThr = opt[Double](default = Some(0.2))
    verify()
  }

  val docCol = DocumentIO.docCol
  val didCol = DocumentIO.idxCol
  val tokCol = "tokens"
  val featureCol = "feature"
  val distCol = "minhashDist"
  val ccCol = "cc"

  def run(spark: SparkSession, conf: Conf): Unit = {
    import spark.implicits._

    val documents =
      DocumentIO.addIndex(DocumentIO.loadRawDocuments(spark, conf.input()))
    println(s"num documents total: ${documents.count()}")

    // preprocess
    val pipe = setupPipeline(mode = conf.mode(), numGrams = conf.ngram())
    val model_pipe = pipe.fit(documents)
    val preprocessed = model_pipe.transform(documents)

    /* Separate documents with empty ngram set, that minhash-lsh cannot handle.
     * They have no common ngrams thus unique enough to skip.
     */
    val uniq_docs = preprocessed
      .filter(row => row.getAs[Vector](featureCol).numNonzeros == 0)
    val cand_docs = preprocessed
      .filter(row => row.getAs[Vector](featureCol).numNonzeros != 0)
      .select(didCol, featureCol)

    // costruct and apply LSH
    val mh = new MinHashLSH()
      .setNumHashTables(conf.numTables())
      .setInputCol(featureCol)
      .setOutputCol("hashes")
    val model_mh = mh.fit(cand_docs)

    val candPairs = model_mh
      .approxSimilarityJoin(cand_docs, cand_docs, conf.joinThr(), distCol)

    val simPairs = candPairs
      // rm self join and duplicated pairs
      .filter(col(s"datasetA.${didCol}") < col(s"datasetB.${didCol}"))

    // construct graph and clc connected components
    val graph = Graph.fromEdges(
      simPairs
        .select(expr(s"datasetA.${didCol}"), expr(s"datasetB.${didCol}"))
        .rdd
        .map(row => Edge(row.getLong(0), row.getLong(1), "")),
      ""
    )
    val cc = graph.connectedComponents().vertices.toDF(didCol, ccCol)

    // save duplication data for the analysis
    if (conf.saveStats()) {
      documents
        .join(cc, Seq(didCol), "inner")
        .select(ccCol, DocumentIO.docCol)
        .write
        .save(conf.output().resolve("stats").toString)
    }

    // rm duplicated documents (take documents with smallest id in the CC)
    val docs_withCC = documents.join(cc, Seq(didCol), "left")
    val dup_docs =
      docs_withCC.filter((!isnull(col(ccCol))) && (col(ccCol) !== col(didCol)))
    val rest_docs =
      docs_withCC.filter((isnull(col(ccCol))) || (col(ccCol) === col(didCol)))

    val dedup_docs =
      rest_docs.select(col(docCol)).union(uniq_docs.select(docCol))
    println(s"num documents duplicated: ${dup_docs.count()}")
    println(s"num documents kept: ${dedup_docs.count()}")

    DocumentIO.saveRawDocuments(dedup_docs, conf.output().resolve("dedup"))
    DocumentIO.saveRawDocuments(dup_docs, conf.output().resolve("dup"))
  }

  /* Returns a spark.ml pipeline for the preprocess.
   *
   * converts documents into n-gram occurrence flag vector
   */
  def setupPipeline(
      mode: String = "C",
      numGrams: Int = 5,
      minDF: Int = 2,
      inputCol: String = docCol,
      outputCol: String = featureCol
  ) = {
    val tok = new SudachiTokenizer()
      .setInputCol(inputCol)
      .setOutputCol(tokCol)
      .setSplitMode(mode)
    val ngram =
      new NGram().setInputCol(tokCol).setOutputCol("ngrams").setN(numGrams)
    val count = new CountVectorizer()
      .setInputCol("ngrams")
      .setOutputCol(outputCol)
      .setMinDF(minDF)
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
