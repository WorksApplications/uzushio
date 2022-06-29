package org.sample.corpus

import java.nio.file.{Path, Paths}
import org.rogach.scallop.ScallopConf
import org.apache.log4j.{LogManager}

import org.apache.spark.sql.{SparkSession, Dataset, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.{NGram, CountVectorizer, MinHashLSH}
import org.apache.spark.graphx.{Graph, Edge}

object MinHashDeduplicator {
  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[Path]](required = true)
    val output = opt[Path](default = Some(Paths.get("./out")))
    val saveStats = opt[Boolean]()

    val mode = opt[String](
      default = Some("C"),
      validate = (m => m.length == 1 && "aAbBcC".contains(m))
    )
    val ngram = opt[Int](default = Some(5), validate = (_ > 0))
    val numVocab = opt[Int](default = Some((1 << 18)), validate = (_ > 0))
    val minDf = opt[Double](default = Some(1.0), validate = (_ >= 0.0))
    val minhashB = opt[Int](default = Some(20), validate = (_ > 0))
    val minhashR = opt[Int](default = Some(450), validate = (_ > 0))
    verify()
  }

  val docCol = DocumentIO.docCol
  val didCol = DocumentIO.idxCol
  val tokCol = "tokens"
  val featureCol = "feature"
  val ccCol = "cc"

  def run(spark: SparkSession, conf: Conf): Unit = {
    import spark.implicits._
    val logger = LogManager.getLogger("MinHashDeduplicator")

    val documents =
      DocumentIO.addIndex(DocumentIO.loadRawDocuments(spark, conf.input()))

    // preprocess
    val preprocessed = setupPipeline(
      mode = conf.mode(),
      ngram = conf.ngram(),
      numVocab = conf.numVocab(),
      minDF = conf.minDf()
    ).fit(documents).transform(documents)

    /* Filter out documents with empty ngram set, that minhash-lsh cannot handle.
     * They have no common ngrams thus unique enough to skip.
     */
    val candDocs = preprocessed
      .filter(row => row.getAs[Vector](featureCol).numNonzeros != 0)
      .select(didCol, featureCol)
    val vocabSize = candDocs.take(1)(0).getAs[Vector](featureCol).size
    logger.info(s"ngram vocab size: ${vocabSize}")

    /* List up document pairs which possibly have large Jaccard similarity using MinHashLSH.
     * The probability of a pair with J-sim s is selected equals to (1-(1-s^b)^r).
     */
    val b = conf.minhashB()
    val r = conf.minhashR()
    val rawhashes = new MinHashLSH()
      .setNumHashTables(b * r)
      .setInputCol(featureCol)
      .setOutputCol("rawhash")
      .fit(candDocs)
      .transform(candDocs)

    // flatten single-value vector returned by minhashLSH.
    val formatHash = udf { rawHash: Array[Vector] =>
      rawHash.map(vec => vec(0))
    }
    val hashes = rawhashes
      .withColumn("hashes", formatHash(col("rawhash")))
      .select(col(didCol), col("hashes"))
      .cache
    logger.info("calcurate minhash done.")

    val matchRdds = Range(0, r * b, b).map(i => {
      // for each of r buckets, documents with same b hashes are match group.
      // list all combination of 2 documents in each group.
      hashes
        .groupByKey(row => { row.getAs[Seq[Double]](1).slice(i, i + b) })
        .flatMapGroups((k, iter) => {
          iter.map(row => row.getLong(0)).toSeq.combinations(2)
        })
        .rdd
    })
    val allMathches = spark.sparkContext
      .union(matchRdds)
      .distinct
      .map(arr => (arr(0), arr(1)))
      .toDF
      .cache
    logger.info("list candidate matches done.")

    // construct graph and clc connected components
    val edgeRdd =
      allMathches.rdd.map(row => Edge(row.getLong(0), row.getLong(1), "")).cache
    val graph = Graph.fromEdges(edgeRdd, "")
    val cc = graph.connectedComponents().vertices.toDF(didCol, ccCol).cache
    logger.info("calcurate connected component done")

    // save duplication data for the analysis
    if (conf.saveStats()) {
      logger.info("save match data")
      documents
        .join(cc, Seq(didCol), "inner")
        .select(ccCol, DocumentIO.docCol)
        .write
        .save(conf.output().resolve("match").toString)
    }

    // rm duplicated documents (take documents with smallest id in the CC)
    logger.info("output result")
    val docsWithCC = documents.join(cc, Seq(didCol), "left").cache
    val dupDocs =
      docsWithCC.filter((!isnull(col(ccCol))) && (col(ccCol) !== col(didCol)))
    val dedupDocs =
      docsWithCC.filter((isnull(col(ccCol))) || (col(ccCol) === col(didCol)))

    DocumentIO.saveRawDocuments(dedupDocs, conf.output().resolve("dedup"))
    DocumentIO.saveRawDocuments(dupDocs, conf.output().resolve("dup"))
  }

  /* Returns a spark.ml pipeline for the preprocess.
   *
   * converts documents into n-gram occurrence flag vector
   */
  def setupPipeline(
      mode: String,
      ngram: Int,
      numVocab: Int,
      minDF: Double,
      inputCol: String = docCol,
      outputCol: String = featureCol
  ) = {
    val tok = new SudachiTokenizer()
      .setInputCol(inputCol)
      .setOutputCol(tokCol)
      .setSplitMode(mode)
    val ngramT =
      new NGram().setInputCol(tokCol).setOutputCol("ngrams").setN(ngram)
    val count = new CountVectorizer()
      .setInputCol("ngrams")
      .setOutputCol(outputCol)
      .setVocabSize(numVocab)
      .setMinDF(minDF)
      .setBinary(true)

    new Pipeline().setStages(Array(tok, ngramT, count))
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val spark =
      SparkSession.builder().appName("MinHashDeduplicator").getOrCreate()

    try { run(spark, conf) }
    finally { spark.stop() }
  }
}
