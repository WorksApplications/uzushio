package org.sample.corpus

import java.nio.file.{Path, Paths, Files}
import org.rogach.scallop.ScallopConf
import org.apache.log4j.{LogManager}
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.{NGram}
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
    val minhashB = opt[Int](default = Some(20), validate = (_ > 0))
    val minhashR = opt[Int](default = Some(450), validate = (_ > 0))
    val simThr =
      opt[Double](default = Some(0.8), validate = { v => (0 <= v) && (v <= 1) })

    val pairPartition = opt[Int](default = Some(100), validate = (_ > 0))
    verify()
  }

  val docCol = DocumentIO.docCol
  val didCol = DocumentIO.idxCol
  val tokCol = "tokens"
  val ngramCol = "ngram"
  val featureCol = "feature"
  val minhashCol = "minhash"
  val ccCol = "cc"

  def run(spark: SparkSession, conf: Conf): Unit = {
    import spark.implicits._
    val logger = LogManager.getLogger("MinHashDeduplicator")
    val outRoot = conf.output()

    val documents =
      DocumentIO.addIndex(DocumentIO.loadRawDocuments(spark, conf.input()))

    /* preprocess documents into a set of n-gram hashes */
    val prepPath = outRoot.resolve("prep").toString
    val preprocessed = if (Files.exists(Paths.get(prepPath))) {
      spark.read.load(prepPath).cache()
    } else {
      val pipeline = new Pipeline()
        .setStages(
          Array(
            new SudachiTokenizer()
              .setInputCol(docCol)
              .setOutputCol(tokCol)
              .setSplitMode(conf.mode()),
            new NGram()
              .setInputCol(tokCol)
              .setOutputCol(ngramCol)
              .setN(conf.ngram()),
            new TokenHasher().setInputCol(ngramCol).setOutputCol(featureCol)
          )
        )
        // those transformers does not require actual data to fit
        .fit(documents.limit(1))

      val preprocessed = pipeline
        .transform(documents)
        .select(didCol, tokCol, featureCol)
        .cache()

      if (conf.saveStats()) {
        logger.info("save preprocessed data")
        preprocessed.write.save(prepPath)
      }
      preprocessed
    }

    /* List up document pairs that possibly have large Jaccard index using MinHashLSH.
     *
     * Split minhashes into r groups, each has b values.
     * Document pair is selected if all of b hashes are same for any of r hash group.
     * The probability of a pair with a similarity s is selected equals to (1-(1-s^b)^r).
     */
    val b = conf.minhashB()
    val r = conf.minhashR()
    val minhashModel = new MinHash()
      .setInputCol(featureCol)
      .setOutputCol(minhashCol)
      .setB(b)
      .setR(r)
      .fit(preprocessed.limit(1))

    val matchRdds = Range(0, r).map(i => {
      minhashModel
        .transformBucket(i, preprocessed)
        .groupByKey(row => { row.getAs[Seq[Long]](minhashCol) })
        .flatMapGroups((k, iter) => {
          iter.toSeq
            .sortBy(r => r.getAs[Long](didCol))
            .combinations(2)
            .map(arr =>
              Tuple6(
                arr(0).getAs[Long](didCol),
                arr(1).getAs[Long](didCol),
                arr(0).getAs[Seq[String]](tokCol),
                arr(1).getAs[Seq[String]](tokCol),
                arr(0).getAs[Seq[Long]](featureCol),
                arr(1).getAs[Seq[Long]](featureCol)
              )
            )
        })
        .rdd
    })
    val candidates = spark.sparkContext
      .union(matchRdds)
      .toDF(
        s"${didCol}1",
        s"${didCol}2",
        s"${tokCol}1",
        s"${tokCol}2",
        s"${featureCol}1",
        s"${featureCol}2"
      )
      .dropDuplicates(s"${didCol}1", s"${didCol}2")

    /* save and load to balance partition size */
    val tmpdir = outRoot.resolve("candidates").toString
    candidates.write.option("maxRecordsPerFile", 10000).save(tmpdir)
    val matches = spark.read.load(tmpdir).repartition(conf.pairPartition())

    /* filter out pairs by jaccard index */
    val clcJaccardIndex = udf { (seq1: Seq[Long], seq2: Seq[Long]) =>
      jaccardIndex(seq1, seq2)
    }
    val jaccard = matches
      .withColumn(
        "jaccardIndex",
        clcJaccardIndex(col(s"${featureCol}1"), col(s"${featureCol}2"))
      )
      .select(
        s"${didCol}1",
        s"${didCol}2",
        s"${tokCol}1",
        s"${tokCol}2",
        "jaccardIndex"
      )
      .cache()

    if (conf.saveStats()) {
      logger.info("save jaccard index")
      jaccard
        .select(s"${didCol}1", s"${didCol}2", "jaccardIndex")
        .write
        .save(outRoot.resolve("jaccard").toString)
    }

    /* filter out pairs by actual edit similarity */
    val clcEditSim = udf { (seq1: Seq[String], seq2: Seq[String]) =>
      editSimilarity(seq1, seq2)
    }
    val editSim = jaccard
      .filter(col("jaccardIndex") >= conf.simThr())
      .withColumn("editSim", clcEditSim(col(s"${tokCol}1"), col(s"${tokCol}2")))
      .select(s"${didCol}1", s"${didCol}2", "editSim")
      .cache()

    if (conf.saveStats()) {
      logger.info("save match")
      editSim.write.save(outRoot.resolve("match").toString)
    }

    /* Use graph connected components algorithm to merge similar groups */
    val edgeRdd = editSim
      .filter(col("editSim") >= conf.simThr())
      .rdd
      .map(row =>
        Edge(row.getAs[Long](s"${didCol}1"), row.getAs[Long](s"${didCol}2"), "")
      )
      .cache()
    val graph = Graph.fromEdges(edgeRdd, "")
    val cc = graph.connectedComponents().vertices.toDF(didCol, ccCol)

    if (conf.saveStats()) {
      logger.info("save connection group id")
      documents
        .join(cc, Seq(didCol), "inner")
        .select(ccCol, docCol)
        .write
        .save(outRoot.resolve("cc").toString)
    }

    /* rm duplicated documents */
    logger.info("save result")
    val docsWithCC = documents.join(cc, Seq(didCol), "left")
    val dupDocs =
      docsWithCC.filter((!isnull(col(ccCol))) && (col(ccCol) !== col(didCol)))
    val dedupDocs =
      docsWithCC.filter((isnull(col(ccCol))) || (col(ccCol) === col(didCol)))

    DocumentIO.saveRawDocuments(dedupDocs, outRoot.resolve("dedup"))
    DocumentIO.saveRawDocuments(dupDocs, outRoot.resolve("dup"))
  }

  /* Jaccard index of two set */
  def jaccardIndex[T](s1: Seq[T], s2: Seq[T]): Double = {
    val set1 = s1.toSet
    val set2 = s2.toSet
    (set1 & set2).size.toDouble / (set1 | set2).size.toDouble
  }

  /* util functions */
  def maxInt(nums: Int*): Int = nums.max
  def minInt(nums: Int*): Int = nums.min
  def delta[T](left: T, right: T): Int = { if (left == right) 0 else 1 }

  /* levenshtein distance of two sequences */
  def levenshteinDistance[T](s1: Seq[T], s2: Seq[T]): Int = {
    val l1 = s1.length
    val l2 = s2.length
    if (l1 == 0) return l2
    if (l2 == 0) return l1

    var p = ArrayBuffer.range(0, l1 + 1)
    var lastDiag: Int = 0
    for (i2 <- 1 to l2) {
      p(0) = i2
      lastDiag = i2 - 1
      for (i1 <- 1 to l1) {
        val tmp = p(i1)
        p(i1) = minInt(
          p(i1) + 1,
          p(i1 - 1) + 1,
          lastDiag + delta(s1(i1 - 1), s2(i2 - 1))
        )
        lastDiag = tmp
      }
    }
    p(l1)
  }

  /* edit similarity of two sequences */
  def editSimilarity[T](s1: Seq[T], s2: Seq[T]): Double = {
    1.0 - levenshteinDistance(s1, s2).toDouble / maxInt(
      s1.length,
      s2.length
    ).toDouble
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val spark =
      SparkSession.builder().appName("MinHashDeduplicator").getOrCreate()

    try { run(spark, conf) }
    finally { spark.stop() }
  }
}
