package com.worksap.nlp.uzushio

import java.nio.file.{Path, Paths, Files}
import org.rogach.scallop.ScallopConf
import org.apache.log4j.{LogManager}
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{NGram}
import org.apache.spark.graphx.{Graph, Edge}

object MinHashDeduplicator {
  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[Path]](required = true)
    val output = opt[Path](default = Some(Paths.get("./out")))

    val mode = opt[String](
      default = Some("C"),
      validate = (m => m.length == 1 && "aAbBcC".contains(m))
    )
    val ngram = opt[Int](default = Some(5), validate = (_ > 0))
    val minhashB = opt[Int](default = Some(15), validate = (_ > 0))
    val minhashR = opt[Int](default = Some(150), validate = (_ > 0))

    val jaccardThr =
      opt[Double](default = Some(0.8), validate = { v => (0 <= v) && (v <= 1) })

    val skipEditsim = opt[Boolean]()
    val editsimThr =
      opt[Double](default = Some(0.8), validate = { v => (0 <= v) && (v <= 1) })

    verify()
  }

  // column name constants
  val docCol = DocumentIO.docCol
  val didCol = DocumentIO.idxCol
  val tokCol = "tokens"
  val ngramCol = "ngram"
  val featureCol = "feature"
  val minhashCol = "minhash"
  val jaccardCol = "jaccard"
  val editSimCol = "editsim"
  val ccCol = "cc"

  def run(spark: SparkSession, conf: Conf): Unit = {
    import spark.implicits._
    val logger = LogManager.getLogger("MinHashDeduplicator")
    val outRoot = conf.output()

    val docPath = outRoot.resolve("doc").toString
    if (!Files.exists(Paths.get(docPath))) {
      val documents =
        DocumentIO.addIndex(DocumentIO.loadRawDocuments(spark, conf.input()))

      logger.info("save indexed documents")
      documents.write.save(docPath)
    }
    val documents = spark.read.load(docPath)

    /* preprocess documents into a set of n-gram hashes */
    val prepPath = outRoot.resolve("prep").toString
    if (!Files.exists(Paths.get(prepPath))) {
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

      logger.info("save preprocessed data")
      preprocessed.write.save(prepPath)
    }
    val preprocessed = spark.read.load(prepPath)

    /* List up document pairs that possibly have large Jaccard index using MinHashLSH.
     *
     * Split minhashes into r groups, each has b values.
     * Document pair is selected if all of b hashes are same for any of r hash group.
     * The probability of a pair with a similarity s is selected equals to (1-(1-s^b)^r).
     *
     * we omit token and hash column during this due to the space limitation.
     */
    val candPath = outRoot.resolve(s"${minhashCol}").toString
    if (!Files.exists(Paths.get(candPath))) {
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
          .transformBucket(i, preprocessed.select(didCol, featureCol))
          .select(didCol, minhashCol)
          .groupByKey(row => { row.getAs[Seq[Long]](minhashCol) })
          .flatMapGroups((k, iter) => {
            iter.toSeq
              .sortBy(r => r.getAs[Long](didCol))
              .combinations(2) // combinations keeps order if items are unique
              .map(arr =>
                Tuple2(arr(0).getAs[Long](didCol), arr(1).getAs[Long](didCol))
              )
          })
          .rdd
      })
      val candidates = spark.sparkContext
        .union(matchRdds)
        .toDF(s"${didCol}1", s"${didCol}2")
        .dropDuplicates(s"${didCol}1", s"${didCol}2")

      /* save and load to balance partition size */
      logger.info("save candidate matches by minhash")
      candidates.write.option("maxRecordsPerFile", 10000 * 1000).save(candPath)
    }

    /* filter out pairs by jaccard index */
    val jaccardPath = outRoot.resolve(jaccardCol).toString
    if (!Files.exists(Paths.get(jaccardPath))) {
      val candidates = spark.read.load(candPath)
      val candWithFeature = candidates
        .join(
          preprocessed.select(didCol, featureCol),
          candidates.col(s"${didCol}1").equalTo(preprocessed.col(didCol))
        )
        .drop(didCol)
        .withColumnRenamed(featureCol, s"${featureCol}1")
        .join(
          preprocessed.select(didCol, featureCol),
          candidates.col(s"${didCol}2").equalTo(preprocessed.col(didCol))
        )
        .drop(didCol)
        .withColumnRenamed(featureCol, s"${featureCol}2")

      val clcJaccardIndex = udf { (seq1: Seq[Long], seq2: Seq[Long]) =>
        jaccardIndex(seq1, seq2)
      }
      val jaccard = candWithFeature
        .withColumn(
          jaccardCol,
          clcJaccardIndex(col(s"${featureCol}1"), col(s"${featureCol}2"))
        )
        .select(s"${didCol}1", s"${didCol}2", jaccardCol)

      logger.info("save jaccard index")
      jaccard.write.save(jaccardPath)
    }

    /* filter out pairs by actual edit similarity */
    val editSimPath = outRoot.resolve(editSimCol).toString
    if (!conf.skipEditsim() && !Files.exists(Paths.get(editSimPath))) {
      /* save and load to balance partition size */
      val tmpPath = outRoot.resolve(s"tmp_${editSimCol}").toString
      spark.read
        .load(jaccardPath)
        .filter(col(jaccardCol) >= conf.jaccardThr())
        .write
        .save(tmpPath)
      val candidates = spark.read.load(tmpPath)

      val candWithFeature = candidates
        .join(
          preprocessed.select(didCol, tokCol),
          candidates.col(s"${didCol}1").equalTo(preprocessed.col(didCol))
        )
        .drop(didCol)
        .withColumnRenamed(tokCol, s"${tokCol}1")
        .join(
          preprocessed.select(didCol, tokCol),
          candidates.col(s"${didCol}2").equalTo(preprocessed.col(didCol))
        )
        .drop(didCol)
        .withColumnRenamed(tokCol, s"${tokCol}2")

      val clcEditSim = udf { (seq1: Seq[String], seq2: Seq[String]) =>
        editSimilarity(seq1, seq2)
      }
      val editSim = candWithFeature
        .withColumn(
          editSimCol,
          clcEditSim(col(s"${tokCol}1"), col(s"${tokCol}2"))
        )
        .select(s"${didCol}1", s"${didCol}2", editSimCol)

      logger.info("save edit similarity")
      editSim.write.save(editSimPath)
    }

    /* Run graph connected components algorithm to merge similar groups.
     * ccCol will contain the smallest did in the connected documents.
     */
    val ccPath = outRoot.resolve(ccCol).toString
    if (!Files.exists(Paths.get(ccPath))) {
      val (matchPath, thrCol, thrVal) =
        if (conf.skipEditsim())
          (jaccardPath, jaccardCol, conf.jaccardThr())
        else
          (editSimPath, editSimCol, conf.editsimThr())

      /* save and load to balance partition size */
      val tmpPath = outRoot.resolve(s"tmp_${ccCol}").toString
      spark.read
        .load(matchPath)
        .filter(col(thrCol) >= thrVal)
        .write
        .save(tmpPath)
      val matches = spark.read.load(tmpPath)

      val edgeRdd = matches.rdd.map(row =>
        Edge(row.getAs[Long](s"${didCol}1"), row.getAs[Long](s"${didCol}2"), "")
      )
      val graph = Graph.fromEdges(edgeRdd, "")
      val cc = graph.connectedComponents().vertices.toDF(didCol, ccCol)

      logger.info("save connected components")
      cc.write.save(ccPath)
    }
    val cc = spark.read.load(ccPath)

    /* rm duplicated documents */
    logger.info("save result")
    val docsWithCC = documents.join(cc, Seq(didCol), "left")
    val dupDocs =
      docsWithCC.filter((!isnull(col(ccCol))) && (col(ccCol) =!= col(didCol)))
    val dedupDocs =
      docsWithCC.filter((isnull(col(ccCol))) || (col(ccCol) === col(didCol)))

    DocumentIO.saveRawDocuments(dedupDocs, outRoot.resolve("dedup"))
    DocumentIO.saveRawDocuments(dupDocs, outRoot.resolve("dup"))
  }

  /* Jaccard index of two sets */
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

    val p = ArrayBuffer.range(0, l1 + 1)
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
    val conf = new Conf(args.toIndexedSeq)
    val spark =
      SparkSession.builder().appName("MinHashDeduplicator").getOrCreate()

    try { run(spark, conf) }
    finally { spark.stop() }
  }
}
