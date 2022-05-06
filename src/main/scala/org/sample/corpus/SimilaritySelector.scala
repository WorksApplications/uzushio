package org.sample.corpus

import java.nio.file.{Path, Paths}
import org.rogach.scallop.ScallopConf

import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.{Vector, Vectors}

object SimilaritySelector {
  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    val corpusDoc = opt[List[Path]](required = true)
    val seedDoc = opt[List[Path]](required = true)
    val feature = opt[List[Path]](required = true)
    val output = opt[Path](default = Some(Paths.get("./out")))

    val bucketLength = opt[Double](default = Some(2))
    val numTables = opt[Int](default = Some(3))

    val numSamples = opt[Int](default = Some(20))
    verify()
  }

  def run(spark: SparkSession, conf: Conf): Unit = {
    import spark.implicits._

    // load documents and feature vec
    val featureVecs = TfIdfVectorizer.loadFeatureVector(spark, conf.feature())
    val corpus = DocumentIO
      .loadIndexedDocuments(spark, conf.corpusDoc())
      .join(featureVecs, DocumentIO.idxCol)
    val seed = DocumentIO
      .loadIndexedDocuments(spark, conf.seedDoc())
      .join(featureVecs, DocumentIO.idxCol)

    // setup model
    val blen = conf.bucketLength()
    val ntable = conf.numTables()
    val outCol = "hashes"

    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(blen)
      .setNumHashTables(ntable)
      .setInputCol(TfIdfVectorizer.featureCol)
      .setOutputCol(outCol)
    val model = brp.fit(corpus)

    val key = avgNorm(seed)
    val selected = model.approxNearestNeighbors(corpus, key, conf.numSamples())

    DocumentIO.saveRawDocuments(
      spark,
      selected.select(DocumentIO.docCol).as[String],
      conf.output()
    )
  }

  def avgNorm(
      seed: DataFrame,
      featureCol: String = TfIdfVectorizer.featureCol
  ): Vector = {
    // avg cossim with seeds equals to cossim with avg of normalized seeds
    VecOps.div(
      seed.rdd
        .map(r => VecOps.normalize(r.getAs[Vector](featureCol)))
        .reduce((a, b) => VecOps.add(a, b))
        .compressed,
      seed.count
    )
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val spark =
      SparkSession.builder().appName("SimilaritySelector").getOrCreate()

    try { run(spark, conf) }
    finally { spark.stop() }
  }
}
