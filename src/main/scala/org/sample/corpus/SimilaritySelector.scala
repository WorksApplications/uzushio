package org.sample.corpus

import java.nio.file.{Path, Paths}
import org.rogach.scallop.ScallopConf

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{
  BucketedRandomProjectionLSH,
  BucketedRandomProjectionLSHModel
}
import org.apache.spark.ml.linalg.{Vector}

object SimilaritySelector {
  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    val corpusDoc = opt[List[Path]](required = true)
    val seedDoc = opt[List[Path]](required = true)
    val feature = opt[List[Path]](required = true)
    val output = opt[Path](default = Some(Paths.get("./out")))

    val bucketLength = opt[Double](default = Some(2))
    val numTables = opt[Int](default = Some(3))

    val numSamples = opt[Int](default = Some(3))
    verify()
  }

  def run(spark: SparkSession, conf: Conf): Unit = {
    // load documents and feature vec
    val featureVecs = Vectorizer.loadFeatureVector(spark, conf.feature())
    val corpus = DocumentIO
      .loadIndexedDocuments(spark, conf.corpusDoc())
      .join(featureVecs, DocumentIO.idxCol)
    val seeds = DocumentIO
      .loadIndexedDocuments(spark, conf.seedDoc())
      .join(featureVecs, DocumentIO.idxCol)

    // setup model
    val blen = conf.bucketLength()
    val ntable = conf.numTables()
    val outCol = "hashes"

    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(blen)
      .setNumHashTables(ntable)
      .setInputCol(Vectorizer.featureCol)
      .setOutputCol(outCol)
    val model = brp.fit(corpus)

    // val selected = knnFromSeedSet(model, corpus, seeds, conf.numSamples())
    val selected = knnFromEachSeeds(model, corpus, seeds, conf.numSamples())

    DocumentIO.saveRawDocuments(selected, conf.output())
  }

  def knnFromSeedSet(
      model: BucketedRandomProjectionLSHModel,
      corpus: DataFrame,
      seeds: DataFrame,
      numSamples: Int,
      featureCol: String = Vectorizer.featureCol
  ): DataFrame = {
    // select doc based on the similarity to whole seed set

    // avg cossim with seeds equals to cossim with avg of normalized seeds
    val avgSeedNorm = VecOps.div(
      seeds.rdd
        .map(r => VecOps.normalize(r.getAs[Vector](featureCol)))
        .reduce((a, b) => VecOps.add(a, b))
        .compressed,
      seeds.count
    )

    model.approxNearestNeighbors(corpus, avgSeedNorm, numSamples).toDF
  }

  def knnFromEachSeeds(
      model: BucketedRandomProjectionLSHModel,
      corpus: DataFrame,
      seeds: DataFrame,
      numSamples: Int,
      featureCol: String = Vectorizer.featureCol
  ): DataFrame = {
    // select doc based on the similarity to each seed and concat them
    // todo: should use spark functionality to speed up
    seeds
      .select(featureCol)
      .collect()
      .map(row =>
        model
          .approxNearestNeighbors(
            corpus,
            row.getAs[Vector](featureCol),
            numSamples
          )
          .toDF
      )
      .reduce((df1, df2) => df1.union(df2))
      .distinct
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val spark =
      SparkSession.builder().appName("SimilaritySelector").getOrCreate()

    try { run(spark, conf) }
    finally { spark.stop() }
  }
}
