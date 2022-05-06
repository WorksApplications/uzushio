package org.sample.corpus

import java.nio.file.{Path, Paths}
import org.rogach.scallop.ScallopConf

import org.apache.spark.sql.{SparkSession, Dataset, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{
  BucketedRandomProjectionLSH,
  BucketedRandomProjectionLSHModel
}
import org.apache.spark.ml.linalg.{Vector, Vectors}

object CorpusSelector {
  val keyCol = "type" // column for key to distinguish corpus/seed

  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    val corpusDoc = opt[List[Path]](required = true)
    val seedDoc = opt[List[Path]](required = true)
    val output = opt[Path](default = Some(Paths.get("./out")))

    val mode = opt[String](default = Some("c"))
    val numTfFeatures = opt[Int](default = Some(1000))
    val bucketLength = opt[Double](default = Some(2))
    val numTables = opt[Int](default = Some(3))

    val numSamples = opt[Int](default = Some(20))
    verify()
  }

  def run(spark: SparkSession, conf: Conf): Unit = {
    val outputRoot = conf.output()

    val rawCorpus = DocumentIO.loadRawDocuments(spark, conf.corpusDoc())
    val rawSeed = DocumentIO.loadRawDocuments(spark, conf.seedDoc())

    val indexed = DocumentIO.addIndex(
      unionWithKey(Map("corpus" -> rawCorpus, "seed" -> rawSeed))
    )
    DocumentIO.saveIndexedDocuments(indexed, outputRoot.resolve("indexed"))

    val tfidf = Vectorizer.setupTfIdfPipeline(conf.mode(), conf.numTfFeatures())
    val tfidfModel = tfidf.fit(indexed)
    val vectorized = tfidfModel.transform(indexed)
    Vectorizer.saveFeatureVector(
      vectorized,
      outputRoot.resolve(Vectorizer.featureCol)
    )

    val outCol = "hashes"
    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(conf.bucketLength())
      .setNumHashTables(conf.numTables())
      .setInputCol(Vectorizer.featureCol)
      .setOutputCol(outCol)
    val brpModel = brp.fit(vectorized)

    val selected =
      SimilaritySelector.knnFromSeedSet(
        brpModel,
        vectorized.filter(col(keyCol) === "corpus"),
        vectorized.filter(col(keyCol) === "seed"),
        conf.numSamples()
      )
    DocumentIO.saveRawDocuments(selected, outputRoot.resolve("selected"))
  }

  def unionWithKey(
      key2df: Map[String, DataFrame],
      keyCol: String = keyCol
  ): DataFrame = {
    key2df.keys
      .map(k => key2df(k).withColumn(keyCol, lit(k)))
      .reduce((df1, df2) => df1.union(df2))
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val spark =
      SparkSession.builder().appName("CorpusSelector").getOrCreate()

    try { run(spark, conf) }
    finally { spark.stop() }
  }
}
