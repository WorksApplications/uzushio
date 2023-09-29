package com.worksap.nlp.uzushio.lib.runners

import com.worksap.nlp.uzushio.lib.cleaning.Document
import com.worksap.nlp.uzushio.lib.filters.{DeduplicateDocuments, DeduplicateDocumentsPercentile, DuplicateDocumentsLengthWeighted, LargeFreqParagraphs}
import com.worksap.nlp.uzushio.lib.utils.Paragraphs
import com.worksap.nlp.uzushio.lib.utils.Resources.AutoClosableResource
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{collect_list, octet_length, udf}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.rogach.scallop.ScallopConf

object DedupFilterStatistics {

  def process(spark: SparkSession, args: Args): Unit = {
    import spark.implicits._

    val rawData = spark.read.parquet(args.input(): _*)

    val limited = args.limit.toOption match {
      case None => rawData
      case Some(lim) =>
        val cnt = rawData.count()
        val ratio = lim.toDouble / cnt
        rawData.sample(withReplacement = false, ratio, 0xdeadbeefL)
    }

    val stats = spark.read.parquet(args.stats())

    val textOnly = limited.select("text", "docId").filter(octet_length($"text") > 2)

    val docsWithStats = DeduplicateParagraphs
      .prepareParagraphsForFiltering(textOnly, stats, debug = false)

    val assembledDocs = docsWithStats.groupBy("docId").agg(
      collect_list("text") as "text",
      collect_list("pos") as "pos",
      collect_list("exactFreq") as "exactFreq",
      collect_list("nearFreq") as "nearFreq"
    )

    val metric = extractFilteredMetric(spark, args)

    val withValues = assembledDocs.select(
      metric($"docId", $"text", $"pos", $"exactFreq", $"nearFreq") as "res"
    ).select( // make columns in the same order as FilterStatistics
      $"res._2" as "value",
      $"res._1" as "text"
    )

    withValues.persist().repartitionByRange(args.partitions(), withValues.col("value"))
      .sortWithinPartitions("value").write.option("escape", "\"").mode(SaveMode.Overwrite)
      .csv(args.output())
  }

  def ratioUdfConstructor(
      sample: Double
  )(extractor: Document => Float): UserDefinedFunction = {
    udf {
      (
          docId: String,
          text: Array[String],
          pos: Array[Int],
          exactFreq: Array[Int],
          nearFreq: Array[Int]
      ) =>
        val docParts = DeduplicateParagraphs.collectDocParts(text, pos, exactFreq, nearFreq)
        val doc = Document(paragraphs = docParts, docId = docId)
        val ratio = extractor(doc)
        val docData =
          if (doc.randomDouble < sample) {
            docParts.map(p => Paragraphs.cleanTextBlocksInParagraph(p.text).mkString("<br>")).mkString("<p>")
          } else ""
        (docData, ratio)
    }
  }

  def extractFilteredMetric(
      sparkSession: SparkSession,
      args: Args
  ): UserDefinedFunction = {
    val ftype = args.filter()
    val arg = args.arg()

    // curry makeUdf with sampling probability
    val udfMaker = ratioUdfConstructor(args.examples()) _

    ftype match {
      case "max-near-freq" =>
        udfMaker(doc => doc.aliveParagraphs.map(_.nearFreq).foldRight(0)(_.max(_)))
      case "min-near-freq" =>
        udfMaker(doc => doc.aliveParagraphs.map(_.nearFreq).foldRight(0)(_.min(_)))
      case "duplication-score" =>
        val filter = new DeduplicateDocuments(10)
        udfMaker(doc => filter.computeNearDuplicateTextRatio(doc))
      case "length-weighted" =>
        udfMaker(doc => DuplicateDocumentsLengthWeighted.nearFreqWeight(doc).toFloat)
      case "freq-percentile" =>
        udfMaker(doc => DeduplicateDocumentsPercentile.freqAtPercentile(doc, 0.05f).toFloat)
      case "large-freq-paragraphs" =>
        val filter = new LargeFreqParagraphs(freq = 100)
        udfMaker(doc => filter.markParagraphs(doc.paragraphs.toBuffer).toFloat)
      case _ => throw new IllegalArgumentException(s"unknown metric $ftype")
    }

  }

  class Args(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[String]](required = true)
    val stats = opt[String](required = true)
    val output = opt[String](required = true)
    val filter = opt[String](required = true)
    val limit = opt[Int]()
    val arg = opt[String](default = Some(""))
    val arg2 = opt[String]()
    val partitions = opt[Int](default = Some(10))
    val examples = opt[Double](default = Some(0.001))
    val master = opt[String]()
    verify()
  }

  def main(args: Array[String]): Unit = {
    val opts = new Args(args)
    val conf = SparkSession.builder()
    opts.master.foreach(m => conf.master(m))

    conf.getOrCreate().use { spark =>
      process(spark, opts)
    }
  }
}
