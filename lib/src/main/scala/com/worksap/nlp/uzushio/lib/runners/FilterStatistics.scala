package com.worksap.nlp.uzushio.lib.runners

import com.worksap.nlp.uzushio.lib.cleaning.Document
import com.worksap.nlp.uzushio.lib.filters.CompressionRate
import com.worksap.nlp.uzushio.lib.utils.Paragraphs
import com.worksap.nlp.uzushio.lib.utils.Resources.AutoClosableResource
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{rand, regexp_replace, udf}
import org.rogach.scallop.ScallopConf

object FilterStatistics {

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

    val textOnly = limited.select("text")
    val extractor = extractFilteredMetrix(spark, args.filter())

    val sample = args.examples()
    val cleanPars = udf { (s: String, rng: Double) =>
      val pars = Paragraphs.extractCleanParagraphs(s)
      if (rng <= sample) {
        pars.map(p => p.replaceAll("\n", "\\n")).mkString("<p>")
      } else {
        ""
      }
    }

    val withValues = textOnly
      .withColumn("value", extractor(textOnly.col("text")))
      .select(
        $"value",
        cleanPars($"text", rand()) as "text"
      )

    withValues.persist().repartitionByRange(args.partitions(), withValues.col("value"))
      .sortWithinPartitions("value")
      .write
      .mode(SaveMode.Overwrite)
      .csv(args.output())
  }

  def extractFilteredMetrix(sparkSession: SparkSession, fiter: String): UserDefinedFunction = {
    import sparkSession.implicits._
    fiter match {
      case "compression" =>
        val filter = new CompressionRate(0, 100)
        udf { (s: String) => filter.compressionRatio(Document.parse(s)) }
    }
  }

  class Args(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[String]](required = true)
    val output = opt[String](required = true)
    val filter = opt[String](required = true)
    val limit = opt[Int]()
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
