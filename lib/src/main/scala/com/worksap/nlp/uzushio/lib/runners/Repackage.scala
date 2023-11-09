package com.worksap.nlp.uzushio.lib.runners

import com.worksap.nlp.uzushio.lib.utils.Paragraphs
import com.worksap.nlp.uzushio.lib.utils.Resources.AutoClosableResource
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.rogach.scallop.ScallopConf

object Repackage {

  def run(args: Args, spark: SparkSession): Unit = {
    val data = spark.read.parquet(args.input)

    val reparitioned = data.coalesce(args.maxParitions)

    val cleaned = if (args.clear && reparitioned.columns.contains("text")) {
      val cleanUdf = udf { s: String => Paragraphs.extractCleanParagraphs(s).mkString("\n\n") }
      reparitioned.withColumn("text", cleanUdf(reparitioned.col("text")))
    } else reparitioned

    cleaned.write.format(args.format).option("compression", args.compression)
      .mode(SaveMode.Overwrite).save(args.output)
  }

  class ArgParser(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[String]()
    val output = opt[String]()
    val format = opt[String](default = Some("parquet"))
    val compression = opt[String](default = Some("zstd"))
    val maxPartitions = opt[Int](default = Some(10000))
    val clear = toggle("clear", default = Some(false))
    verify()

    def toArgs: Args = Args(
      input = input(),
      output = output(),
      format = format(),
      compression = compression(),
      maxParitions = maxPartitions(),
      clear = clear()
    )
  }

  case class Args(
      input: String,
      output: String,
      format: String,
      compression: String,
      maxParitions: Int,
      clear: Boolean
  )

  def main(args: Array[String]): Unit = {
    val argObj = new ArgParser(args).toArgs
    SparkSession.builder().master("local").getOrCreate().use { spark =>
      run(argObj, spark)
    }
  }
}
