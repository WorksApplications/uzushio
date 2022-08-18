package org.sample.corpus.warc

import java.nio.file.{Path, Paths}
import org.rogach.scallop.ScallopConf
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object WarcLoader {
  /* Load WARC file as RDD. */
  def readFrom(
      spark: SparkSession,
      name: String
  ): RDD[(LongWritableSerializable, WarcWritable)] = {
    spark.sparkContext.newAPIHadoopFile(
      name,
      classOf[WarcInputFormat],
      classOf[LongWritableSerializable],
      classOf[WarcWritable]
    )
  }

  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[Path]](required = true)
    val output = opt[Path](default = Some(Paths.get("./out")))

    val take = opt[Int](default = Some(10))
    verify()
  }

  def run(spark: SparkSession, conf: Conf): Unit = {
    val logger = LogManager.getLogger(this.getClass.getSimpleName)

    val rdd = readFrom(spark, conf.input().mkString(","))

    rdd.map { case (k, v) => v.getRecord() }.take(conf.take()).foreach(println)
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val spark =
      SparkSession.builder().appName(this.getClass.getSimpleName).getOrCreate()

    try { run(spark, conf) }
    finally { spark.stop() }
  }
}
