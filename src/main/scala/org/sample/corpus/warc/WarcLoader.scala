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
  ): RDD[WarcRecord] = {
    spark.sparkContext
      .newAPIHadoopFile(
        name,
        classOf[WarcInputFormat],
        classOf[LongWritableSerializable],
        classOf[WarcWritable]
      )
      .map { case (k, v) => v.getRecord() }
  }

  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[Path]](required = true)
    verify()
  }

  /* count and log the number of records.  */
  def run(spark: SparkSession, conf: Conf): Unit = {
    import spark.implicits._
    val logger = LogManager.getLogger(this.getClass.getSimpleName)

    // all warc record
    val warcRecords = readFrom(spark, conf.input().mkString(",")).cache
    logger.info(s"num warc record: ${warcRecords.count}")

    // http response record
    val responseRecords = warcRecords
      .filter(arc => {
        val contentType = arc.headers.getOrElse("Content-Type", "")
        arc.isResponse && contentType.startsWith(
          "application/http"
        ) && !arc.isTruncated
      })
      .cache
    logger.info(s"num http/resp record: ${responseRecords.count}")

    // content-type text/html
    val htmlRecords = responseRecords
      .mapPartitions(iter => {
        val httpParser = new HttpResponseParser()
        iter.map(arc => {
          val resp = httpParser.parseWarcRecord(arc)
          (arc.headers, resp)
        })
      })
      .filter {
        case (headers, resp) => {
          val contentType =
            resp.getFirstHeader("Content-Type").getOrElse("").trim
          contentType.startsWith("text/html")
        }
      }
    logger.info(s"num html: ${htmlRecords.count}")
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val spark =
      SparkSession.builder().appName(this.getClass.getSimpleName).getOrCreate()

    try { run(spark, conf) }
    finally { spark.stop() }
  }
}
