package org.sample.corpus.warc

import collection.JavaConverters._

import java.io.ByteArrayInputStream
import java.nio.file.{Path, Paths}
import org.rogach.scallop.ScallopConf
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import org.apache.tika.parser.html.HtmlParser
import org.apache.tika.parser.html.HtmlMapper
import org.apache.tika.parser.ParseContext
import org.apache.tika.metadata.Metadata
import org.apache.tika.sax.BodyContentHandler

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

  /* Load WARC response records from file as RDD  */
  def readFullResponseFrom(
      spark: SparkSession,
      name: String
  ): RDD[WarcRecord] = {
    readFrom(spark, name).filter(arc => arc.isResponse && !arc.isTruncated)
  }

  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[Path]](required = true)
    val output = opt[Path](default = Some(Paths.get("./out")))

    val take = opt[Int](default = Some(10))
    verify()
  }

  def parseHtml(resp: HttpResponseSerializable, parser: HtmlParser) = {
    val handler = new BodyContentHandler(
      new ToParagraphHandler()
      // new NWCToolkitHandler()
    )

    val meta = new Metadata()
    resp.getHeader("Content-Type") match { // provide content-type as a hint
      case Some(ct) => meta.add("Content-Type", ct)
      case None     => {}
    }

    val context = new ParseContext()
    context.set(classOf[HtmlMapper], new AllTagMapper())

    val bodyIs = new ByteArrayInputStream(resp.body)

    try {
      parser.parse(bodyIs, handler, meta, context)
    } catch {
      // case e: java.io.IOException => {}
      case e: org.xml.sax.SAXException => { println(s"${e}") }
      case e: org.apache.tika.exception.TikaException => {
        println(s"${e}")
      }
    } finally {
      bodyIs.close()
    }

    (
      meta.names.map(k => (k -> Option(meta.get(k)).getOrElse(""))).toMap,
      handler.toString
    )
  }

  def run(spark: SparkSession, conf: Conf): Unit = {
    import spark.implicits._
    val logger = LogManager.getLogger(this.getClass.getSimpleName)

    val rdd = readFrom(spark, conf.input().mkString(","))

    val parsed = rdd
      .filter(arc => arc.isResponse && !arc.isTruncated)
      .mapPartitions(iter => {
        val httpParser = new HttpResponseParser()
        iter.map(arc => httpParser.parseWarcRecord(arc))
      })
      .filter(resp => {
        val contentType = resp.getHeader("Content-Type").getOrElse("").trim
        contentType.startsWith("text/html")
      })
      .mapPartitions(iter => {
        val tikaParser = new HtmlParser()
        iter.map(resp => { parseHtml(resp, tikaParser) })
      })
      .toDF("tikaMetadata", "content")
      .limit(conf.take())

    parsed.write.save(conf.output().toString)
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val spark =
      SparkSession.builder().appName(this.getClass.getSimpleName).getOrCreate()

    try { run(spark, conf) }
    finally { spark.stop() }
  }
}
