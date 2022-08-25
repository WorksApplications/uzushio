package org.sample.corpus.warc

import collection.JavaConverters._

import java.io.ByteArrayInputStream
import java.nio.file.{Path, Paths}
import org.rogach.scallop.ScallopConf
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import org.apache.tika.detect.EncodingDetector
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.html.{
  HtmlParser,
  HtmlMapper,
  HtmlEncodingDetector
}
import org.apache.tika.parser.ParseContext
import org.apache.tika.sax.BodyContentHandler

object WarcToDocument {
  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[Path]](required = true)
    val output = opt[Path](default = Some(Paths.get("./out")))

    val textOnly = opt[Boolean]()
    // take [sample] items from head
    val sample = opt[Int]()
    verify()
  }

  /* parse html using tika parser */
  def parseHtml(resp: HttpResponseSerializable, parser: HtmlParser) = {
    val handler = new BodyContentHandler(
      new ParagraphHandler()
      // new NWCToolkitHandler()
      // new JusTextHandler()
    )
    val meta = new Metadata()
    val context = new ParseContext()

    // provide content-type as a hint for charset detection
    resp.getHeader("Content-Type") match {
      case Some(ct) => { meta.add("Content-Type", ct) }
      case None     => {}
    }
    // use all html tags in the handler
    context.set(classOf[HtmlMapper], new AllTagMapper())
    // auto detect charset from meta-tag
    context.set(classOf[EncodingDetector], new HtmlEncodingDetector())

    val bodyIs = new ByteArrayInputStream(resp.body)

    try {
      parser.parse(bodyIs, handler, meta, context)
    } catch {
      // In the case of error, meta and handler are empty.
      case e: org.xml.sax.SAXException => { println(s"${e}") }
      case e: org.apache.tika.exception.TikaException => {
        println(s"${e}")
      }
    } finally {
      bodyIs.close()
    }

    (meta, handler)
  }

  def run(spark: SparkSession, conf: Conf): Unit = {
    import spark.implicits._
    val logger = LogManager.getLogger(this.getClass.getSimpleName)

    val parsed = WarcLoader
      .readFrom(spark, conf.input().mkString(","))
      // use http response record only
      .filter(arc => {
        val contentType = arc.headers.getOrElse("Content-Type", "")
        arc.isResponse && contentType.startsWith(
          "application/http"
        ) && !arc.isTruncated
      })
      // parse body as http response
      .mapPartitions(iter => {
        val httpParser = new HttpResponseParser()
        iter.map(arc => {
          val resp = httpParser.parseWarcRecord(arc)
          (arc.headers, resp)
        })
      })
      // filter out non-html records
      .filter {
        case (headers, resp) => {
          val contentType = resp.getHeader("Content-Type").getOrElse("").trim
          contentType.startsWith("text/html")
        }
      }
      // parse response body and extract text
      .mapPartitions(iter => {
        val tikaParser = new HtmlParser()
        iter.map(v => {
          val (warcHeaders, resp) = v
          val (meta, handler) = parseHtml(resp, tikaParser)

          (
            warcHeaders,
            resp.getHeaders(),
            meta.names.map(k => (k -> Option(meta.get(k)).getOrElse(""))).toMap,
            new String(resp.body),
            handler.toString
          )
        })
      })
      .toDF("warcHeaders", "httpHeaders", "tikaMetadata", "html", "content")

    // sampling for debug purpose
    val result = conf.sample.toOption match {
      case None    => parsed
      case Some(n) => parsed.limit(n)
    }

    if (conf.textOnly()) {
      result.select("content").write.text(conf.output().toString)
    } else {
      result.write.save(conf.output().toString)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val spark =
      SparkSession.builder().appName(this.getClass.getSimpleName).getOrCreate()

    try { run(spark, conf) }
    finally { spark.stop() }
  }
}
