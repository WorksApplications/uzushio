package com.worksap.nlp.uzushio.warc

import com.worksap.nlp.uzushio.lib.html.AllTagMapper

import java.io.ByteArrayInputStream
import java.nio.file.{Path, Paths}
import org.rogach.scallop.ScallopConf
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.tika.detect.EncodingDetector
import org.apache.tika.metadata.{HttpHeaders, Metadata}
import org.apache.tika.parser.html.{
  HtmlEncodingDetector,
  HtmlMapper,
  HtmlParser
}
import org.apache.tika.parser.ParseContext
import org.apache.tika.sax.BodyContentHandler

object WarcToDocument {
  @transient lazy val logger = LogManager.getLogger(this.getClass.getSimpleName)

  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[Path]](required = true)
    val output = opt[Path](default = Some(Paths.get("./out")))

    val paragraphDelim = opt[String](default = Some("\n\n"))
    val repartition = opt[Int](descr =
      "repartition count for total input file (default: 1 per file)."
    )

    val resultOnly = opt[Boolean](descr = "set not to save fat medium data.")
    val sample = opt[Int](descr = "take sample items from head (for debug).")
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

    // TODO: use charset detection tool instead of relying header or metatag
    // provide content-type as a hint for charset detection
    resp.getFirstHeader(HttpHeaders.CONTENT_TYPE) match {
      case Some(ct) => { meta.add(HttpHeaders.CONTENT_TYPE, ct) }
      case None     => {}
    }
    // auto detect charset from meta-tag (if not provided from content-type)
    context.set(classOf[EncodingDetector], new HtmlEncodingDetector())

    // use all html tags in the handler
    context.set(classOf[HtmlMapper], new AllTagMapper())

    val bodyIs = new ByteArrayInputStream(resp.body)

    try {
      parser.parse(bodyIs, handler, meta, context)
    } catch {
      // In the case of error, meta and handler are empty.
      // TODO: error handling if recoverable
      case e: org.xml.sax.SAXException => { logger.warn(s"${e}") }
      case e: org.apache.tika.exception.TikaException => {
        logger.warn(s"${e}")
      }
      case e: java.lang.StringIndexOutOfBoundsException => {
        logger.warn(s"${e}")
      }
      case e: java.lang.NullPointerException => {
        // TODO: parsing common crawl file (2022-40, 0-9) raises this.
        logger.warn(s"${e}")
      }
      case e: java.nio.charset.IllegalCharsetNameException => {
        // TODO: update charset/encoding detection
        logger.warn(s"${e}")
      }
    } finally {
      bodyIs.close()
    }

    (meta, handler)
  }

  def run(spark: SparkSession, conf: Conf): Unit = {
    import spark.implicits._

    // load warc files into RDD
    val warcRecords = WarcLoader
      .readFrom(spark, conf.input().mkString(","))
      // use http response record only
      .filter(arc => {
        val contentType = arc.headers.getOrElse(HttpHeaders.CONTENT_TYPE, "")
        arc.isResponse() && contentType.startsWith(
          "application/http"
        ) && !arc.isTruncated()
      })

    // repartition
    val repartitioned = (conf.repartition.toOption match {
      case None    => warcRecords
      case Some(n) => warcRecords.coalesce(n, shuffle = true)
    }).persist()
    logger.info(s"warc http responce record count: ${repartitioned.count()}")

    val htmlResponces = repartitioned
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
          val contentType =
            resp.getFirstHeader(HttpHeaders.CONTENT_TYPE).getOrElse("").trim
          contentType.startsWith("text/html")
        }
      }
      .persist()
    logger.info(s"html responce count: ${htmlResponces.count()}")
    repartitioned.unpersist()

    val pDelim = conf.paragraphDelim() // conf is not serializable
    val textParsed = htmlResponces
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
            ParagraphHandler.toCleanString(handler.toString, outDelim = pDelim)
          )
        })
      })
      .toDF("warcHeaders", "httpHeaders", "tikaMetadata", "html", "document")
      .persist()
    logger.info(
      s"persed document count: ${textParsed.filter(_.getAs[String](4) != "").count()}"
    )
    htmlResponces.unpersist()

    // sampling for debug purpose
    val limited = conf.sample.toOption match {
      case None    => textParsed
      case Some(n) => textParsed.limit(n)
    }

    // save full data if specified
    val reloaded = if (conf.resultOnly()) {
      limited
    } else {
      val p = conf.output().toString + "_fulldata"
      limited.write.save(p)
      spark.read.load(p)
    }

    // pickup neccessary parts
    val urlKey = "WARC-Target-URI"
    val takeUrl = udf { wh: Map[String, String] => wh.getOrElse(urlKey, "") }

    val result = reloaded
      .withColumn(urlKey, takeUrl(col("warcHeaders")))
      .select(urlKey, "document")

    result.write.save(conf.output().toString)
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args.toIndexedSeq)
    val spark =
      SparkSession.builder().appName(this.getClass.getSimpleName).getOrCreate()

    try { run(spark, conf) }
    finally { spark.stop() }
  }
}
