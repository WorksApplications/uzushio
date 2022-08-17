package org.sample.corpus.warc

import collection.JavaConverters._
import java.nio.file.{Path, Paths}
import java.io.{
  InputStream,
  ByteArrayInputStream,
  FileInputStream,
  SequenceInputStream
}
import org.rogach.scallop.ScallopConf
import org.apache.commons.io.IOUtils
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

import org.archive.io.{ArchiveReader, ArchiveRecord, ArchiveRecordHeader}
import org.archive.io.warc.WARCReaderFactory

import org.apache.hc.core5.http.HttpResponse
import org.apache.hc.core5.http.impl.io.{
  DefaultHttpResponseParser,
  SessionInputBufferImpl
}
import org.apache.hc.core5.http.io.SessionInputBuffer

// import org.apache.tika.parser.AutoDetectParser
// import org.apache.tika.parser.http.HttpParser
import org.apache.tika.parser.html.HtmlParser
import org.apache.tika.metadata.Metadata
import org.apache.tika.sax.{BodyContentHandler, ToXMLContentHandler}
import org.xml.sax.{ContentHandler, Attributes}

object WarcLoader {
  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[Path]](required = true)
    val output = opt[Path](default = Some(Paths.get("./out")))

    val take = opt[Int](default = Some(10))
    verify()
  }

  val bufSize = 1024

  val warcKeys =
    Array("WARC-Target-URI", "WARC-Record-ID", "WARC-Date", "reader-identifier")
  val headerKeys = Array("Content-Type")

  def warcHeaderMap(header: ArchiveRecordHeader): Map[String, String] = {
    header
      .getHeaderFields()
      .asScala
      .map { case (k, v) => (k, v.toString) }
      .toMap
  }

  def responseHeaderMap(resp: HttpResponse): Map[String, String] = {
    resp.getHeaders().map(h => (h.getName -> h.getValue)).toMap
  }

  def metaMap(meta: Metadata): Map[String, String] = {
    meta.names.map(k => (k -> Option(meta.get(k)).getOrElse(""))).toMap
  }

  def run(spark: SparkSession, conf: Conf): Unit = {
    import spark.implicits._
    val logger = LogManager.getLogger(this.getClass.getSimpleName)

    val inputFiles = conf.input()
    val arcIter = loadAsArcIterator(inputFiles(0)).take(conf.take())

    val warcResponseIter = arcIter.filter(arc => {
      // use only http response
      // ref https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0
      val headerMap = arc.getHeader().getHeaderFields().asScala
      val warcType = headerMap.get("WARC-Type").getOrElse("")
      val truncated = headerMap.get("WARC-Truncated").nonEmpty
      warcType == "response" && !truncated
    })

    val responseParser = new DefaultHttpResponseParser()
    val siBuf = new SessionInputBufferImpl(bufSize)

    val htmlIter = warcResponseIter
      .map(arc => {
        val warcHeader = arc.getHeader()
        try {
          // parse arc body as http response
          val resp = responseParser.parse(siBuf, arc)
          val respBodyIs = bodyInputStream(siBuf, arc)

          (warcHeader, Some(resp), Some(respBodyIs))
        } catch {
          // case e: java.io.IOException => {}
          case e: org.apache.hc.core5.http.HttpException => {
            logger.info(s"http parse error: ${e}")

            (warcHeader, None, None)
          }
        }
      })
      .filter {
        // use only html response (warc)
        // ref https://commoncrawl.github.io/cc-crawl-statistics/plots/mimetypes
        // whrn handling cc-wet file, "text/plain" will do
        case (warcHeader, Some(resp), Some(body)) => {
          val contentType =
            Option(resp.getHeader("Content-Type")).map(_.getValue).getOrElse("")
          contentType.startsWith("text/html")
        }
        case _ => { false }
      }

    val parsed = htmlIter.map {
      case (wh, Some(resp), Some(body)) => {
        val tikaParser = new HtmlParser()
        // val handler = new ToXMLContentHandler()
        // val handler = new BodyContentHandler()
        // val handler = new BodyContentHandler(new ToXMLContentHandler())
        val handler = new BodyContentHandler(new ToParagraphHandler())
        val meta = new Metadata()

        // provide content type as a hint
        val contentType =
          Option(resp.getHeader("Content-Type")).map(_.getValue).getOrElse("")
        meta.add("Content-Type", contentType)

        try {
          tikaParser.parse(body, handler, meta)
        } catch {
          // case e: java.io.IOException => {}
          case e: org.xml.sax.SAXException => { logger.info(s"${e}") }
          case e: org.apache.tika.exception.TikaException => {
            logger.info(s"${e}")
          }
        } finally {
          body.close()
        }

        (wh, resp, meta, handler)
      }
    }

    val rdd = spark.sparkContext
      .parallelize(parsed.map {
        case (wh, resp, meta, handler) => {
          (
            warcHeaderMap(wh),
            responseHeaderMap(resp),
            metaMap(meta),
            handler.toString
          )
        }
      }.toSeq)
      .toDF("warcHeader", "respHeader", "tikaMetadata", "content")

    rdd.write.save(conf.output().toString)

  }

  def loadAsArcIterator(file: Path) = {
    val is: FileInputStream = new FileInputStream(file.toString);
    // The file name identifies the ArchiveReader and indicates if it should be decompressed
    val arcReader = WARCReaderFactory.get(file.toString, is, true);
    arcReader.iterator.asScala
  }

  def emptyInputStream() = {
    new java.io.ByteArrayInputStream(Array.emptyByteArray)
  }

  def bodyInputStream(inBuff: SessionInputBuffer, rest: InputStream) = {
    val byteBuf = Array.ofDim[Byte](inBuff.length)
    val empty = emptyInputStream()
    val leftBytes = inBuff.read(byteBuf, empty)
    empty.close()
    new SequenceInputStream(
      new ByteArrayInputStream(byteBuf.slice(0, leftBytes)),
      rest
    )
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val spark =
      SparkSession.builder().appName(this.getClass.getSimpleName).getOrCreate()

    try { run(spark, conf) }
    finally { spark.stop() }
  }
}
