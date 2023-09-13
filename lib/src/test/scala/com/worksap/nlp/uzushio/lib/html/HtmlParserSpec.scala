package com.worksap.nlp.uzushio.lib.html

import com.worksap.nlp.uzushio.lib.html.HtmlParserSpec.RichByteArray
import com.worksap.nlp.uzushio.lib.utils.ClasspathAccess
import com.worksap.nlp.uzushio.lib.warc.{WarcEntryParser, WarcRecord}
import org.apache.hadoop.fs.Path
import org.archive.io.{ArchiveRecord, ArchiveRecordHeader}
import org.scalatest.freespec.AnyFreeSpec

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.util
import scala.collection.mutable.ArrayBuffer

class HtmlParserSpec extends AnyFreeSpec with ClasspathAccess {
  "html parsing" - {
    "works with small document" in {
      val processor = new WarcEntryParser
      val data = classpathBytes("docs/perldoc_ja_small.html")
      val paragraphs = processor.parseHtml(data.warc, 0, StandardCharsets.UTF_8)
      assert(paragraphs.length == 26)
    }

    "correct paragraph detection" in {
      val processor = new WarcEntryParser
      val data = classpathBytes("docs/paragraph_detect.html")
      val paragraphs = processor.parseHtml(data.warc, 0, StandardCharsets.UTF_8)
      assert(
        paragraphs == Seq(
          "body>div.containerこんにちは",
          "body>div.container>div#12345早稲田大学で",
          "body>div.container>div#12345>div自然言語処理",
          "body>div.container>div#12345を",
          "body>div.container勉強する。"
        )
      )
    }

    "empty paragraphs are ignored" in {
      val processor = new WarcEntryParser
      val data = classpathBytes("docs/links.html")
      val paragraphs = processor.parseHtml(data.warc, 0, StandardCharsets.UTF_8)
      assert(
        paragraphs == Seq(
          "body>div画像リンク"
        )
      )
    }
  }
}

object HtmlParserSpec {
  implicit class RichByteArray(val x: Array[Byte]) extends AnyVal {
    def warc: WarcRecord = new WarcRecord(
      new ArchiveRecord(
        new ByteArrayInputStream(x),
        new ArchiveRecordHeader {
          override def getDate: String = ???

          override def getLength: Long = x.length
          override def getContentLength: Long = x.length

          override def getUrl: String = ???

          override def getMimetype: String = ???

          override def getVersion: String = ???
          override def getOffset: Long = 0
          override def getHeaderValue(key: String): AnyRef = "none"
          override def getHeaderFieldKeys: util.Set[String] = ???
          override def getHeaderFields: util.Map[String, AnyRef] = {
            val res = new util.HashMap[String, AnyRef]()
            res
          }
          override def getReaderIdentifier: String = ???
          override def getRecordIdentifier: String = ???
          override def getDigest: String = ???
          override def getContentBegin: Int = ???
        },
        0,
        false,
        false
      ) {},
      new Path("file:///dev/mem")
    )
  }
}
