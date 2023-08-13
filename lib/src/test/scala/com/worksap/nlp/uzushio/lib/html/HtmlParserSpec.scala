package com.worksap.nlp.uzushio.lib.html

import com.worksap.nlp.uzushio.lib.utils.ClasspathAccess
import com.worksap.nlp.uzushio.lib.warc.WarcEntryParser
import org.scalatest.freespec.AnyFreeSpec

import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer

class HtmlParserSpec extends AnyFreeSpec with ClasspathAccess {
  "html parsing" - {
    "works with small document" in {
      val processor = new WarcEntryParser
      val data = classpathBytes("docs/perldoc_ja_small.html")
      val paragraphs = processor.parseHtml(data, 0, StandardCharsets.UTF_8)
      assert(paragraphs.length == 20)
    }
    "correct paragraph detection" in {
      val processor = new WarcEntryParser
      val data = classpathBytes("docs/perldoc_ja_tiny.html")
      val paragraphs = processor.parseHtml(data, 0, StandardCharsets.UTF_8)
      assert(paragraphs == ArrayBuffer(
        "body>div.containerこんにちは",
        "body>div.container>div#12345早稲田大学で",
        "body>div.container>div#12345>div自然言語処理",
        "body>div.container>div#12345を",
        "body>div.container勉強する。"
      ))
    }
  }
}