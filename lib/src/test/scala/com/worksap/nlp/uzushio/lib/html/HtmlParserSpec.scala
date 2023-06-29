package com.worksap.nlp.uzushio.lib.html

import com.worksap.nlp.uzushio.lib.utils.ClasspathAccess
import com.worksap.nlp.uzushio.lib.warc.WarcEntryParser
import org.scalatest.freespec.AnyFreeSpec

import java.nio.charset.StandardCharsets

class HtmlParserSpec extends AnyFreeSpec with ClasspathAccess {
  "html parsing" - {
    "works with small document" in {
      val processor = new WarcEntryParser
      val data = classpathBytes("docs/perldoc_ja_small.html")
      val paragraphs = processor.parseHtml(data, 0, StandardCharsets.UTF_8)
      assert(paragraphs.length == 20)
    }
  }
}
