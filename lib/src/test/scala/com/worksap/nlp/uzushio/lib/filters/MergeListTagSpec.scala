package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.{Document, Paragraph}
import org.scalatest.freespec.AnyFreeSpec

class MergeListTagSpec extends AnyFreeSpec {
  "MergeListTag" - {
    val filter = new MergeListTag()
    "do no operation for empty document" in {
      val doc = testDoc("")
      assert("" == filter.checkDocument(doc).aliveParagraphs.map(_.text).mkString(""))
    }

    "do no operation for no list document" in {
      val texts = Seq("test 1", "test 2", "test 3")
      val doc = Document(testParagraphs(texts))
      assert(texts.mkString("") == filter.checkDocument(doc).aliveParagraphs.map(_.text).mkString(""))
    }

    "merge two paragraphs into a list" in {
      val doc = Document(
        Paragraph("body>ul>li.text", "test"),
        Paragraph("body>ul>li.text", "test2"),
      )
      val filtered = filter.checkDocument(doc).paragraphs

      assert(filtered.size == 1)
      assert(filtered(0).text == "- test\n- test2")
    }

    "merge three paragraphs into a list" in {
      val doc = Document(
        Paragraph("body>ul>li.text", "test", nearFreq = 6, exactFreq = 4),
        Paragraph("body>ul>li.text", "test2", nearFreq = 9, exactFreq = 2),
        Paragraph("body>ul>li.text", "test3", nearFreq = 3, exactFreq = 3),
      )
      val filtered = filter.checkDocument(doc).paragraphs

      assert(filtered.size == 1)
      assert(filtered(0).text == "- test\n- test2\n- test3")
      assert(filtered(0).nearFreq == 3)
      assert(filtered(0).exactFreq == 2)
    }

    "paragraphs with different paths are not merged" in {
      val doc = Document(
        Paragraph("body>ul>li.text1", "test"),
        Paragraph("body>ul>li.text2", "test2"),
        Paragraph("body>ul>li.text3", "test3"),
      )
      val filtered = filter.checkDocument(doc).paragraphs

      assert(filtered.size == 3)
      assert(filtered(0).text == "test")
      assert(filtered(1).text == "test2")
      assert(filtered(2).text == "test3")
    }

    "deleted paragraphs break list merging (start)" in {
      val doc = Document(
        Paragraph("body>ul>li.text", "test", remove = this, nearFreq = 5, exactFreq = 5),
        Paragraph("body>ul>li.text", "test2", nearFreq = 6, exactFreq = 8),
        Paragraph("body>ul>li.text", "test3", nearFreq = 3, exactFreq = 10),
      )
      val filtered = filter.checkDocument(doc).paragraphs

      assert(filtered.size == 2)
      assert(filtered(0).text == "test")
      assert(filtered(0).nearFreq == 5)
      assert(filtered(0).exactFreq == 5)
      assert(filtered(1).text == "- test2\n- test3")
      assert(filtered(1).nearFreq == 3)
      assert(filtered(1).exactFreq == 8)
    }

    "deleted paragraphs break list merging (mid)" in {
      val doc = Document(
        Paragraph("body>ul>li.text", "test", nearFreq = 2, exactFreq = 5),
        Paragraph("body>ul>li.text", "test2", remove = this, nearFreq = 4, exactFreq = 7),
        Paragraph("body>ul>li.text", "test3"),
      )
      val filtered = filter.checkDocument(doc).paragraphs

      assert(filtered.size == 3)
      assert(filtered(0).text == "test")
      assert(filtered(0).nearFreq == 2)
      assert(filtered(0).exactFreq == 5)
      assert(filtered(1).text == "test2")
      assert(filtered(1).nearFreq == 4)
      assert(filtered(1).exactFreq == 7)
      assert(filtered(2).text == "test3")
      assert(filtered(2).nearFreq == 1)
      assert(filtered(2).exactFreq == 1)
    }

    "deleted paragraphs break list merging (end)" in {
      val doc = Document(
        Paragraph("body>ul>li.text", "test", nearFreq = 10, exactFreq = 10),
        Paragraph("body>ul>li.text", "test2", nearFreq = 3, exactFreq = 3),
        Paragraph("body>ul>li.text", "test3", remove = this),
      )
      val filtered = filter.checkDocument(doc).paragraphs

      assert(filtered.size == 2)
      assert(filtered(0).text == "- test\n- test2")
      assert(filtered(0).nearFreq == 3)
      assert(filtered(0).exactFreq == 3)
      assert(filtered(1).text == "test3")
      assert(filtered(1).nearFreq == 1)
      assert(filtered(1).exactFreq == 1)
    }
  }
}
