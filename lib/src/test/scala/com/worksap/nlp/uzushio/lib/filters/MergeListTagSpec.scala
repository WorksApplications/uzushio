package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.Document
import com.worksap.nlp.uzushio.lib.utils.Paragraphs
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

    "merge list tag texts and put 'remove' sign on rests for document including list tags" in {
      val texts = Seq("test 1", "li test 1", "li test 2", "li test 3", "li test 4", "li test 5", "test 2")
      val nearFreqs = Seq(1, 2, 3, 5, 4, 1, 1)
      val exactFreqs = Seq(1, 2, 3, 5, 4, 1, 1)
      val paths = Seq("body>p.text", "body>ul>li.text", "body>ul>li.text", "body>ul>li.text2", "body>ul>li.text2", "body>ul>li.text3", "body>p.text")
      val paragraphs = testParagraphs(texts, nearFreqs, exactFreqs, paths)
      val doc = Document(paragraphs)

      assert(Seq("test 1", "- li test 1\n- li test 2", "- li test 3\n- li test 4", "- li test 5", "test 2") == filter.checkDocument(doc).aliveParagraphs.map(_.text).toSeq)
      assert(2 == filter.checkDocument(doc).aliveParagraphs.map(_.nearFreq).drop(1).toList.head)
      assert(2 == filter.checkDocument(doc).aliveParagraphs.map(_.exactFreq).drop(1).toList.head)
      assert(Seq(false, false, true, false, true, false, false) == filter.checkDocument(doc).paragraphs.map(_.remove != null))
      assert(Seq("body>p.text", "body>ul>li.text", "body>ul>li.text2", "body>ul>li.text3", "body>p.text") == filter.checkDocument(doc).aliveParagraphs.map(_.path).toSeq)
    }
  }
}
