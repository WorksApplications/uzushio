package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.{Document, Paragraph}
import org.scalatest.freespec.AnyFreeSpec

class AdjacentDuplicateParagraphsSpec extends AnyFreeSpec {
  "AdjacentDuplicateParagraphs" - {
    val filter = new AdjacentDuplicateParagraphs()
    "works with empty document" in {
      val filtered = filter.checkDocument(Document())
      assert(filtered.paragraphs.isEmpty)
    }


    "filters out docs correctly" in {
      val doc = Document(
        Paragraph("", "test1"),
        Paragraph("", "test1"),
        Paragraph("", "test2"),
      )
      val filtered = filter.checkDocument(doc)
      assert(filtered.paragraphs == Seq(
        Paragraph("", "test1"),
        Paragraph("", "test2"),
      ))
    }
  }
}
