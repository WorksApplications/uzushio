package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.{Document, Paragraph}
import org.scalatest.freespec.AnyFreeSpec

class LargeFreqParagraphsSpec extends AnyFreeSpec {
  "LargeFreqParagraphs" - {
    val filter = new LargeFreqParagraphs(freq = 10)

    "deletes all paragraphs" in {
      val doc = Document(
        Paragraph("p", "test1", nearFreq = 15),
        Paragraph("p", "test2", nearFreq = 30),
        Paragraph("p", "test3", nearFreq = 30),
      )
      val filtered = filter.checkDocument(doc)
      assert(filtered.paragraphs(0).remove eq filter)
      assert(filtered.paragraphs(1).remove eq filter)
      assert(filtered.paragraphs(2).remove eq filter)

      assert(filtered.countDroppedParagraphs() == 3)
      val docs = filtered.splitByFilteredParagraphs()
      assert(docs.length == 1)
      val firstDoc = docs.head
      assert(firstDoc.remove eq filter)
    }

    "works with prefix (3)" in {
      val doc = Document(
        Paragraph("p", "test1", nearFreq = 15),
        Paragraph("p", "test2", nearFreq = 30),
        Paragraph("p", "test3", nearFreq = 30),
        Paragraph("p", "test4", nearFreq = 5),
      )
      val filtered = filter.checkDocument(doc)
      assert(filtered.paragraphs(0).remove eq filter)
      assert(filtered.paragraphs(1).remove eq filter)
      assert(filtered.paragraphs(2).remove eq filter)
      assert(filtered.paragraphs(3).remove eq null)

      assert(filtered.countDroppedParagraphs() == 3)
      val docs = filtered.splitByFilteredParagraphs()
      assert(docs.length == 2)
    }

    "works with prefix (2)" in {
      val doc = Document(
        Paragraph("p", "test1", nearFreq = 15),
        Paragraph("p", "test2", nearFreq = 30),
        Paragraph("p", "test3", nearFreq = 5),
        Paragraph("p", "test4", nearFreq = 5),
      )
      val filtered = filter.checkDocument(doc)
      assert(filtered.paragraphs(0).remove eq filter)
      assert(filtered.paragraphs(1).remove eq filter)
      assert(filtered.paragraphs(2).remove eq null)
      assert(filtered.paragraphs(3).remove eq null)

      assert(filtered.countDroppedParagraphs() == 2)
      val docs = filtered.splitByFilteredParagraphs()
      assert(docs.length == 2)
    }

    "works with prefix (1)" in {
      val doc = Document(
        Paragraph("p", "test1", nearFreq = 15),
        Paragraph("p", "test2", nearFreq = 5),
        Paragraph("p", "test3", nearFreq = 5),
        Paragraph("p", "test4", nearFreq = 5),
      )
      val filtered = filter.checkDocument(doc)
      assert(filtered.paragraphs(0).remove eq filter)
      assert(filtered.paragraphs(1).remove eq null)
      assert(filtered.paragraphs(2).remove eq null)
      assert(filtered.paragraphs(3).remove eq null)

      assert(filtered.countDroppedParagraphs() == 1)
      val docs = filtered.splitByFilteredParagraphs()
      assert(docs.length == 2)
    }

    "works with suffix (3)" in {
      val doc = Document(
        Paragraph("p", "test1", nearFreq = 5),
        Paragraph("p", "test2", nearFreq = 15),
        Paragraph("p", "test3", nearFreq = 15),
        Paragraph("p", "test4", nearFreq = 15),
      )
      val filtered = filter.checkDocument(doc)
      assert(filtered.paragraphs(0).remove eq null)
      assert(filtered.paragraphs(1).remove eq filter)
      assert(filtered.paragraphs(2).remove eq filter)
      assert(filtered.paragraphs(3).remove eq filter)

      assert(filtered.countDroppedParagraphs() == 3)
      val docs = filtered.splitByFilteredParagraphs()
      assert(docs.length == 2)
    }

    "works with suffix (2)" in {
      val doc = Document(
        Paragraph("p", "test1", nearFreq = 5),
        Paragraph("p", "test2", nearFreq = 5),
        Paragraph("p", "test3", nearFreq = 15),
        Paragraph("p", "test4", nearFreq = 15),
      )
      val filtered = filter.checkDocument(doc)
      assert(filtered.paragraphs(0).remove eq null)
      assert(filtered.paragraphs(1).remove eq null)
      assert(filtered.paragraphs(2).remove eq filter)
      assert(filtered.paragraphs(3).remove eq filter)

      assert(filtered.countDroppedParagraphs() == 2)
      val docs = filtered.splitByFilteredParagraphs()
      assert(docs.length == 2)
    }

    "works with suffix (1)" in {
      val doc = Document(
        Paragraph("p", "test1", nearFreq = 5),
        Paragraph("p", "test2", nearFreq = 5),
        Paragraph("p", "test3", nearFreq = 5),
        Paragraph("p", "test4", nearFreq = 15),
      )
      val filtered = filter.checkDocument(doc)
      assert(filtered.paragraphs(0).remove eq null)
      assert(filtered.paragraphs(1).remove eq null)
      assert(filtered.paragraphs(2).remove eq null)
      assert(filtered.paragraphs(3).remove eq filter)

      assert(filtered.countDroppedParagraphs() == 1)
      val docs = filtered.splitByFilteredParagraphs()
      assert(docs.length == 2)
    }

    "skips infix (1)" in {
      val doc = Document(
        Paragraph("p", "test1", nearFreq = 5),
        Paragraph("p", "test2", nearFreq = 5),
        Paragraph("p", "test3", nearFreq = 15),
        Paragraph("p", "test4", nearFreq = 5),
        Paragraph("p", "test5", nearFreq = 5),
      )
      val filtered = filter.checkDocument(doc)
      assert(filtered.paragraphs(0).remove eq null)
      assert(filtered.paragraphs(1).remove eq null)
      assert(filtered.paragraphs(2).remove eq null)
      assert(filtered.paragraphs(3).remove eq null)
      assert(filtered.paragraphs(4).remove eq null)

      assert(filtered.countDroppedParagraphs() == 0)
      val docs = filtered.splitByFilteredParagraphs()
      assert(docs.length == 1)
    }

    "skips infix (2)" in {
      val doc = Document(
        Paragraph("p", "test1", nearFreq = 5),
        Paragraph("p", "test2", nearFreq = 5),
        Paragraph("p", "test3", nearFreq = 15),
        Paragraph("p", "test4", nearFreq = 15),
        Paragraph("p", "test5", nearFreq = 5),
      )
      val filtered = filter.checkDocument(doc)
      assert(filtered.paragraphs(0).remove eq null)
      assert(filtered.paragraphs(1).remove eq null)
      assert(filtered.paragraphs(2).remove eq null)
      assert(filtered.paragraphs(3).remove eq null)
      assert(filtered.paragraphs(4).remove eq null)

      assert(filtered.countDroppedParagraphs() == 0)
      val docs = filtered.splitByFilteredParagraphs()
      assert(docs.length == 1)
    }

    "works with infix (3)" in {
      val doc = Document(
        Paragraph("p", "test1", nearFreq = 5),
        Paragraph("p", "test2", nearFreq = 15),
        Paragraph("p", "test3", nearFreq = 15),
        Paragraph("p", "test4", nearFreq = 15),
        Paragraph("p", "test5", nearFreq = 5),
      )
      val filtered = filter.checkDocument(doc)
      assert(filtered.paragraphs(0).remove eq null)
      assert(filtered.paragraphs(1).remove eq filter)
      assert(filtered.paragraphs(2).remove eq filter)
      assert(filtered.paragraphs(3).remove eq filter)
      assert(filtered.paragraphs(4).remove eq null)

      assert(filtered.countDroppedParagraphs() == 3)
      val docs = filtered.splitByFilteredParagraphs()
      assert(docs.length == 2)
    }

    "works with infix (4)" in {
      val doc = Document(
        Paragraph("p", "test1", nearFreq = 5),
        Paragraph("p", "test2", nearFreq = 15),
        Paragraph("p", "test3", nearFreq = 15),
        Paragraph("p", "test4", nearFreq = 15),
        Paragraph("p", "test5", nearFreq = 15),
        Paragraph("p", "test6", nearFreq = 5),
      )
      val filtered = filter.checkDocument(doc)
      assert(filtered.paragraphs(0).remove eq null)
      assert(filtered.paragraphs(1).remove eq filter)
      assert(filtered.paragraphs(2).remove eq filter)
      assert(filtered.paragraphs(3).remove eq filter)
      assert(filtered.paragraphs(4).remove eq filter)
      assert(filtered.paragraphs(5).remove eq null)

      assert(filtered.countDroppedParagraphs() == 4)
      val docs = filtered.splitByFilteredParagraphs()
      assert(docs.length == 2)
    }

    "works with prefix/suffix (2)" in {
      val doc = Document(
        Paragraph("p", "test1", nearFreq = 15),
        Paragraph("p", "test2", nearFreq = 15),
        Paragraph("p", "test3", nearFreq = 5),
        Paragraph("p", "test4", nearFreq = 5),
        Paragraph("p", "test5", nearFreq = 15),
        Paragraph("p", "test6", nearFreq = 15),
      )
      val filtered = filter.checkDocument(doc)
      assert(filtered.paragraphs(0).remove eq filter)
      assert(filtered.paragraphs(1).remove eq filter)
      assert(filtered.paragraphs(2).remove eq null)
      assert(filtered.paragraphs(3).remove eq null)
      assert(filtered.paragraphs(4).remove eq filter)
      assert(filtered.paragraphs(5).remove eq filter)

      assert(filtered.countDroppedParagraphs() == 4)
      val docs = filtered.splitByFilteredParagraphs()
      assert(docs.length == 2)
    }

    "skips two infices (2)" in {
      val doc = Document(
        Paragraph("p", "test1", nearFreq = 5),
        Paragraph("p", "test2", nearFreq = 15),
        Paragraph("p", "test3", nearFreq = 15),
        Paragraph("p", "testx", nearFreq = 5),
        Paragraph("p", "test4", nearFreq = 15),
        Paragraph("p", "test5", nearFreq = 15),
        Paragraph("p", "test6", nearFreq = 5),
      )
      val filtered = filter.checkDocument(doc)
      assert(filtered.paragraphs(0).remove eq null)
      assert(filtered.paragraphs(1).remove eq null)
      assert(filtered.paragraphs(2).remove eq null)
      assert(filtered.paragraphs(3).remove eq null)
      assert(filtered.paragraphs(4).remove eq null)
      assert(filtered.paragraphs(5).remove eq null)
      assert(filtered.paragraphs(6).remove eq null)

      assert(filtered.countDroppedParagraphs() == 0)
      val docs = filtered.splitByFilteredParagraphs()
      assert(docs.length == 1)
    }


  }
}
