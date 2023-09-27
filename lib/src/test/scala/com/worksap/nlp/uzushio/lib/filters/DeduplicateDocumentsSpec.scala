package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.Document
import javax.swing.tree.FixedHeightLayoutCache
import org.scalatest.freespec.AnyFreeSpec


class FixedProbRandomGenerator(
  val returnProb: Double = 0.5
) extends RandomGeneratorFromStringBase {
  def generateRandom(docId: String): Double = returnProb
}


class DeduplicateDocumentsSpec extends AnyFreeSpec {
  def generateFilter(returnProb: Double): DeduplicateDocuments = {
    val randomGenerator = new FixedProbRandomGenerator(returnProb)
    new DeduplicateDocuments(100, randomGenerator)
  }

  "DeduplicateDocumentsSpec" - {
    val filter = generateFilter(0.5)

    "computes correct ratio for non-deuplicated documents" in {
      val paragraphs = testParagraphs(
        Seq("test", "test", "test", "test"),
        Seq(1, 1, 1, 1)
      )
      val doc = Document(paragraphs, "test")
      assert(0.0f == filter.computeNearDuplicateTextRatio(doc))
      assert(false == filter.shouldRemoveDocument(doc))
    }

    "computes correct ratio for non-deuplicated documents (boundary)" in {
      val paragraphs = testParagraphs(
        Seq("test", "test", "test", "test"),
        Seq(1, 1, 99, 100)
      )
      val doc = Document(paragraphs, "test")
      assert(0.5f > filter.computeNearDuplicateTextRatio(doc))
      assert(false == filter.shouldRemoveDocument(doc))
    }

    "computes correct ratio for deuplicated documents" in {
      val paragraphs = testParagraphs(
        Seq("test", "test", "test", "test"),
        Seq(100, 100, 100, 100)
      )
      val doc = Document(paragraphs, "test")
      assert(1.0f == filter.computeNearDuplicateTextRatio(doc))
      assert(true == filter.shouldRemoveDocument(doc))
    }

    "computes correct ratio for deuplicated documents (boundary)" in {
      val paragraphs = testParagraphs(
        Seq("test", "test", "test", "test"),
        Seq(1, 1, 100, 100)
      )
      val doc = Document(paragraphs, "test")
      assert(0.5f == filter.computeNearDuplicateTextRatio(doc))
      assert(true == filter.shouldRemoveDocument(doc))
    }
  }
}