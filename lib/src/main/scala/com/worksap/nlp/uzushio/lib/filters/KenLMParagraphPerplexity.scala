package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.{Document, Paragraph}
import com.worksap.nlp.uzushio.lib.filters.base.DocFilter

import scala.collection.mutable

final case class ParagraphWithPerplexity(p: Paragraph, ppx: Float) {
  def isAlive: Boolean = p.isAlive

  def remove(x: AnyRef): ParagraphWithPerplexity = copy(p = p.copy(remove = x))
}

class KenLMParagraphPerplexity(
    sudachi: String,
    kenlm: String,
    outliers: Float = 0.02f,
    count: Int = 3,
    threshold: Float = 1e6f
) extends DocFilter {
  private val lmScore = -Math.log10(threshold).toFloat

  @transient
  private lazy val processor = KenLMEvaluator.make(sudachi, kenlm, outliers)

  override def checkDocument(doc: Document): Document = {
    val proc = processor
    val paragraphs = doc.paragraphs
      .map(p => ParagraphWithPerplexity(p, proc.scoreParagraph(p).toFloat)).toBuffer

    val nchanged = markParagraphs(paragraphs)

    if (nchanged > 0) {
      doc.copy(paragraphs = paragraphs.map(_.p))
    } else {
      doc
    }
  }

  def markParagraphs(paragraphs: mutable.Buffer[ParagraphWithPerplexity]): Int = {
    var nchanged = 0
    var idx = 0
    val len = paragraphs.length
    while (idx < len) {
      val p = paragraphs(idx)
      if (p.isAlive && (shouldRemoveBack(paragraphs, idx) || shouldRemoveFwd(paragraphs, idx, len))) {
        paragraphs(idx) = p.remove(this)
        nchanged += removePrev(paragraphs, idx)
        nchanged += 1
      }
      idx += 1
    }
    nchanged
  }

  def removePrev(paragraphs: mutable.Buffer[ParagraphWithPerplexity], offset: Int): Int = {
    var result = 0
    val end = math.max(offset - count, 0)
    var idx = offset - 1
    while (idx >= end) {
      val p = paragraphs(idx)
      if (p.isAlive && p.ppx <= lmScore) {
        paragraphs(idx) = p.remove(this)
        result += 1
      }

      idx -= 1
    }
    result
  }

  def shouldRemoveBack(
      paragraphs: mutable.Buffer[ParagraphWithPerplexity],
      offset: Int
  ): Boolean = {
    var idx = offset
    val end = math.max(offset - count + 1, 0)
    while (idx >= end) {
      val p = paragraphs(idx)
      if (p.ppx > lmScore) {
        return false
      }
      idx -= 1
    }
    true
  }

  def shouldRemoveFwd(
      paragraphs: mutable.Buffer[ParagraphWithPerplexity],
      offset: Int,
      length: Int
  ): Boolean = {
    var idx = offset
    val end = math.min(offset + count, length)
    while (idx < end) {
      val p = paragraphs(idx)
      if (p.ppx > lmScore) {
        return false
      }
      idx += 1
    }
    true
  }

  override val toString = s"KenLMPar($outliers,$count,$threshold)"
}
