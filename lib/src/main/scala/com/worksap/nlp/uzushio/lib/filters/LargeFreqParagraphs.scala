package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.{Document, Paragraph}
import com.worksap.nlp.uzushio.lib.filters.base.DocFilter

import scala.collection.mutable

class LargeFreqParagraphs(count: Int = 3, freq: Int = 100) extends DocFilter {
  override def checkDocument(doc: Document): Document = {
    doc.paragraphs match {
      case p: mutable.Buffer[Paragraph] =>
        markParagraphs(p)
        doc
      case _ =>
        val buf = doc.paragraphs.toBuffer
        val nmarked = markParagraphs(buf)
        if (nmarked > 0) {
          doc.copy(paragraphs = buf)
        } else {
          doc
        }
    }
  }

  def markParagraphs(paragraphs: mutable.Buffer[Paragraph]): Int = {
    var nchanged = 0
    var idx = 0
    val len = paragraphs.length
    while (idx < len) {
      if (shouldRemove(paragraphs, idx)) {
        paragraphs(idx) = paragraphs(idx).copy(remove = this)
        nchanged += removePrev(paragraphs, idx)
        nchanged += 1
      }
      idx += 1
    }
    nchanged
  }

  def removePrev(paragraphs: mutable.Buffer[Paragraph], offset: Int): Int = {
    var result = 0
    val end = math.max(offset - count, 0)
    var idx = offset - 1
    while (idx >= end) {
      val p = paragraphs(idx)
      if (p.isAlive) {
        paragraphs(idx) = p.copy(remove = this)
        result += 1
      }

      idx -= 1
    }
    result
  }

  def shouldRemove(paragraphs: mutable.Buffer[Paragraph], offset: Int): Boolean = {
    var idx = offset
    val end = math.max(offset - count, 0)
    while (idx >= end) {
      val p = paragraphs(idx)
      if (p.nearFreq < freq) {
        return false
      }
      idx -= 1
    }
    true
  }

}
