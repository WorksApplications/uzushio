package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.{Document, Paragraph}
import com.worksap.nlp.uzushio.lib.filters.base.DocFilter

import scala.collection.mutable

class LargeFreqParagraphs(count: Int = 3, freq: Int = 100) extends DocFilter {
  override def checkDocument(doc: Document): Document = {
    doc.paragraphs match {
      case p: mutable.Buffer[Paragraph] =>
        markParagraphs(p)
        val nmarked = markParagraphs(p)
        if (nmarked > 0) {
          doc.copy(paragraphs = p)
        } else {
          doc
        }
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
      val p = paragraphs(idx)
      if (p.isAlive && (shouldRemoveBack(paragraphs, idx) || shouldRemoveFwd(paragraphs, idx, len))) {
        paragraphs(idx) = p.copy(remove = this)
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
      if (p.isAlive && p.nearFreq >= freq) {
        paragraphs(idx) = p.copy(remove = this)
        result += 1
      }

      idx -= 1
    }
    result
  }

  def shouldRemoveBack(paragraphs: mutable.Buffer[Paragraph], offset: Int): Boolean = {
    var idx = offset
    val end = math.max(offset - count + 1, 0)
    while (idx >= end) {
      val p = paragraphs(idx)
      if (p.nearFreq < freq) {
        return false
      }
      idx -= 1
    }
    true
  }

  def shouldRemoveFwd(paragraphs: mutable.Buffer[Paragraph], offset: Int, length: Int): Boolean = {
    var idx = offset
    val end = math.min(offset + count, length)
    while (idx < end) {
      val p = paragraphs(idx)
      if (p.nearFreq < freq) {
        return false
      }
      idx += 1
    }
    true
  }

  override val toString = s"LargeFreqParagraphs($count,$freq)"
}
