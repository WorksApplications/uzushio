package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.{Document, Paragraph}
import com.worksap.nlp.uzushio.lib.filters.base.DocFilter

import scala.collection.mutable.ArrayBuffer

/** This class is a hack put in place before the final bugfix
  */
class AdjacentDuplicateParagraphs extends DocFilter {

  private def compressParagraphs(paragraphs: Seq[Paragraph]): Seq[Paragraph] = {
    val result = new ArrayBuffer[Paragraph]()
    val iter = paragraphs.iterator
    if (!iter.hasNext) {
      return paragraphs
    }

    var prev = iter.next()
    while (iter.hasNext) {
      val next = iter.next()
      if (next.text != prev.text) {
        result += prev
        prev = next
      }
    }

    result += prev
    result
  }

  override def checkDocument(doc: Document): Document = {
    val newPars = compressParagraphs(doc.paragraphs)
    if (newPars.length == doc.paragraphs.length) {
      doc
    } else {
      doc.copy(paragraphs = newPars)
    }
  }

  override val toString = "AdjacentDuplicateParagraphs"
}
