package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.filters.base.DocFilter
import com.worksap.nlp.uzushio.lib.cleaning.{Document, Paragraph}

import scala.collection.mutable.ArrayBuffer

class MergeListTag extends DocFilter {
  final private val acceptedTags: Seq[String] = Array("li", "option")

  override def checkDocument(doc: Document): Document = {
    val iter = doc.paragraphs.iterator

    if (!iter.hasNext) {
      return doc
    }

    var paragraph = iter.next()
    var merged = false
    val result = new ArrayBuffer[Paragraph]()
    val textBuffer = new ArrayBuffer[String]()
    var exactFreq = paragraph.exactFreq
    var nearFreq = paragraph.nearFreq

    while (iter.hasNext) {
      val nextParagraph = iter.next()
      val isList = nextParagraph.containsTags(acceptedTags)
      if (
        paragraph.isAlive && nextParagraph.isAlive && isList && paragraph.path == nextParagraph.path
      ) {
        merged = true
        textBuffer += paragraph.text
        exactFreq = math.min(exactFreq, nextParagraph.exactFreq)
        nearFreq = math.min(nearFreq, nextParagraph.nearFreq)
      } else {
        if (textBuffer.nonEmpty) {
          textBuffer += paragraph.text
          val mergedText = textBuffer.mkString("- ", "\n- ", "")
          result += Paragraph(
            path = paragraph.path,
            text = mergedText,
            index = result.size,
            exactFreq = exactFreq,
            nearFreq = nearFreq
          )
          textBuffer.clear()
        } else {
          result += paragraph.copy(index = result.size)
        }

        exactFreq = nextParagraph.exactFreq
        nearFreq = nextParagraph.nearFreq
      }

      paragraph = nextParagraph
    }

    if (merged) {
      if (textBuffer.nonEmpty) {
        textBuffer += paragraph.text
        val mergedText = textBuffer.mkString("- ", "\n- ", "")
        result += Paragraph(
          path = paragraph.path,
          text = mergedText,
          index = result.size,
          exactFreq = exactFreq,
          nearFreq = nearFreq
        )
      } else {
        result += paragraph.copy(index = result.size)
      }

      doc.copy(paragraphs = result)
    } else {
      doc
    }
  }
}
