package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.filters.base.DocFilter
import com.worksap.nlp.uzushio.lib.cleaning.{Document, Paragraph}
import scala.collection.mutable.{ArrayBuffer, Queue}

class MergeListTag extends DocFilter {
  final val listTagString = "li"
  final val mdListSymbol = "- "

  def containsListTag(cssSelectorStrs: Seq[String]): Boolean = {
    extractDescendantTag(cssSelectorStrs, listTagString) match {
      case Some(_) => true
      case None => false
    }
  }

  def extractDescendantTag(cssSelectorStrs: Seq[String], tagName: String): Option[String] = {
    val iter = cssSelectorStrs.reverse.iterator

    while (iter.hasNext) {
      val tagWithCSS = iter.next()
      val tagWithAttrs = tagWithCSS.split("[#\\.]")
      if (tagWithAttrs.head == tagName) {
        return Option(tagWithCSS)
      }
    }
    return None
  }

  def mergeListParagraphs(listTagParagraphs: Seq[Paragraph]): Seq[Paragraph] = {
    val listParsGroup = listTagParagraphs
      .groupBy(par => extractDescendantTag(par.cssSelectors, listTagString).get)

    listParsGroup.map {
      case (tagName, groupedPars) => {
        val exactFreqs = groupedPars.map(par => par.exactFreq)
        val nearFreqs = groupedPars.map(par => par.nearFreq)
        val listTexts = groupedPars.map(par => mdListSymbol + par.text)
        val text = listTexts.mkString("\n")
        val mergedListParagraph = groupedPars.head
          .copy(text = text, exactFreq = exactFreqs.min, nearFreq = nearFreqs.min)

        // for consistency of paragraph index
        val tail = groupedPars.tail.map(par => par.copy(remove = par))
        mergedListParagraph +: tail
      }
    }.flatten.to[Seq]
  }

  def findConsecutiveListTag(paragraphs: Seq[Paragraph]): Seq[Paragraph] = {
    var isListBlock = true
    val listTagParagraphs = ArrayBuffer.empty[Paragraph]
    val iter = paragraphs.iterator
    while (isListBlock && iter.hasNext) {
      val paragraph = iter.next
      isListBlock = containsListTag(paragraph.cssSelectors)
      if (isListBlock) {
        listTagParagraphs.append(paragraph)
      }
    }
    listTagParagraphs
  }

  override def checkDocument(doc: Document): Document = {
    val paragraphs = doc.aliveParagraphs.to[Seq]
    val iter = paragraphs.iterator.zipWithIndex
    val newParagraphs = ArrayBuffer.empty[Paragraph]
    val listTagParagraphQueue = Queue[Paragraph]()

    while (iter.hasNext) {
      val (paragraph, i) = iter.next
      val cssSelectorStrs = paragraph.cssSelectors

      if (listTagParagraphQueue.isEmpty && containsListTag(cssSelectorStrs)) {
        val listTagParagraphs = findConsecutiveListTag(paragraphs.drop(i))
        listTagParagraphQueue ++= mergeListParagraphs(listTagParagraphs)
        newParagraphs += listTagParagraphQueue.dequeue
      } else if (!listTagParagraphQueue.isEmpty) {
        // if queue is not empty, add merged paragraph or remove-signed one
        newParagraphs += listTagParagraphQueue.dequeue
      } else {
        newParagraphs += paragraph.copy()
      }
    }

    doc.copy(paragraphs = newParagraphs)
  }
}
