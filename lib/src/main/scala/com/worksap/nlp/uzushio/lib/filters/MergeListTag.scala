package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.filters.base.DocFilter
import com.worksap.nlp.uzushio.lib.cleaning.{Document, Paragraph}
import scala.collection.mutable.ArrayBuffer

class MergeListTag extends DocFilter {
  final val acceptedTags = Seq("li", "option")
  final val mdListSymbol = "- "

  def containsAcceptedTag(cssSelectorStrs: Seq[String]): Boolean = {
    extractDescendantTagWithParentSelectors(cssSelectorStrs, acceptedTags) match {
      case Some(_) => true
      case None => false
    }
  }

  def extractDescendantTagWithParentSelectors(
      cssSelectorStrs: Seq[String],
      tagNames: Seq[String]
  ): Option[Tuple2[String, Seq[String]]] = {
    val iter = cssSelectorStrs.reverse.iterator
    var i = 0

    while (iter.hasNext) {
      val tagWithCSS = iter.next()
      val tagWithAttrs = tagWithCSS.split("[#\\.]")
      i += 1
      if (acceptedTags.contains(tagWithAttrs.head)) {
        return Option((tagWithCSS, cssSelectorStrs.take(cssSelectorStrs.length - i)))
      }
    }
    return None
  }

  def matchesTagAndParentPath(paragraph1: Paragraph, paragraph2: Paragraph): Boolean = {
    val tagWithCSS1 = extractDescendantTagWithParentSelectors(paragraph1.cssSelectors, acceptedTags)
    val tagWithCSS2 = extractDescendantTagWithParentSelectors(paragraph2.cssSelectors, acceptedTags)

    (tagWithCSS1, tagWithCSS2) match {
      case (Some((tag1, path1)), Some((tag2, path2))) => {
        tag1 == tag2 && path1 == path2
      }
      case _ => false
    }
  }

  override def checkDocument(doc: Document): Document = {
    var paragraphs = doc.aliveParagraphs.to[Seq]

    (0 until paragraphs.length - 1).foreach { i =>
      val paragraph = paragraphs(i)
      val nextParagraph = paragraphs(i + 1)
      val isAccteptedTags = containsAcceptedTag(paragraph.cssSelectors) && containsAcceptedTag(
        nextParagraph.cssSelectors
      )

      if (isAccteptedTags && matchesTagAndParentPath(paragraph, nextParagraph)) {
        val mergedParagraph = nextParagraph.copy(
          text = List(paragraph.text, nextParagraph.text)
            .map(s => if (s.startsWith(mdListSymbol)) s else mdListSymbol + s).mkString("\n"),
          exactFreq = math.min(paragraph.exactFreq, nextParagraph.exactFreq),
          nearFreq = math.min(paragraph.nearFreq, nextParagraph.nearFreq)
        )
        paragraphs = paragraphs.updated(i, paragraph.copy(remove = paragraph))
        paragraphs = paragraphs.updated(i + 1, mergedParagraph)
      }
    }

    doc.copy(paragraphs = paragraphs)
  }
}
