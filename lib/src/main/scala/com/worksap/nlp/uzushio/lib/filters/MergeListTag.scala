package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.filters.base.DocFilter
import com.worksap.nlp.uzushio.lib.cleaning.Document

class MergeListTag extends DocFilter {
  final val acceptedTags = Seq("li", "option")
  final val mdListSymbol = "- "

  def containsAcceptedTag(cssSelectorStrs: Seq[String]): Boolean = {
    extractDescendantTag(cssSelectorStrs, acceptedTags) match {
      case Some(_) => true
      case None => false
    }
  }

  def extractDescendantTag(
      cssSelectorStrs: Seq[String],
      tagNames: Seq[String]
  ): Option[String] = {
    val iter = cssSelectorStrs.reverse.iterator
    var i = 0

    while (iter.hasNext) {
      val tagWithCSS = iter.next()
      val tagWithAttrs = tagWithCSS.split("[#\\.]")
      i += 1
      if (acceptedTags.contains(tagWithAttrs.head)) {
        return Option(tagWithCSS)
      }
    }
    return None
  }

  override def checkDocument(doc: Document): Document = {
    var paragraphs = doc.aliveParagraphs.to[Seq]

    (0 until paragraphs.length - 1).foreach { i =>
      val paragraph = paragraphs(i)
      val nextParagraph = paragraphs(i + 1)
      val isAccteptedTags = containsAcceptedTag(paragraph.cssSelectors) && containsAcceptedTag(
        nextParagraph.cssSelectors
      )

      if (isAccteptedTags && paragraph.path == nextParagraph.path) {
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
