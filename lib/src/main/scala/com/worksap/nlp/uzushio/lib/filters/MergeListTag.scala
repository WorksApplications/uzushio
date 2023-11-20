package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.filters.base.DocFilter
import com.worksap.nlp.uzushio.lib.cleaning.Document

class MergeListTag extends DocFilter {
  final val acceptedTags = Seq("li", "option")
  final val mdListSymbol = "- "

  override def checkDocument(doc: Document): Document = {
    var paragraphs = doc.aliveParagraphs.to[Seq]

    (0 until paragraphs.length - 1).foreach { i =>
      val paragraph = paragraphs(i)
      val nextParagraph = paragraphs(i + 1)
      val isAccteptedTags = paragraph.containsTags(acceptedTags) && nextParagraph
        .containsTags(acceptedTags)

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
