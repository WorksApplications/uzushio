package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.Paragraph
import com.worksap.nlp.uzushio.lib.filters.base.ParagraphFilter

class MarkdownizeHeading extends ParagraphFilter {
  final val acceptedTags = Seq("h1", "h2", "h3", "h4", "h5", "h6")
  final val mdHeadningSymbol = "#"

  def tagToMarkdownSymbol(tag: String): String = {
    val numHeading = acceptedTags.indexOf(tag) + 1

    if (numHeading == 0) {
      throw new IllegalArgumentException(s"tag $tag is not heading")
    }

    mdHeadningSymbol * numHeading + " "
  }

  override def checkParagraph(p: Paragraph): Paragraph = {
    val tagWithCSS = p.extractDescendantTag(acceptedTags)
    tagWithCSS match {
      case Some(v) => p.copy(text = tagToMarkdownSymbol(v) + p.text)
      case None => p
    }
  }
}
