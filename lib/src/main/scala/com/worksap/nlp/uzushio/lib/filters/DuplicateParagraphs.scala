package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.{Paragraph, ParagraphFilter}

class DuplicateParagraphs(limit: Int = 2) extends ParagraphFilter {
  override def checkParagraph(p: Paragraph): Paragraph = {
    if (p.nearFreq >= limit) {
      p.copy(remove = this)
    } else p
  }

  override val toString = s"DuplicateParagraphs($limit)"
}
