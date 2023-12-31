package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.Paragraph
import com.worksap.nlp.uzushio.lib.filters.base.ParagraphFilter

class NoContentDOM extends ParagraphFilter {
  // This names are tag names, but also class names and id names
  final private val filteringDomNames: Seq[String] =
    Array("header", "footer", "aside", "nav", "left-box", "sidebar")

  // I checked some of the Common Crawl extracts and noticed that `div#header` and `div.nav` are also often used instead of `<header>` and `<nav>`.
  def containsTagWithIdAndClasses(
      p: Paragraph,
      tagName: String,
      classOrIdNames: Seq[String]
  ): Boolean = {
    val iter = p.cssPath.reverseIterator

    while (iter.hasNext) {
      val css = iter.next()
      if (
        classOrIdNames
          .exists(name => css.tag == tagName && (css.id == name || css.classes.contains(name)))
      ) {
        return true
      }
    }
    false
  }

  override def checkParagraph(p: Paragraph): Paragraph = {
    if (
      p.containsTags(filteringDomNames) || containsTagWithIdAndClasses(p, "div", filteringDomNames)
    ) {
      p.copy(remove = this)
    } else {
      p
    }
  }

  override def toString: String = "Nav"
}
