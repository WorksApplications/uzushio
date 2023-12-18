package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.Paragraph
import com.worksap.nlp.uzushio.lib.filters.base.ParagraphFilter

class NoContentDOM extends ParagraphFilter {
  final private val filteringDomNames: Seq[String] =
    Array("header", "footer", "aside", "nav", "noscript", "form")

  final private val filteringFullMatchClassOrIdNames: Seq[String] = Array(
    "left-box",
    "blog-title-inner",
    "globalheader",
    "blogtitle",
    "blog-name",
    "head-block1",
    "head-blog-name",
    "head-introduction",
  )

  final private val filteringPartialMatchClassOrIdNames: Seq[String] = Array(
    "header",
    "footer",
    "side",
    "aside",
    "sidebar",
    "menu",
    "nav",
    "navi",
    "navigation",
    "navbar",
    "banner",
    "logo",
    "pankuzu",
    "breadcrumb",
    "breadcrumbs",
    "widget",
    // "profile",
    "button",
  )

  def toCamelCase(s: String): String = {
    val words = s.split("-")
    words.head + words.tail.map(_.capitalize).mkString
  }

  // I checked some of the Common Crawl extracts and noticed that `div#header` and `div.nav` are also often used instead of `<header>` and `<nav>`.
  def containsTagWithIdAndClasses(
      p: Paragraph,
      tagNames: Seq[String],
      fullMatchCandidates: Seq[String],
      partialMatchCandidates: Seq[String]
  ): Boolean = {
    val iter = p.cssPath.reverseIterator

    while (iter.hasNext) {
      val css = iter.next()

      if (
        fullMatchCandidates
          .exists(name => tagNames.contains(css.tag) && (css.id == name || css.classes.contains(name)))
      ) {
        return true
      }

      if (
        partialMatchCandidates.exists(name =>
          tagNames.contains(css.tag)
            && ((css.id != null && (css.id.split("[_-]").contains(name) || css.id.capitalize
              .contains(name.capitalize)))
              || (css.classes.exists(_.split("[_-]").contains(name)) || css.classes
                .exists(_.capitalize.contains(name.capitalize))))
        )
      ) {
        return true
      }
    }
    false
  }

  override def checkParagraph(p: Paragraph): Paragraph = {
    val fullMatchCandidates =
      filteringPartialMatchClassOrIdNames ++ filteringFullMatchClassOrIdNames ++ filteringFullMatchClassOrIdNames
        .map(toCamelCase)
    if (
      p.containsTags(filteringDomNames) || containsTagWithIdAndClasses(
        p,
        Seq("div", "p", "ul", "h1"),
        fullMatchCandidates,
        filteringPartialMatchClassOrIdNames
      )
    ) {
      p.copy(remove = this)
    } else {
      p
    }
  }

  override def toString: String = "Nav"
}
