package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.{Paragraph, PathSegment}
import com.worksap.nlp.uzushio.lib.filters.base.ParagraphFilter

class NoContentDOM extends ParagraphFilter {
  final private val filteringDomNames: Seq[String] =
    Array("header", "footer", "aside", "nav", "noscript", "form")

  final private val DOMCandidatesForFiliteringClassOrId = Array("div", "p", "ul", "h1")

  final private val filteringFullMatchClassOrIdCandidates: Seq[String] = Array(
    "left-box",
    "blog-title-inner",
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
    "menu",
    "nav",
    "banner",
    "logo",
    "pankuzu",
    "breadcrumb",
    "widget",
    "button",
  )

  final private val filteringFullMatchClassOrIdNames =
    filteringPartialMatchClassOrIdNames ++ filteringFullMatchClassOrIdCandidates ++ filteringFullMatchClassOrIdCandidates
      .map(toCamelCase)

  def toCamelCase(s: String): String = {
    val words = s.split("[_-]")
    words.head + words.tail.map(_.capitalize).mkString
  }

  def partialMatchIds(css: PathSegment): Boolean = {
    if (css.id == null) {
      return false
    }

    filteringPartialMatchClassOrIdNames.exists(name => css.lowerId.contains(name))
  }

  def partialMatchClasses(css: PathSegment): Boolean = {
    filteringPartialMatchClassOrIdNames.exists(name => css.lowerClasses.exists(_.contains(name)))
  }

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
        tagNames.contains(css.tag)
        && fullMatchCandidates.exists(name => css.id == name || css.classes.contains(name))
      ) {
        return true
      }

      if (
        tagNames.contains(css.tag)
        && partialMatchCandidates.exists(name => partialMatchIds(css) || partialMatchClasses(css))
      ) {
        return true
      }
    }
    false
  }

  override def checkParagraph(p: Paragraph): Paragraph = {
    if (
      p.containsTags(filteringDomNames) || containsTagWithIdAndClasses(
        p,
        DOMCandidatesForFiliteringClassOrId,
        filteringFullMatchClassOrIdNames,
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
