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

  final private val filteringFullMatchClassOrIdNames =
    filteringPartialMatchClassOrIdNames ++ filteringFullMatchClassOrIdCandidates ++ filteringFullMatchClassOrIdCandidates
      .map(toCamelCase)

  def toCamelCase(s: String): String = {
    val words = s.split("-")
    words.head + words.tail.map(_.capitalize).mkString
  }

  def partialMatchIds(css: PathSegment): Boolean = filteringPartialMatchClassOrIdNames
    .exists(name =>
      css.id != null && (css.id.split("[_-]").contains(name) || css.id.capitalize
        .contains(name.capitalize))
    )

  def partialMatchClasses(css: PathSegment): Boolean = filteringPartialMatchClassOrIdNames
    .exists(name =>
      css.classes.exists(_.split("[_-]").contains(name)) || css.classes
        .exists(_.capitalize.contains(name.capitalize))
    )

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

      // checking filtering keywords in snake case and camel case, kebab case
      if (
        partialMatchCandidates.exists(name =>
          tagNames.contains(css.tag) && (partialMatchIds(css) || partialMatchClasses(css))
        )
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
