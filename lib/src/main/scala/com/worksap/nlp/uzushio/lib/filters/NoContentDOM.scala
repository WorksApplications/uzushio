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
    val words = s.split("[_-]")
    words.head + words.tail.map(_.capitalize).mkString
  }

  def partialMatchIds(css: PathSegment): Boolean = {
    if (css.id == null) {
      return false
    }

    val id_segments = css.id.split("[_-]")

    (id_segments.toSet & filteringPartialMatchClassOrIdNames.toSet).nonEmpty ||
    filteringPartialMatchClassOrIdNames.exists(name => css.id.capitalize.contains(name.capitalize))
  }

  def partialMatchClasses(css: PathSegment): Boolean = {
    val class_segments = css.classes.flatMap(_.split("[_-]"))

    (class_segments.toSet & filteringPartialMatchClassOrIdNames.toSet).nonEmpty ||
    (class_segments.map(_.capitalize).toSet & filteringPartialMatchClassOrIdNames.map(_.capitalize)
      .toSet).nonEmpty
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
        fullMatchCandidates
          .exists(name => tagNames.contains(css.tag) && (css.id == name || css.classes.contains(name)))
      ) {
        return true
      }

      // check filtering keywords in snake case, camel case and kebab case
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
