package com.worksap.nlp.uzushio.lib.utils

import org.apache.commons.lang3.StringUtils

import java.lang

object Paragraphs {

  final val PARAGRAPH_SEP = "\n\n"
  final val HTML_PATH_SEPARATOR: Char = 0x1c // ASCII FIELD SEPARATOR
  final val FULLWIDTH_SPACE: Char = '　'



  def extractCleanParagraphs(text: String): Seq[String] = {
    val paragraphs = StringUtils.split(text, PARAGRAPH_SEP)
    paragraphs.flatMap { x =>
      val par = extractCleanParagraph(x)
      if (hasContent(par)) Some(par) else None
    }
  }

  def extractCleanParagraph(text: String): String = {
    val (_, par) = splitPath(text)
    removeLinks(par)
  }

  def splitPath(text: String): (String, String) = {
    val idx = text.indexOf(HTML_PATH_SEPARATOR)
    if (idx < 0) {
      ("", text)
    } else {
      val path = text.substring(0, idx)
      val par = text.substring(idx + 1)
      (path, par)
    }
  }

  def hasContent(seq: CharSequence): Boolean = {
    var i = 0
    val len = seq.length()
    while (i < len) {
      val c = seq.charAt(i)
      if (c > 0x20 && c != FULLWIDTH_SPACE) {
        return true
      }
      i += 1
    }
    false
  }

  def removeLinks(text: String): String = {
    val noLinkMarkers = linkChars.replaceAllIn(text, "")
    cleanParagraph(noLinkMarkers)
  }

  private final val spacesRegex = "[\u0000-\u0001\u0004-\u0020\u00a0]+".r
  private final val emptyLinks = "\u0002[\u0000-\u0001\u0004-\u0020\u00a0]*\u0003".r
  private final val linkChars = "[\u0002\u0003]".r

  final val HTML_LINK_START: Char = 0x02 // ASCII TEXT START
  final val HTML_LINK_END: Char = 0x03 // ASCII TEXT END


  private def cleanInsideLinks(x: String): String = {
    val idx = x.indexOf('\u0002')
    val noBreaks = if (idx < 0) {
      x
    } else {
      cleanLinksImpl(new lang.StringBuilder(x), idx)
    }
    emptyLinks.replaceAllIn(noBreaks, "")
  }


  private def cleanLinksImpl(builder: lang.StringBuilder, start: Int): String = {
    var idx = start
    val end = builder.length()
    var inside = false

    while (idx < end) {
      val c = builder.charAt(idx)
      if (inside) {
        c match {
          case HTML_LINK_END => inside = false
          case _ if c < 0x20 || c == 0xa0 => builder.setCharAt(idx, ' ')
          case _ => // do nothing
        }

      } else if (c == HTML_LINK_START) {
        inside = true
      }
      idx += 1
    }

    builder.toString
  }

  def cleanParagraph(str: String): String = {
    StringUtils.split(cleanInsideLinks(str), '\n').map { s =>
      val collapsedSpaces = spacesRegex.replaceAllIn(s, " ")
      StringUtils.strip(collapsedSpaces)
    }.filter(Paragraphs.hasContent).mkString("\n")
  }


}
