package com.worksap.nlp.uzushio.lib.html

import com.worksap.nlp.uzushio.lib.html.ParagraphExtractor.{HTML_LINK_END, HTML_LINK_START, HTML_PATH_SEPARATOR, blockTags, cleanString, hasContent, ignoreTags}
import org.apache.commons.lang.StringUtils
import org.xml.sax.Attributes
import org.xml.sax.helpers.DefaultHandler

import java.lang
import java.util.Locale
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** Handler to segment text into paragraphs.
  *
  * The delimiter is used to indicate the start/end of paragraphs. It always
  * locates the start of a line, but may contain additional text after it.
  */
class ParagraphExtractor(
    val paragraphs: ArrayBuffer[String]
) extends DefaultHandler {
  private var ignoreLevel = 0
  private val writer = new StringBuilder()
  private var tag_path = mutable.Stack[String]()
  private var per_tag_path_str = ""

  private def ignoreText: Boolean = ignoreLevel > 0

  override def characters(ch: Array[Char], start: Int, length: Int): Unit = {
    // skip texts inside specific tags
    if (ignoreText) { return }
    writer.appendAll(ch, start, length)
  }

  override def ignorableWhitespace(ch: Array[Char], start: Int, length: Int): Unit = {
    characters(ch, start, length);
  }

  override def startElement(
      uri: String,
      localName: String,
      qName: String,
      atts: Attributes
  ): Unit = {
    val q = qName.toLowerCase(Locale.ROOT)
    val id = atts.getValue("id")
    val classes = atts.getValue("class")

    var tag_path_str = s"$q"
    if (classes != null) {
      tag_path_str += s".${classes.split(" ").mkString(".")}"
    }
    if (id != null) {
      tag_path_str += s"#$id"
    }
    tag_path.push(tag_path_str)

    if (blockTags.contains(q)) {
      pushParagraph(per_tag_path_str)
      per_tag_path_str = tag_path.reverse.mkString(">")
    }

    if (ignoreTags.contains(q)) {
      ignoreLevel += 1
    }

    if ("br" == q) {
      writer.append("\n")
    }
    else if ("a" == q) {
      writer.append(HTML_LINK_START)
    }
  }

  override def endElement(uri: String, localName: String, qName: String): Unit = {
    val q = qName.toLowerCase(Locale.ROOT)

    if (blockTags.contains(q)) {
      pushParagraph(tag_path.reverse.mkString(">"))
    }
    tag_path.pop()

    if (ignoreTags.contains(q)) {
      ignoreLevel -= 1
    }

    if ("a" == q) {
      writer.append(HTML_LINK_END)
    }
  }

  override def endDocument(): Unit = {
    pushParagraph("")
  }

  private def pushParagraph(tag_path_str: String): Unit = {
    val str = cleanString(writer.result())
    if (hasContent(str)) {
      paragraphs += tag_path_str + HTML_PATH_SEPARATOR + str
    }
    writer.clear()
  }

  override def toString: String = { paragraphs.mkString("", "\n", writer.result()) }
}

object ParagraphExtractor {
  private final val emptyLinks = "\u0002[\u0000-\u0001\u0004-\u0020\u00a0]*\u0003".r
  private final val spacesRegex = "[\u0000-\u0001\u0004-\u0020\u00a0]+".r

  final val HTML_PATH_SEPARATOR: Char = 0x1c // ASCII FIELD SEPARATOR
  final val HTML_LINK_START: Char = 0x02 // ASCII TEXT START
  final val HTML_LINK_END: Char = 0x03 // ASCII TEXT END
  final val FULLWIDTH_SPACE = '　'

  def cleanString(str: String): String = {
    cleanLinks(str).split('\n').map { s =>
      StringUtils.strip(spacesRegex.replaceAllIn(s, " "))
    }.filter(_.nonEmpty).mkString("\n")
  }

  private def cleanLinks(x: String): String = {
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

  /** Texts inside these tags will be removed. */
  private val ignoreTags = Set("style", "script")

  /** Tags that separate paragraphs. */
  private val blockTags = Set(
    "address",
    "article",
    "aside",
    "blockquote",
    // "br", // should not separate paragraph
    "caption",
    "center",
    // "col", // used inside table
    // "colgroup", // used inside table
    // "dd", // concat datalist contents
    "dialog",
    "dir", // concat list contents (deprecated in HTML5, instead use "ul")
    "div",
    "dl", // concat datalist contents
    // "dt", // concat datalist contents
    "fieldset",
    "figure", // may contain "figcaption"
    "footer",
    "form",
    "frame", // deprecated in HTML5
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "hr",
    // "isindex", // deprecated in HTML5, instead use "input"
    "legend",
    // "li", // concat list contents
    "main",
    // "menu", // only FireFox supports
    // "multicol", // only NetscapeNavigator supports
    "nav",
    "noframes", // deprecated in HTML5
    "noscript",
    "ol", // concat list contents
    "optgroup",
    "option",
    "p",
    "pre",
    "section",
    "table", // concat table contents
    // "tbody", // must contain tr
    // "td", // concat table contents
    "textarea",
    // "tfoot", // must contain tr
    // "th", // concat table contents
    // "thead", // must contain tr
    "title",
    // "tr", // concat table contents
    "ul", // concat list contents
    "xmp" // deprecated in HTML5, instead use "pre"
  )
}
