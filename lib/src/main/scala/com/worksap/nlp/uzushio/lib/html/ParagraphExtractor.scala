package com.worksap.nlp.uzushio.lib.html

import com.worksap.nlp.uzushio.lib.html.ParagraphExtractor.{
  MAX_PATH_SIZE,
  blockTags,
  ignoreTags
}
import com.worksap.nlp.uzushio.lib.utils.Paragraphs
import org.xml.sax.Attributes
import org.xml.sax.helpers.DefaultHandler

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
  private val tag_path = mutable.ArrayStack[String]()
  private var per_tag_path_str = ""

  private def ignoreText: Boolean = ignoreLevel > 0

  override def characters(ch: Array[Char], start: Int, length: Int): Unit = {
    // skip texts inside specific tags
    if (ignoreText) { return }
    writer.appendAll(ch, start, length)
  }

  override def ignorableWhitespace(
      ch: Array[Char],
      start: Int,
      length: Int
  ): Unit = {
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
    if (tag_path.length > MAX_PATH_SIZE) {
      throw new ParseAbortException()
    }

    if (blockTags.contains(q)) {
      pushParagraph(per_tag_path_str)
      per_tag_path_str = tag_path.reverse.mkString(">")
    }

    if (ignoreTags.contains(q)) {
      ignoreLevel += 1
    }

    if ("br" == q) {
      writer.append("\n")
    } else if ("a" == q) {
      writer.append(Paragraphs.HTML_LINK_START)
    }
  }

  override def endElement(
      uri: String,
      localName: String,
      qName: String
  ): Unit = {
    val q = qName.toLowerCase(Locale.ROOT)

    if (blockTags.contains(q)) {
      pushParagraph(tag_path.reverse.mkString(">"))
    }
    tag_path.pop()

    if (ignoreTags.contains(q)) {
      ignoreLevel -= 1
    }

    if ("a" == q) {
      writer.append(Paragraphs.HTML_LINK_END)
    }
  }

  override def endDocument(): Unit = {
    pushParagraph("")
  }

  private def pushParagraph(tag_path_str: String): Unit = {
    val str = Paragraphs.cleanParagraph(writer.result())
    if (Paragraphs.hasContent(str)) {
      paragraphs += tag_path_str + Paragraphs.HTML_PATH_SEPARATOR + str
    }
    writer.clear()
  }

  override def toString: String = {
    paragraphs.mkString("", "\n", writer.result())
  }
}

object ParagraphExtractor {

  final val MAX_PATH_SIZE = 256

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
