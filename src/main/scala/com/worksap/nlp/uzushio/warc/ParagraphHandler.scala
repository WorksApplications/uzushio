package com.worksap.nlp.uzushio.warc

import java.io.IOException
import java.io.OutputStream
import java.io.StringWriter
import java.io.Writer
import org.xml.sax.{Attributes, SAXException}
import org.xml.sax.helpers.DefaultHandler
import scala.util.matching.Regex

/** Handler to segment text into paragraphs.
  *
  * The delimiter is used to indicate the start/end of paragraphs. It always
  * locates the start of a line, but may contain additional text after it.
  */
class ParagraphHandler(
    writer: Writer = new StringWriter(),
    val paragraphDelimiter: String = ParagraphHandler.paragraphDelimiter
) extends DefaultHandler {
  // TODO: better flag hangling
  val tagDepth = scala.collection.mutable.Map[String, Int]()

  override def characters(ch: Array[Char], start: Int, length: Int): Unit = {
    // skip texts inside specific tags
    if (
      ParagraphHandler.ignoreTags
        .map(k => tagDepth.getOrElse(k, 0))
        .reduce(_ + _) > 0
    ) { return }

    try {
      writer.write(ch, start, length)
    } catch {
      case e: IOException => {
        new SAXException(s"Error writing: ${new String(ch, start, length)}", e)
      }
    }
  }

  override def ignorableWhitespace(ch: Array[Char], start: Int, length: Int) = {
    characters(ch, start, length);
  }

  override def startElement(
      uri: String,
      localName: String,
      qName: String,
      atts: Attributes
  ) = {
    val q = qName.toLowerCase()

    if (ParagraphHandler.ignoreTags.contains(q)) {
      tagDepth.update(q, tagDepth.getOrElse(q, 0) + 1)
    }

    if (ParagraphHandler.blockTags.contains(q)) {
      try {
        writer.write(s"\n${paragraphDelimiter} ${q} start\n")
      } catch {
        case e: IOException => {
          throw new SAXException("Error writing block start delimiter", e);
        }
      }
    }
  }

  override def endElement(uri: String, localName: String, qName: String) = {
    val q = qName.toLowerCase()

    if (ParagraphHandler.ignoreTags.contains(q)) {
      val v = tagDepth.getOrElse(q, 0)
      tagDepth.update(q, math.max(0, v - 1))
    }

    if (ParagraphHandler.blockTags.contains(q)) {
      try {
        writer.write(s"\n${paragraphDelimiter} ${q} end\n")
      } catch {
        case e: IOException => {
          throw new SAXException("Error writing block end delimiter", e);
        }
      }
    }
  }

  override def endDocument() = {
    try {
      writer.flush()
    } catch {
      case e: IOException => {
        throw new SAXException("Error flushing character output", e);
      }
    }
  }

  override def toString() = { writer.toString() }
}

object ParagraphHandler {

  /** Default paragraph delimiter. */
  val paragraphDelimiter = "CORPUS_PARAGRAPH_DELIMITER"

  /** Pattern to split text at line with paragraph delimiter.
    *
    * The delimiter may contain additional information after it. We need (?m) to
    * make ^/$ match all newlines.
    */
  def toDelimPattern(delim: String) = s"(?m)^${paragraphDelimiter}.+$$".r
  val paragraphDelimiterPattern = toDelimPattern(paragraphDelimiter)

  /** Clean parsed string (trimming). */
  def toCleanString(
      target: String,
      delimPattern: Regex = paragraphDelimiterPattern,
      outDelim: String = "\n\n"
  ) = {
    // split into paragraphs
    delimPattern
      .split(target)
      // trim each lines and rm empty ones
      .map(p => p.split("\n").map(_.trim).filter(_.nonEmpty).mkString("\n"))
      // rm empty paragraphs
      .filter(_.nonEmpty)
      .mkString(outDelim)
  }

  /** Texts inside these tags will be removed. */
  val ignoreTags = Set("style", "script")

  /** Tags that separate paragraphs. */
  val blockTags = Set(
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
