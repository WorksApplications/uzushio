package org.sample.corpus.warc

import java.io.IOException
import java.io.OutputStream
import java.io.StringWriter
import java.io.Writer
import org.xml.sax.{Attributes, SAXException}
import org.xml.sax.helpers.DefaultHandler

/** Handler to segment text into paragraphs.
  *
  * The delimiter is used to indicate the start/end of paragraphs. It always
  * locates the start of a line, but may contain additional text after it.
  */
class ParagraphHandler(writer: Writer, val paragraphDelimiter: String)
    extends DefaultHandler {
  // TODO: better flag hangling
  val tagDepth = scala.collection.mutable.Map[String, Int]()

  def this() = this(new StringWriter(), ParagraphHandler.paragraphDelimiter)
  def this(writer: Writer) = this(writer, ParagraphHandler.paragraphDelimiter)
  def this(paragraphDelimiter: String) =
    this(new StringWriter(), paragraphDelimiter)

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

  /** Texts inside these tags will be removed. */
  val ignoreTags = Set("style", "script")

  /** Tags that separate paragraphs.
    *
    * Cup of the that of nwc-toolkit and jusText.
    */
  val blockTags = Set(
    "address",
    "article",
    "aside",
    "blockquote",
    "br",
    "caption",
    "center",
    "col",
    "colgroup",
    "dd",
    "dialog",
    "dir",
    "div",
    "dl",
    "dt",
    "fieldset",
    "figure",
    "footer",
    "form",
    "frame",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "hr",
    "isindex",
    "legend",
    "li",
    "menu",
    "multicol",
    "nav",
    "noframes",
    "noscript",
    "ol",
    "optgroup",
    "option",
    "p",
    "pre",
    "section",
    "table",
    "tbody",
    "td",
    "textarea",
    "tfoot",
    "th",
    "thead",
    "title",
    "tr",
    "ul",
    "xmp"
  )
}
