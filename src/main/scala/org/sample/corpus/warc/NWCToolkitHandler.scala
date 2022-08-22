package org.sample.corpus.warc

import java.io.IOException
import java.io.StringWriter
import java.io.Writer

import org.xml.sax.{ContentHandler, Attributes, SAXException}
import org.xml.sax.helpers.DefaultHandler

class NWCToolkitHandler(writer: Writer) extends DefaultHandler {
  // TODO: better flag hangling
  val tagDepth = scala.collection.mutable.Map[String, Int]()

  def this() = this(new StringWriter())

  override def characters(ch: Array[Char], start: Int, length: Int): Unit = {
    if (
      NWCToolkitHandler.invisibleModeTags
        .map(k => tagDepth.getOrElse(k, 0))
        .reduce(_ + _) > 0
    ) {
      return
    }

    var keepNewLine = false
    if (
      (NWCToolkitHandler.plainModeTags | NWCToolkitHandler.preModeTags)
        .map(k => tagDepth.getOrElse(k, 0))
        .reduce(_ + _) > 0
    ) { keepNewLine = true }

    try {
      Range(start, start + length).foreach(i => {
        val crr = ch(i)
        if (
          (keepNewLine && NWCToolkitHandler.newlineChars.contains(crr)) ||
          !crr.isControl
        ) {
          writer.write(crr)
        }
      })
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
    tagDepth.update(q, 1)

    if (NWCToolkitHandler.blockTags.contains(q)) { writer.write("\n") }
  }

  override def endElement(uri: String, localName: String, qName: String) = {
    val q = qName.toLowerCase()
    tagDepth.update(q, 0)

    if (NWCToolkitHandler.blockTags.contains(q)) { writer.write("\n") }
  }

  override def endDocument() = {
    try {
      writer.write("\n")
      writer.flush()
    } catch {
      case e: IOException => {
        throw new SAXException("Error flushing character output", e);
      }
    }
  }

  override def toString() = { writer.toString() }
}

object NWCToolkitHandler {
  val newlineChars = "\r\n".toCharArray()

  val invisibleModeTags = Set("script", "style")
  val plainModeTags = Set("script", "style", "xmp", "plaintext")
  val preModeTags = Set("pre", "listing", "textarea")

  val blockTags = Set(
    "address",
    "article",
    "aside",
    "blockquote",
    "br",
    "caption",
    "center",
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
    "legenda",
    "li",
    "menu",
    "multicol",
    "nav",
    "noframes",
    "noscript",
    "ol",
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
