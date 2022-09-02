package org.sample.corpus.warc

import java.io.IOException
import java.io.StringWriter
import java.io.Writer

import org.xml.sax.{ContentHandler, Attributes, SAXException}
import org.xml.sax.helpers.DefaultHandler

/** Handler that mimics the behaviour of jusText segmentation.
  *
  * You need to use this with AllTagMapper to utilize all tag information.
  *
  * ref: [jusText](http://corpus.tools/wiki/Justext)
  */
class JusTextHandler(
    writer: Writer = new StringWriter(),
    blockDelimiter: String = JusTextHandler.blockDelimiter
) extends DefaultHandler {
  // TODO: better flag hangling
  val tagDepth = scala.collection.mutable.Map[String, Int]()
  var brTagCount: Int = 0

  override def characters(ch: Array[Char], start: Int, length: Int): Unit = {
    // skip texts inside specific tags
    if (
      JusTextHandler.ignoreTags
        .map(k => tagDepth.getOrElse(k, 0))
        .reduce(_ + _) > 0
    ) {
      return
    }

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
    if (JusTextHandler.ignoreTags.contains(q)) {
      tagDepth.update(q, 1)
    }

    // count sequential (starting) br tag
    if (q == "br") { brTagCount += 1 }
    else { brTagCount = 0 }

    if (JusTextHandler.blockTags.contains(q) || brTagCount == 2) {
      writer.write(s"\n${blockDelimiter} ${q} start\n")
    }
  }

  override def endElement(uri: String, localName: String, qName: String) = {
    val q = qName.toLowerCase()
    if (JusTextHandler.ignoreTags.contains(q)) {
      tagDepth.update(q, 0)
    }

    // count sequential br tag
    if (q != "br") { brTagCount = 0 }

    if (JusTextHandler.blockTags.contains(q)) {
      writer.write(s"\n${blockDelimiter} ${q} end\n")
    }
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

object JusTextHandler {
  val blockDelimiter = "CORPUS_BLOCK_DELIMITER"

  // "blocks containing a copyright symbol (©)" are also removed
  val ignoreTags = Set("header", "style", "script", "select")

  // "A sequence of two or more BR tags also separates blocks"
  val blockTags = Set(
    "blockquote",
    "caption",
    "center",
    "col",
    "colgroup",
    "dd",
    "div",
    "dl",
    "dt",
    "fieldset",
    "form",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "legend",
    "li",
    "optgroup",
    "option",
    "p",
    "pre",
    "table",
    "td",
    "textarea",
    "tfoot",
    "th",
    "thead",
    "tr",
    "ul"
  )
}
