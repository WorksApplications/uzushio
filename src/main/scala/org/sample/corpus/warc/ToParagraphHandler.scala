package org.sample.corpus.warc

import java.io.IOException
import java.io.OutputStream
import java.io.OutputStreamWriter
import java.io.StringWriter
import java.io.UnsupportedEncodingException
import java.io.Writer
import java.nio.charset.Charset

import org.xml.sax.{ContentHandler, Attributes, SAXException}
import org.xml.sax.helpers.DefaultHandler

class ToParagraphHandler(writer: Writer) extends DefaultHandler {

  val paragraphTags = List("p")
  val ignoreTags = List("script")
  val tagDepthMap = scala.collection.mutable.Map[String, Int]()

  def this() = {
    this(new StringWriter())
  }

  override def characters(ch: Array[Char], start: Int, length: Int): Unit = {
    if (tagDepthMap.values.fold(0)(_ + _) > 0) {
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

  override def endDocument() = {
    try {
      writer.flush()
    } catch {
      case e: IOException => {
        throw new SAXException("Error flushing character output", e);
      }
    }
  }

  override def startElement(
      uri: String,
      localName: String,
      qName: String,
      atts: Attributes
  ) = {
    // todo: rm
    writer.write(s"\nse: ${uri}, ${localName}, ${qName}, ${atts}\n")

    val q = qName.toLowerCase()
    tagDepthMap.update(q, tagDepthMap.getOrElse(q, 0) + 1)
  }

  override def endElement(uri: String, localName: String, qName: String) = {
    // todo: rm
    writer.write(s"\nee: ${uri}, ${localName}, ${qName}, ${atts}\n")

    val q = qName.toLowerCase()
    tagDepthMap.update(q, tagDepthMap.getOrElse(q, 0) - 1)
  }

  override def toString() = { writer.toString() }
}
