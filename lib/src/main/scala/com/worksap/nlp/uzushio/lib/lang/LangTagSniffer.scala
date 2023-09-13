package com.worksap.nlp.uzushio.lib.lang

import com.worksap.nlp.uzushio.lib.lang.LangTagSniffer.{
  extractCharset,
  metaRegex
}

import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.nio.{ByteBuffer, CharBuffer}
import java.util.regex.Pattern

case class LangTagSniff(charset: String, language: String)

/** Try to sniff language and encoding by decoding first 10k bytes as ASCII and
  * using regexes to find `<meta>` tags.
  */
class LangTagSniffer() {
  private val decoder = {
    val dec = StandardCharsets.US_ASCII.newDecoder()
    dec.onMalformedInput(CodingErrorAction.REPLACE)
    dec
  }

  private val charBuf = CharBuffer.allocate(10 * 1024)

  private def doSniff(buffer: CharBuffer): LangTagSniff = {
    var charset = ""
    var language = ""
    val iter = metaRegex.findAllIn(buffer)
    while (iter.hasNext) {
      val metaTag = iter.next()
      val cs = extractCharset(metaTag)
      if (cs.nonEmpty) {
        charset = cs
      }

    }
    LangTagSniff(charset, language)
  }

  def sniffTags(data: ByteBuffer): LangTagSniff = {
    val pos = data.position()
    val lim = data.limit()

    charBuf.clear()
    val res = decoder.decode(data, charBuf, false)
    charBuf.flip()

    data.position(pos)
    data.limit(lim)
    doSniff(charBuf)
  }

  def sniffTags(data: Array[Byte], offset: Int, position: Int): LangTagSniff = {
    val buffer = ByteBuffer.wrap(data, offset, position)
    sniffTags(buffer)
  }
}

object LangTagSniffer {
  private val metaRegex = "<meta[^>]*>".r
  private val charsetRegex =
    Pattern.compile("charset=([^\"' >]+)", Pattern.CASE_INSENSITIVE)

  def extractCharset(tag: String): String = {
    val matcher = charsetRegex.matcher(tag)
    if (matcher.find()) {
      matcher.group(1)
    } else {
      ""
    }
  }
}
