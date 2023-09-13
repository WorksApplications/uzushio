package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.Document
import com.worksap.nlp.uzushio.lib.filters.CompressionRate.{INPUT_SIZE, OUTPUT_SIZE}
import com.worksap.nlp.uzushio.lib.filters.base.HighLowDocFilter
import net.jpountz.lz4.{LZ4Exception, LZ4Factory}

import java.nio.charset.StandardCharsets
import java.nio.{ByteBuffer, CharBuffer}

/** Filter out documents which have too low or too high compression rate (using LZ4 algorithm)
  *
  * @param low
  *   low compression rate threshold
  * @param high
  *   high compression rate threshold
  */
class CompressionRate(override val low: Float, override val high: Float) extends HighLowDocFilter {
  @transient private lazy val lz4 = LZ4Factory.fastestInstance()
  @transient private lazy val utf8Buffer = ByteBuffer.allocateDirect(INPUT_SIZE)
  @transient private lazy val compressBuffer = ByteBuffer.allocateDirect(OUTPUT_SIZE)

  def encodeDocContent(doc: Document): ByteBuffer = {
    val enc = StandardCharsets.UTF_8.newEncoder()
    val buf = utf8Buffer
    buf.clear()
    val iter = doc.aliveParagraphs
    while (iter.hasNext) {
      val p = iter.next()
      val cbuf = CharBuffer.wrap(p.text)
      val res = enc.encode(cbuf, buf, true)
      if (res.isOverflow) {
        // Scala does not has nice break/continue :/
        buf.flip()
        return buf
      }
    }
    buf.flip()
    buf
  }

  override def checkDocument(doc: Document): Document = {
    val ratio: Float = compressionRatio(doc)
    maybeFilter(doc, ratio)
  }

  def compressionRatio(doc: Document): Float = {
    val compressor = lz4.fastCompressor()
    val buf = encodeDocContent(doc)
    val uncompressedSize = buf.limit()
    val outBuf = compressBuffer
    outBuf.clear()
    val compressedSize =
      try {
        compressor.compress(buf, outBuf)
        outBuf.position()
      } catch {
        case _: LZ4Exception => OUTPUT_SIZE
      }
    val ratio = compressedSize.toFloat / uncompressedSize.toFloat
    ratio
  }
}

object CompressionRate {
  final val INPUT_SIZE = 1024 * 1024
  final val OUTPUT_SIZE = 1200 * 1024
}
