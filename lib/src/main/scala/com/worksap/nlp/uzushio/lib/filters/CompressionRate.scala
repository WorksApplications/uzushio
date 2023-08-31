package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.{DocFilter, Document}
import com.worksap.nlp.uzushio.lib.filters.CompressionRate.{INPUT_SIZE, OUTPUT_SIZE}
import net.jpountz.lz4.{LZ4Exception, LZ4Factory}

import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.StandardCharsets

/**
 * Filter out documents which have too low or too high compression rate
 *
 * @param low  low compression rate threshold
 * @param high high compression rate threshold
 */
class CompressionRate(low: Float, high: Float) extends DocFilter {
  @transient private lazy val lz4 = LZ4Factory.fastestInstance()
  @transient private lazy val utf8Buffer = ByteBuffer.allocateDirect(INPUT_SIZE)
  @transient private lazy val compressBuffer = ByteBuffer.allocateDirect(OUTPUT_SIZE)

  def encodeDocContent(doc: Document): ByteBuffer = {
    val enc = StandardCharsets.UTF_8.newEncoder()
    val buf = utf8Buffer
    buf.clear()
    val iter = doc.paragraphs.iterator
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
    if (ratio < low) {
      doc.copy(remove = Low)
    } else if (ratio > high) {
      doc.copy(remove = High)
    } else doc
  }

  def compressionRatio(doc: Document): Float = {
    val compressor = lz4.fastCompressor()
    val buf = encodeDocContent(doc)
    val uncompressedSize = buf.limit()
    val outBuf = compressBuffer
    outBuf.clear()
    val compressedSize = try {
      compressor.compress(buf, outBuf)
      outBuf.position()
    } catch {
      case _: LZ4Exception => OUTPUT_SIZE
    }
    val ratio = compressedSize.toFloat / uncompressedSize.toFloat
    ratio
  }

  object Low {
    override val toString: String = s"CompressionRate.Low($low)"
  }

  object High {
    override val toString: String = s"CompressionRate.High($high)"
  }

  override def toString: String = s"CompressionRate($low, $high)"
}

object CompressionRate {
  final val INPUT_SIZE = 1024 * 1024
  final val OUTPUT_SIZE = 1200 * 1024
}
