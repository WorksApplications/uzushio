package com.worksap.nlp.uzushio.lib.lang

import com.github.pemistahl.lingua.api.{Language, LanguageDetectorBuilder}

import java.nio.charset.{Charset, CodingErrorAction}
import java.nio.{ByteBuffer, CharBuffer}

sealed trait EstimationResult {
  def str: String = "unk"
}
case object BadEncoding extends EstimationResult
case object EstimationFailure extends EstimationResult
case class ProbableLanguage(lang: String) extends EstimationResult {
  override def str: String = lang
}

class LangEstimation(private val minBytes: Int = 1024) {
  private val internalBuffer = CharBuffer.allocate(5 * 1024)
  private val decodeBuffer = CharBuffer.allocate(4 * 1024)
  private val langDetector = LanguageDetectorBuilder.fromAllLanguages().build()


  private def copyNonAscii(input: CharBuffer, output: CharBuffer) = {
    var prevWhitespace = false
    while (input.hasRemaining && output.remaining() > 1) {
      val char = input.get()
      if ((char & 0xffff) >= 128) {
        if (prevWhitespace) {
          output.put(' ')
          prevWhitespace = false
        }
        output.put(char)
      } else {
        prevWhitespace = true
      }
    }
  }

  private def prepareBuffer(bytes: Array[Byte], offset: Int, charset: Charset): Option[Int] = {
    val decBuf = decodeBuffer
    val buf = internalBuffer
    val inputData = ByteBuffer.wrap(bytes, offset, (bytes.length - offset).min(20 * 1024))
    val decoder = charset.newDecoder().onUnmappableCharacter(CodingErrorAction.REPORT)
    decBuf.clear()
    buf.clear()

    while (inputData.remaining() > 0 && buf.remaining() > 0) {
      val result = decoder.decode(inputData, decBuf, true)
      if (result.isUnmappable || result.isError || result.isMalformed) {
        return None
      }
      decBuf.flip()
      copyNonAscii(decBuf, buf)
      decBuf.clear()
    }

    buf.flip()
    Some(buf.limit())
  }



  def estimateLang(data: Array[Byte], offset: Int, charset: Charset): EstimationResult = {
    val bufferStatus = prepareBuffer(data, offset, charset)
    if (bufferStatus.isEmpty) {
      return BadEncoding
    }
    val ncopied = bufferStatus.get
    if (ncopied > minBytes) {
      val language = langDetector.detectLanguageOf(internalBuffer.toString)
      if (language == Language.UNKNOWN) {
        EstimationFailure
      } else {
        val code = language.getIsoCode639_1.toString
        ProbableLanguage(code)
      }
    } else {
      EstimationFailure
    }
  }
}
