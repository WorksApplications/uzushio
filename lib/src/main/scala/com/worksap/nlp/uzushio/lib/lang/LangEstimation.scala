package com.worksap.nlp.uzushio.lib.lang

import com.optimaize.langdetect.LanguageDetectorBuilder
import com.optimaize.langdetect.ngram.NgramExtractor

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

class LangEstimation(private val minBytes: Int = 256) {
  private val internalBuffer = CharBuffer.allocate(5 * 1024)
  private val decodeBuffer = CharBuffer.allocate(4 * 1024)
  private def langDetector = LangEstimation.cachedDetector

  /** Copy non-ASCII characters into detection buffer
    * @param input
    *   input buffer
    * @param output
    *   output buffer
    */
  private def copyNonAscii(input: CharBuffer, output: CharBuffer): Unit = {
    var prevWhitespace = false
    while (input.hasRemaining && output.remaining() > 1) {
      val char = input.get()
      // <asdf> </asdf> <!--- --->
      //problem: inline javascript, inline html
      // take character from the middle of document

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

  private def prepareBuffer(
      bytes: Array[Byte],
      offset: Int,
      charset: Charset
  ): Option[Int] = {
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
      // TODO: very short documents!
      if (inputData.position() >= inputData.capacity() / 2) {
        copyNonAscii(decBuf, buf)
      }
      decBuf.clear()
    }

    buf.flip()
    Some(buf.limit())
  }

  /** Estimate language by taking at most 5k characters from first 20kb of text. This detector
    * ignores all ASCII characters, so languages which use such scripts are not detectable. Returns
    * [[BadEncoding]] if there exist non-mappable characters using the passed encoding.
    *
    * @param data
    *   text to detect language from
    * @param offset
    *   offset from the array start
    * @param charset
    *   charset to use for converting byte stream to characters
    * @return
    *   child classes of [[EstimationResult]]
    */
  def estimateLang(
      data: Array[Byte],
      offset: Int,
      charset: Charset
  ): EstimationResult = {
    val bufferStatus = prepareBuffer(data, offset, charset)
    if (bufferStatus.isEmpty) {
      return BadEncoding
    }
    val ncopied = bufferStatus.get
    if (ncopied > minBytes) {
      val language = langDetector.detect(internalBuffer)
      if (!language.isPresent) {
        EstimationFailure
      } else {
        val code = language.get().getLanguage
        ProbableLanguage(code)
      }
    } else {
      EstimationFailure
    }
  }
}

object LangEstimation {

  private lazy val cachedDetector = {
    val builtinLangs = com.optimaize.langdetect.profiles.BuiltInLanguages.getLanguages
    val profileReader = new com.optimaize.langdetect.profiles.LanguageProfileReader
    val profiles = profileReader.readBuiltIn(builtinLangs)
    LanguageDetectorBuilder.create(NgramExtractor.gramLengths(1, 2)).withProfiles(profiles).build()
  }

}
