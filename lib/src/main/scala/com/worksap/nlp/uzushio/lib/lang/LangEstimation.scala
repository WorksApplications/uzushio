package com.worksap.nlp.uzushio.lib.lang

import com.optimaize.langdetect.LanguageDetectorBuilder
import com.optimaize.langdetect.ngram.NgramExtractor
import java.nio.charset.{Charset, CodingErrorAction}
import java.nio.{ByteBuffer, CharBuffer}
import java.util.regex.{Matcher, Pattern}

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

  /** Remove HTML tags and inline JavaScript from the input.
    * @param input
    *   input buffer as a string
    * @return
    *   cleaned string with HTML and JavaScript removed
    */
  private def cleanHtmlContent(input: String): String = {
    // Use regex to remove HTML tags and content inside <script> tags
    val scriptPattern = Pattern.compile("(?s)<script.*?>.*?</script>")
    val stylePattern = Pattern.compile("(?s)<style.*?>.*?</style>")
    val commentPattern = Pattern.compile("(?s)<!--.*?-->")
    val htmlTagPattern = Pattern.compile("<[^>]+>")

    
    // Process from 50% ~
    val startPos = input.length / 2
    var cleanedContent = input.substring(startPos)

    // First, remove the <script> and etc block
    cleanedContent = removePattern(cleanedContent, scriptPattern)
    cleanedContent = removePattern(cleanedContent, stylePattern)
    cleanedContent = removePattern(cleanedContent, commentPattern)

    // remove html tags
    cleanedContent = removePattern(cleanedContent, htmlTagPattern)

    println(s"Cleaned content: ${cleanedContent.take(100)}...") // debug print
    cleanedContent
  }
  
  /** Helper function to remove pattern from string using JVM's regex */
  private def removePattern(input: String, pattern: Pattern): String = {
    val matcher: Matcher = pattern.matcher(input)
    matcher.replaceAll("")
  }

  /** Copy meaningful content into detection buffer, removing HTML, JavaScript, and retaining text.
    * Retains both ASCII and non-ASCII characters, focusing on meaningful language content.
    *
    * @param input
    *   input CharBuffer
    * @param output
    *   output CharBuffer
    */
  private def copyMeaningfulContent(input: CharBuffer, output: CharBuffer): Unit = {
    // Convert the input to a string
    val content = input.toString

    // Use regex to remove HTML tags and JavaScript
    val cleanedContent = cleanHtmlContent(content)

    // Filter and clean the remaining text, retaining letters, digits, whitespace, and non-ASCII characters
    val meaningfulContent = cleanedContent.flatMap { char =>
      if (char.isLetterOrDigit || char.isWhitespace || char >= 128) {
        Some(char)
      } else {
        None
      }
    }

    // Put the cleaned content into the output buffer
    val result = meaningfulContent.mkString.trim
    println(s"Meaningful content: $result") // Print the meaningful content
    // Copy meaningful content to the output buffer
    output.put(result)
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
      copyMeaningfulContent(decBuf, buf)
      decBuf.clear()
    }

    buf.flip()
    Some(buf.limit())
  }

  /** Estimate the language by taking at most 5k characters from the first 20kb of text.
    * Retains both ASCII and non-ASCII characters, but removes HTML and JavaScript tags.
    * Returns [[BadEncoding]] if there are unmappable characters using the provided encoding.
    *
    * @param data
    *   the text to detect language from
    * @param offset
    *   the offset from the start of the array
    * @param charset
    *   the charset to use for converting byte stream to characters
    * @return
    *   a subclass of [[EstimationResult]]
    */
  def estimateLang(
      data: Array[Byte],
      offset: Int,
      charset: Charset
  ): EstimationResult = {
    val bufferStatus = prepareBuffer(data, offset, charset)
    val internalBufferString = internalBuffer.toString
    println(s"internalBuffer: $internalBufferString") // Print the content of the internal buffer
    if (bufferStatus.isEmpty) {
      return BadEncoding
    }
    val ncopied = bufferStatus.get
    println(s"Copied characters: $ncopied") // Print the number of copied characters
    if (ncopied > minBytes) {
      val language = langDetector.detect(internalBuffer)
      println(s"Detected language: ${language}") // Print the detected language
      if (!language.isPresent) {
        EstimationFailure
      } else {
        val code = language.get().getLanguage
        println(s"Detected language code: $code") // Print the detected language code
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