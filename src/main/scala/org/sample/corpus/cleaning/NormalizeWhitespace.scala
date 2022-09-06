package org.sample.corpus.cleaning

/** Removes excess whitespaces. */
class NormalizeWhitespace extends SentenceNormalizer {
  val continuousWhitespacePattern = """[\s　]+""".r

  override def normalizeSentence(sent: String): String = {
    continuousWhitespacePattern.replaceAllIn(sent, " ")
  }
}
