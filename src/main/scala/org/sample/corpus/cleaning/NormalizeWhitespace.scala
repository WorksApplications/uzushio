package org.sample.corpus.cleaning

import com.typesafe.config.ConfigObject

/** Removes excess whitespaces. */
class NormalizeWhitespace extends SentenceNormalizer {
  val continuousWhitespacePattern = """[\s　]+""".r

  override def normalizeSentence(sent: String): String = {
    continuousWhitespacePattern.replaceAllIn(sent, " ")
  }
}

object NormalizeWhitespace extends FromConfig {
  override def fromConfig(conf: ConfigObject): NormalizeWhitespace =
    new NormalizeWhitespace
}
