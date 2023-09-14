package com.worksap.nlp.uzushio.cleaning

import com.typesafe.config.ConfigObject

/** Removes citation markers (from Wikipedia). */
class RemoveWikipediaCitation extends SentenceNormalizer {
  val citationPattern = """\[\d+?\]|\[要.+?\]|\{\{+[^{}]+?\}\}+|\[(要出典|リンク切れ|.+?\?)\]""".r

  override def normalizeSentence(sent: String): String = {
    citationPattern.replaceAllIn(sent, "")
  }
}

object RemoveWikipediaCitation extends FromConfig {
  override def fromConfig(conf: ConfigObject): RemoveWikipediaCitation = new RemoveWikipediaCitation
}
