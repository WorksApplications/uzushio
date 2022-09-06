package org.sample.corpus.cleaning

/** Removes citation markers (from Wikipedia). */
class RemoveWikipediaCitation extends SentenceNormalizer {
  val citationPattern =
    """\[\d+?\]|\[要.+?\]|\{\{+[^{}]+?\}\}+|\[(要出典|リンク切れ|.+?\?)\]""".r

  override def normalizeSentence(sent: String): String = {
    citationPattern.replaceAllIn(sent, "")
  }
}
