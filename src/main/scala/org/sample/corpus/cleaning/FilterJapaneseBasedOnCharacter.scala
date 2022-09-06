package org.sample.corpus.cleaning

/** Filters non Japanese document based on the type of characters.
  *
  * Default threshold follows nwc-toolkit:text-filter.
  *
  * @param kanaRate
  *   texts with hiragana/katakana less than this are filtered.
  * @param jpRate
  *   texts with kana/kanji less than this are filtered.
  */
class FilterJapaneseBasedOnCharacter(
    kanaRate: Double = 0.05,
    jpRate: Double = 0.7
) extends SentenceFilter {
  val kanaPattern = """\p{InHiragana}|\p{InKatakana}""".r
  val jpCharPattern =
    """\p{InHiragana}|\p{InKatakana}|\p{InCJKUnifiedIdeographs}""".r

  override def isFiltered(sent: String): Boolean = {
    val kanaCount = kanaPattern.findAllIn(sent).length.toDouble
    val jpCount = jpCharPattern.findAllIn(sent).length.toDouble
    val charCount = sent.length.toDouble

    (kanaCount / charCount) > kanaRate && (jpCount / charCount) > jpRate
  }
}
