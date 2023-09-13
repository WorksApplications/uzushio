package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.Document
import com.worksap.nlp.uzushio.lib.filters.HiraganaRatio.isHiragana
import com.worksap.nlp.uzushio.lib.filters.base.HighLowDocFilter
import com.worksap.nlp.uzushio.lib.utils.MathUtil

final class HiraganaRatio(
    override val low: Float = 0.0f,
    override val high: Float = 1.0f
) extends HighLowDocFilter {
  override def checkDocument(doc: Document): Document = {
    val ratio = computeHiraganaRatio(doc)
    maybeFilter(doc, ratio)
  }

  def computeHiraganaRatio(document: Document): Float = {
    var nchars = 0
    var nhiragana = 0
    val iter = document.aliveParagraphs
    while (iter.hasNext) {
      val par = iter.next()
      val text = par.text
      nchars += text.length
      nhiragana += countHiraganaChars(text)
    }
    MathUtil.ratio(nhiragana, nchars)
  }

  def countHiraganaChars(str: String): Int = {
    val len = str.length
    var idx = 0
    var count = 0
    while (idx < len) {
      val ch = str.charAt(idx)
      if (isHiragana(ch)) {
        count += 1
      }
      idx += 1
    }
    count
  }
}

object HiraganaRatio {
  def isHiragana(c: Char): Boolean = {
    c >= 0x3040 && c <= 0x309f
  }
}
