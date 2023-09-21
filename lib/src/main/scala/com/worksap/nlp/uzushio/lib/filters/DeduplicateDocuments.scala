package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.Document
import com.worksap.nlp.uzushio.lib.filters.base.HighLowDocFilter
import com.worksap.nlp.uzushio.lib.utils.MathUtil


class DeduplicateDocuments(
  override val low: Float = 0.0f,
  override val high: Float = 1.0f
) extends HighLowDocFilter {

  override def checkDocument(doc: Document): Document = {
    val iter = doc.aliveParagraphs

    var lengthNearFreqOverOne = 0
    var totalLength = 0

    while (iter.hasNext) {
      val paragraph = iter.next()
      val text = paragraph.text
      val textLength = text.length()
      val nearFreq = paragraph.nearFreq

      totalLength += textLength

      if (nearFreq > 1) {
        lengthNearFreqOverOne += textLength
      }
    }

    val nearDuplicateTextRatio = MathUtil.ratio(lengthNearFreqOverOne, totalLength)
    
    val thresholdProb = doc.randomDouble
    if (nearDuplicateTextRatio >= thresholdProb) {
      doc.copy(remove = this)
    } else doc
  }
}