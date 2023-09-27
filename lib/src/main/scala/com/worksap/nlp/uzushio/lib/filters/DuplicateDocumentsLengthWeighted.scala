package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.Document
import com.worksap.nlp.uzushio.lib.filters.base.DocFilter
import com.worksap.nlp.uzushio.lib.utils.MathUtil

class DuplicateDocumentsLengthWeighted(expected: Double = 1.0) extends DocFilter {
  override def checkDocument(doc: Document): Document = {
    val weight = DuplicateDocumentsLengthWeighted.nearFreqWeight(doc)
    val prob = expected / weight
    doc.removeWhen(doc.randomDouble > prob, this)
  }

  override val toString = s"DuplicateDocumentsLengthWeighted($expected)"
}

object DuplicateDocumentsLengthWeighted {
  def nearFreqWeight(doc: Document): Double = {
    var nchars = 0L
    var weight = 0.0

    val iter = doc.aliveParagraphs
    while (iter.hasNext) {
      val par = iter.next()
      val len = par.text.length.toLong
      nchars += len
      weight += len * (Math.log10(par.nearFreq) + 1)
    }
    MathUtil.doubleRatio(weight, nchars)
  }

}
