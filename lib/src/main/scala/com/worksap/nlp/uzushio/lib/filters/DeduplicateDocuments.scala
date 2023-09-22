package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.stats.NgramHashExtractor
import com.worksap.nlp.uzushio.lib.cleaning.Document
import com.worksap.nlp.uzushio.lib.filters.base.DocFilter
import com.worksap.nlp.uzushio.lib.utils.MathUtil
import scala.math._

trait DocumentRandomGeneratorBase {
  def randomSeed(docId: String): Long

  def randomDouble(docId: String): Double
}

class DocumentRandomGenerator extends DocumentRandomGeneratorBase {
  def randomSeed(docId: String): Long = NgramHashExtractor.hashString(docId)

  def randomDouble(docId: String): Double = MathUtil.asRandomDouble(randomSeed(docId))
}

class DeduplicateDocuments(
    val baseNumFreq: Int = 100,
    val randomGenerator: DocumentRandomGeneratorBase = new DocumentRandomGenerator
) extends DocFilter {

  def computeNearDuplicateTextRatio(doc: Document): Float = {
    val iter = doc.aliveParagraphs

    var totalLengthWeightedNearFreq = 0.0
    var totalLength = 0.0

    while (iter.hasNext) {
      val paragraph = iter.next()
      val text = paragraph.text
      val textLength = text.length()
      val nearFreq = if (paragraph.nearFreq < baseNumFreq) paragraph.nearFreq else baseNumFreq
      val weight = log(nearFreq) / log(baseNumFreq)

      totalLength += textLength
      totalLengthWeightedNearFreq += (textLength * weight)
    }

    MathUtil.ratio(totalLengthWeightedNearFreq.toFloat, totalLength.toFloat)
  }

  def shouldRemoveDocument(doc: Document) = {
    val nearDuplicateTextRatio = computeNearDuplicateTextRatio(doc)
    val thresholdProb = randomGenerator.randomDouble(doc.docId)
    nearDuplicateTextRatio >= thresholdProb
  }

  override def checkDocument(doc: Document): Document = {
    if (shouldRemoveDocument(doc)) {
      doc.copy(remove = this)
    } else doc
  }
}
