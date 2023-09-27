package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.stats.NgramHashExtractor
import com.worksap.nlp.uzushio.lib.cleaning.Document
import com.worksap.nlp.uzushio.lib.filters.base.DocFilter
import com.worksap.nlp.uzushio.lib.utils.MathUtil
import scala.math._
import scala.util.Random

trait RandomGeneratorFromStringBase {
  def generateRandom(docId: String): Double
}

// An object in arguments of DocFilter on Spark needs to mixin Serializable.
object RandomGeneratorFromString extends RandomGeneratorFromStringBase with Serializable {
  def generateRandom(docId: String): Double = {
    val seed = NgramHashExtractor.hashString(docId)
    MathUtil.asRandomDouble(seed)
  }
}

class GaussianRandomGeneratorFromString(
    val mu: Double = 0.3,
    val sd: Double = 0.1
) extends RandomGeneratorFromStringBase
    with Serializable {
  def generateRandom(docId: String): Double = {
    val seed = NgramHashExtractor.hashString(docId)
    val rng = new Random(seed)
    rng.nextGaussian() * mu + sd
  }
}

class DeduplicateDocuments(
    val baseNumFreq: Int = 10,
    val randomGenerator: RandomGeneratorFromStringBase = new GaussianRandomGeneratorFromString
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
    val thresholdProb = randomGenerator.generateRandom(doc.docId)

    nearDuplicateTextRatio >= thresholdProb
  }

  override def checkDocument(doc: Document): Document = {
    doc.removeWhen(shouldRemoveDocument(doc), this)
  }
}
