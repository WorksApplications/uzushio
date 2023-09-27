package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.Document
import com.worksap.nlp.uzushio.lib.filters.base.DocFilter
import spire.math.QuickSelect

class DeduplicateDocumentsPercentile(percentile: Float = 0.05f, expected: Double = 1.0)
    extends DocFilter {
  override def checkDocument(doc: Document): Document = {
    val freq = DeduplicateDocumentsPercentile.freqAtPercentile(doc, percentile)
    val probability = expected / freq
    doc.removeWhen(doc.randomDouble > probability, this)
  }

  override val toString = s"DedupDocsPercentile($percentile,$expected)"
}

object DeduplicateDocumentsPercentile {
  import spire.std.any.IntAlgebra

  def freqAtPercentile(doc: Document, percentile: Float): Int = {
    val counts = doc.aliveParagraphs.map(_.nearFreq).toArray
    if (counts.isEmpty) {
      return 0
    }
    val position = (counts.length * percentile).toInt
    QuickSelect.select(counts, position)
    counts(position)
  }

}
