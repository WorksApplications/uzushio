package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.Document
import com.worksap.nlp.uzushio.lib.filters.base.HighLowDocIntFilter

class DocLength(
                 override val low: Int = 0,
                 override val high: Int = Int.MaxValue
               ) extends HighLowDocIntFilter {
  override def checkDocument(doc: Document): Document = {
    val length = doc.aliveParagraphs.map(_.text.length).sum
    maybeFilter(doc, length)
  }
}
