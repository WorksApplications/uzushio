package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.{DocFilter, Document}

class DocLength(min: Int = 0, max: Int = Int.MaxValue) extends DocFilter {
  override def checkDocument(doc: Document): Document = {
    val length = doc.paragraphs.iterator.map(_.text.length).sum
    doc.removeWhen(length < min || length > max, this)
  }

  override def toString: String = {
    (min, max) match {
      case (min, Int.MaxValue) if min != 0 => s"DocLength(>=$min)"
      case (0, max) if max != Int.MaxValue => s"DocLength(<=$max)"
      case (0, Int.MaxValue) => "DocLength(all)"
      case (min, max) => s"DocLength($min<=x=<$max)"
    }
  }
}
