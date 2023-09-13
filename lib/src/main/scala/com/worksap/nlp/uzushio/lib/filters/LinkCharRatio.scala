package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.Document
import com.worksap.nlp.uzushio.lib.filters.base.HighLowDocFilter
import com.worksap.nlp.uzushio.lib.utils.{MathUtil, Paragraphs}

class LinkCharRatio(
    override val low: Float = 0.0f,
    override val high: Float = 1.0f
) extends HighLowDocFilter {

  def calcLinkCharRatio(doc: Document): Float = {
    val iter = doc.aliveParagraphs
    var total = 0
    var inLink = 0
    while (iter.hasNext) {
      val par = iter.next()
      var i = 0
      val txt = par.text
      val len = txt.length
      var inside = 0
      while (i < len) {
        val ch = txt.charAt(i)
        if (ch == Paragraphs.HTML_LINK_START) {
          inside = 1
        } else if (ch == Paragraphs.HTML_LINK_END) {
          inside = 0
        } else {
          total += 1
          inLink += inside
        }
        i += 1
      }
    }
    MathUtil.ratio(inLink, total)
  }

  override def checkDocument(doc: Document): Document = {
    val ratio = calcLinkCharRatio(doc)
    maybeFilter(doc, ratio)
  }
}
