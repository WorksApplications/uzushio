package com.worksap.nlp.uzushio.lib.filters.base

import com.worksap.nlp.uzushio.lib.cleaning.Document

trait HighLowDocFilter extends DocFilter {
  def high: Float

  def low: Float

  def maybeFilter(doc: Document, metric: Float): Document = {
    if (metric < low) {
      doc.copy(remove = Low)
    } else if (metric > high) {
      doc.copy(remove = High)
    } else doc
  }

  @transient object Low {
    override val toString = s"${getClass.getSimpleName}.Low($low)"
  }

  @transient object High {
    override val toString = s"${getClass.getSimpleName}.High($high)"
  }

  override def toString = s"${getClass.getSimpleName}($low,$high)"
}

trait HighLowDocIntFilter extends DocFilter {
  def high: Int

  def low: Int

  def maybeFilter(doc: Document, metric: Int): Document = {
    if (metric < low) {
      doc.copy(remove = Low)
    } else if (metric > high) {
      doc.copy(remove = High)
    } else doc
  }

  @transient object Low {
    override val toString = s"${getClass.getSimpleName}.Low($low)"
  }

  @transient object High {
    override val toString = s"${getClass.getSimpleName}.High($high)"
  }

  override def toString = s"${getClass.getSimpleName}($low,$high)"
}
