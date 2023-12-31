package com.worksap.nlp.uzushio.lib.filters.base

import com.worksap.nlp.uzushio.lib.cleaning.Document

trait HighLowDocFilter extends DocFilter { self =>
  def high: Float

  def low: Float

  def maybeFilter(doc: Document, metric: Float): Document = {
    if (metric < low) {
      doc.copy(remove = Low)
    } else if (metric > high) {
      doc.copy(remove = High)
    } else doc
  }

  def describeFilter: String = self.getClass.getSimpleName

  @transient object Low {
    override val toString = s"$describeFilter.Low($low)"
  }

  @transient object High {
    override val toString = s"$describeFilter.High($high)"
  }

  override def toString = s"$describeFilter($low,$high)"
}

trait HighLowDocIntFilter extends DocFilter { self =>
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
    override val toString = s"${self.getClass.getSimpleName}.Low($low)"
  }

  @transient object High {
    override val toString = s"${self.getClass.getSimpleName}.High($high)"
  }

  override def toString = s"${self.getClass.getSimpleName}($low,$high)"
}
