package org.sample.corpus.cleaning

/** Filters documents that contain script. */
class RemoveScriptDocument extends DocumentFilter {
  val curlyBracketsPattern = """[\{|\}]""".r

  def isFilteredSent(sent: String): Boolean = {
    curlyBracketsPattern.findFirstIn(sent).isEmpty
  }

  override def isFiltered(doc: Seq[String]): Boolean = {
    doc.foldLeft(true)((b, sent) => b && isFilteredSent(sent))
  }
}
