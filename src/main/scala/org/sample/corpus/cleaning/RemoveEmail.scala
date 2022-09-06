package org.sample.corpus.cleaning

/** Filters sentences that contain email address. */
class RemoveEmail extends SentenceFilter {
  val emailPattern = """[\w\d_-]+@[\w\d_-]+\.[\w\d._-]+""".r

  override def isFiltered(sent: String): Boolean = {
    emailPattern.findFirstIn(sent).isEmpty
  }
}
