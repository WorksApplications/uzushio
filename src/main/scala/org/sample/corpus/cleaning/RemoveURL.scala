package org.sample.corpus.cleaning

/** Filters sentences that contain URL. */
class RemoveURL extends SentenceFilter {
  val urlPattern = """(https?|sftp?)://[\w/:%#\$&\?\(\)~\.=\+\-]+""".r

  override def isFiltered(sent: String): Boolean = {
    urlPattern.findFirstIn(sent).isEmpty
  }
}
