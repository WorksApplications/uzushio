package com.worksap.nlp.uzushio.cleaning

import com.typesafe.config.ConfigObject

/** Filters documents that contain script. */
class RemoveScriptDocument extends DocumentFilter {
  val curlyBracketsPattern = """[\{|\}]""".r

  def isFilteredSent(sent: String): Boolean = {
    curlyBracketsPattern.findFirstIn(sent).isEmpty
  }

  override def isFiltered(doc: Seq[String]): Boolean = {
    doc.forall(sent => isFilteredSent(sent))
  }
}

object RemoveScriptDocument extends FromConfig {
  override def fromConfig(conf: ConfigObject): RemoveScriptDocument = new RemoveScriptDocument
}
