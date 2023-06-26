package com.worksap.nlp.uzushio.cleaning

import com.typesafe.config.ConfigObject

/** Filters sentences that contain URL. */
class RemoveURL extends SentenceFilter {
  val urlPattern = """(https?|sftp?)://[\w/:%#\$&\?\(\)~\.=\+\-]+""".r

  override def isFiltered(sent: String): Boolean = {
    urlPattern.findFirstIn(sent).isEmpty
  }
}

object RemoveURL extends FromConfig {
  override def fromConfig(conf: ConfigObject): RemoveURL = new RemoveURL
}
