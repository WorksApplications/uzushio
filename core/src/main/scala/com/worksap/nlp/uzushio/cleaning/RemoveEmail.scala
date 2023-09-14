package com.worksap.nlp.uzushio.cleaning

import com.typesafe.config.ConfigObject

/** Filters sentences that contain email address. */
class RemoveEmail extends SentenceFilter {
  val emailPattern = """[\w\d_-]+@[\w\d_-]+\.[\w\d._-]+""".r

  override def isFiltered(sent: String): Boolean = {
    emailPattern.findFirstIn(sent).isEmpty
  }
}

object RemoveEmail extends FromConfig {
  override def fromConfig(conf: ConfigObject): RemoveEmail = new RemoveEmail
}
