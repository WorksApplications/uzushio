package com.worksap.nlp.uzushio.cleaning

import com.typesafe.config.ConfigObject

/** Removes non-printable characters.
  *
  * Following python's str.isprintable, remove unicode general-category "Other"
  * or "Separator" except space. We also keep surrogate code points (that are
  * not in python).
  *
  * @param keepWS
  *   If true, keep whitespaces other than space (" "), including \u3000. This
  *   is not python compatible behaviour.
  */
class NormalizeCharacter(keepWS: Boolean = NormalizeCharacter.defaultKeepWS)
    extends SentenceNormalizer {
  val nonPrintablePattern =
    if (keepWS) """[\p{gc=C}\p{gc=Z}&&[^\s　\p{gc=Cs}]]""".r
    else """[\p{gc=C}\p{gc=Z}&&[^ \p{gc=Cs}]]""".r

  override def normalizeSentence(sent: String): String = {
    nonPrintablePattern.replaceAllIn(sent, "")
  }

  override def toString(): String =
    s"${this.getClass.getSimpleName}(keepWS=${keepWS})"
}

object NormalizeCharacter extends FromConfig {
  val defaultKeepWS = false

  override def fromConfig(conf: ConfigObject): NormalizeCharacter = {
    val keepWS = conf.getOrElseAs[Boolean]("keepWS", defaultKeepWS)
    new NormalizeCharacter(keepWS)
  }
}
