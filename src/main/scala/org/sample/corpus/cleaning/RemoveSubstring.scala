package org.sample.corpus.cleaning

import com.typesafe.config.ConfigObject
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths, Files}

/** Removes given substrings from documents.
  *
  * If perSentence is true, remove if it starts/ends at newline.
  */
class RemoveSubstring(substrs: Set[String], perSentence: Boolean = false)
    extends DocumentNormalizer {
  val substrPattern = perSentence match {
    case false => { s"""(${substrs.mkString("|")})""".r }
    case true  => { s"""(?m)(^${substrs.mkString("$|^")}$$)""".r }
  }

  override def normalizeDocument(doc: Seq[String]): Seq[String] = {
    val fullDoc = doc.mkString("\n")
    val removed = substrPattern.replaceAllIn(fullDoc, "")
    removed.split("\n").filter(_.length > 0).toSeq
  }

  override def toString(): String =
    s"${this.getClass.getSimpleName}(#str=${substrs.size})"
}

object RemoveSubstring extends FromConfig {
  val defaultPath = "./resources/template_sentences.txt"
  val defaultDelim = "\n\n"
  val defaultPerSentence = false

  def fromFile(
      filePath: Path,
      delim: String = defaultDelim,
      perSentence: Boolean = defaultPerSentence
  ): RemoveSubstring = {
    val fullstr =
      new String(Files.readAllBytes(filePath), StandardCharsets.UTF_8)
    new RemoveSubstring(
      fullstr.split(delim).map(_.trim).filter(_.nonEmpty).toSet,
      perSentence
    )
  }

  override def fromConfig(conf: ConfigObject): RemoveSubstring = {
    val filePath =
      conf.getOrElseAs[String]("path", defaultPath)
    val delim = conf.getOrElseAs[String]("delim", defaultDelim)
    val perSentence =
      conf.getOrElseAs[Boolean]("perSentence", defaultPerSentence)

    fromFile(Paths.get(filePath), delim = delim, perSentence = perSentence)
  }
}
