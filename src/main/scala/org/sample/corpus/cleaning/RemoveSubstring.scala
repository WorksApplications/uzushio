package org.sample.corpus.cleaning

import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Files}

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
}

object RemoveSubstring {
  def fromFile(
      substrFile: Path,
      delim: String = "\n\n",
      perSentence: Boolean = false
  ): RemoveSubstring = {
    val fullstr =
      new String(Files.readAllBytes(substrFile), StandardCharsets.UTF_8)
    new RemoveSubstring(
      fullstr.split(delim).map(_.trim).filter(_.nonEmpty).toSet,
      perSentence
    )
  }
}
