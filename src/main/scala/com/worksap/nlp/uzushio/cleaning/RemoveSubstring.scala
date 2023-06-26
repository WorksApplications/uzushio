package com.worksap.nlp.uzushio.cleaning

import com.typesafe.config.ConfigObject
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths, Files}
import scala.io.Source

/** Removes given substrings from documents.
  *
  * @param matchSentence
  *   If true, match string with only full sentence, i.e. substr have to
  *   start/end at newline.
  */
class RemoveSubstring(
    substrs: Set[String],
    matchSentence: Boolean = RemoveSubstring.defaultMatchSentence
) extends DocumentNormalizer {
  val substrPattern = matchSentence match {
    case false => { s"""(${substrs.mkString("|")})""".r }
    case true  => { s"""(?m)(^${substrs.mkString("$|^")}$$)""".r }
  }

  override def normalizeDocument(doc: Seq[String]): Seq[String] = {
    val fullDoc = doc.mkString("\n")
    val removed = substrPattern.replaceAllIn(fullDoc, "")
    removed.split("\n").filter(_.length > 0).toSeq
  }

  override def toString(): String =
    s"${this.getClass.getSimpleName}(#substr=${substrs.size})"
}

object RemoveSubstring extends FromConfig {
  val defaultPath = "template_sentences.txt"
  val defaultDelim = "\n\n" // Delimiter of substrings in the file.
  val defaultMatchSentence = false // Whether if match only full sentence.

  def fromFile(
      filePath: Path,
      delim: String = defaultDelim,
      matchSentence: Boolean = defaultMatchSentence
  ): RemoveSubstring = {
    val fullstr =
      new String(Files.readAllBytes(filePath), StandardCharsets.UTF_8)
    new RemoveSubstring(
      fullstr.split(delim).map(_.trim).filter(_.nonEmpty).toSet,
      matchSentence
    )
  }

  override def fromConfig(conf: ConfigObject): RemoveSubstring = {
    val pathStr = conf.getOrElseAs[String]("path", defaultPath)
    val delim = conf.getOrElseAs[String]("delim", defaultDelim)
    val matchSentence =
      conf.getOrElseAs[Boolean]("matchSentence", defaultMatchSentence)

    val filepath = Paths.get(pathStr)
    if (filepath.toFile.exists) {
      fromFile(filepath, delim, matchSentence)
    } else {
      val fullstr = Source.fromResource(pathStr).mkString
      new RemoveSubstring(
        fullstr.split(delim).map(_.trim).filter(_.nonEmpty).toSet,
        matchSentence
      )
    }
  }
}
