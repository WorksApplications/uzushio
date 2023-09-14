package com.worksap.nlp.uzushio.cleaning

import com.typesafe.config.ConfigObject

/** Deduplicate same sentences repeating many times.
  */
class DeduplicateRepeatingSentence(minRep: Int = 2)
    extends DocumentNormalizer
    with FieldSettable[DeduplicateRepeatingSentence] {
  override def normalizeDocument(doc: Seq[String]): Seq[String] = {
    var (i, j) = (0, 0)
    var indices: Seq[Int] = Vector()
    while (i < doc.length) {
      j = i + 1
      while ((j < doc.length) && (doc(i) == doc(j))) { j += 1 }

      if (i + minRep <= j) { indices :+= i }
      else { indices ++= i until j }
      i = j
    }
    for (i <- indices) yield doc(i)
  }

  override def toString(): String = s"${this.getClass.getSimpleName}($minRep)"
}

object DeduplicateRepeatingSentence extends FromConfig {
  override def fromConfig(conf: ConfigObject): DeduplicateRepeatingSentence = {
    val args = Map[String, Option[Any]](
      "minRep" -> conf.getAs[Int]("minRepeat")
    ).collect { case (k, Some(v)) => k -> v }

    new DeduplicateRepeatingSentence().setFields(args)
  }
}
