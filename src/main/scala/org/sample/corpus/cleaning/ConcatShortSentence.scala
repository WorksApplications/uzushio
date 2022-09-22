package org.sample.corpus.cleaning

import com.typesafe.config.ConfigObject

/** Concat too short sentences to the previous sentence. */
class ConcatShortSentence(concatThr: Int = 2)
    extends DocumentNormalizer
    with FieldSettable[ConcatShortSentence] {
  override def normalizeDocument(doc: Seq[String]): Seq[String] = {
    if (doc.length <= 1) {
      doc
    } else {
      val shortSentIdx = doc.zipWithIndex
        .map(z => { if (z._1.length <= concatThr) z._2 else -1 })
        .filter(_ > 0) // keep first sentence regardless of its length

      val appended = shortSentIdx.reverse.foldLeft(doc)((d, i) =>
        d.updated(i - 1, d(i - 1) + d(i))
      )

      for (i <- 0 until appended.length if (!shortSentIdx.contains(i)))
        yield appended(i)
    }
  }

  override def toString(): String =
    s"${this.getClass.getSimpleName}(${concatThr})"
}

object ConcatShortSentence extends FromConfig {
  override def fromConfig(conf: ConfigObject): ConcatShortSentence = {
    val args = Map[String, Option[Any]](
      "concatThr" -> conf.getAs[Int]("concatThr")
    ).collect { case (k, Some(v)) => k -> v }

    new ConcatShortSentence().setFields(args)
  }
}
