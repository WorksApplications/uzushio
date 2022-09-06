package org.sample.corpus.cleaning

/** Deduplicate same sentences repeating many times.
  */
class DeduplicateRepeatingSentence(minRep: Int = 2) extends DocumentNormalizer {
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
}
