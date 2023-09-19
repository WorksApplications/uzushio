package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.{Document, Paragraph}
import com.worksap.nlp.uzushio.lib.filters.base.DocFilter
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import org.apache.commons.math3.util.FastMath

class WordTypes(list: String, threshold: Float = 3, kind: String = "uniq") extends DocFilter {
  private val trie = WordInstances.readToTrie(list)
  private val scorer = kind match {
    case "uniq" => WordTypes.SizeScorer
    case "log10" => WordTypes.Log10Scorer
    case "sqrt" => WordTypes.SqrtScorer
    case _ => throw new IllegalArgumentException("unknown kind, can be one of: uniq, log10, sqrt")
  }
  override def checkDocument(doc: Document): Document = {
    val score = scoreDocument(doc)
    doc.removeWhen(score >= threshold, this)
  }

  def scoreDocument(doc: Document): Float = {
    val counts = new Int2IntOpenHashMap()
    val iter = doc.aliveParagraphs
    while (iter.hasNext) {
      consumeParagraph(counts, iter.next())
    }
    scoreCounts(counts)
  }

  private def consumeParagraph(counts: Int2IntOpenHashMap, paragraph: Paragraph): Unit = {
    val text = paragraph.text
    var start = 0
    val len = text.length
    while (start < len) {
      val res = trie.findLongest(text, start)
      if (res.found) {
        start = res.end
        counts.addTo(res.index, 1)
      } else {
        start += 1
      }
    }
  }

  private def scoreCounts(map: Int2IntOpenHashMap): Float = {
    if (map.isEmpty) return 0
    scorer(map)
  }
}

object WordTypes {
  private trait Scorer extends (Int2IntOpenHashMap => Float) with Serializable

  private object SizeScorer extends Scorer {
    override def apply(v1: Int2IntOpenHashMap): Float = v1.size()
  }

  private object SqrtScorer extends Scorer {
    override def apply(v1: Int2IntOpenHashMap): Float = {
      var score = 0.0
      val iter = v1.values().iterator()
      while (iter.hasNext) {
        score += FastMath.sqrt(v1.size())
      }
      score.toFloat
    }
  }

  private object Log10Scorer extends Scorer {
    override def apply(v1: Int2IntOpenHashMap): Float = {
      var score = v1.size().toDouble // log_10 (1) == 0, so add size to the score
      val iter = v1.values().iterator()
      while (iter.hasNext) {
        score += FastMath.log10(v1.size())
      }
      score.toFloat
    }
  }
}
