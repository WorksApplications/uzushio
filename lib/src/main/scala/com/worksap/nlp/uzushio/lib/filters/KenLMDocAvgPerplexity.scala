package com.worksap.nlp.uzushio.lib.filters

import com.github.jbaiter.kenlm.BufferEvaluator
import com.worksap.nlp.sudachi.{Dictionary, Morpheme}
import com.worksap.nlp.uzushio.lib.cleaning.{Document, Paragraph}
import com.worksap.nlp.uzushio.lib.filters.base.{DocFilter, HighLowDocFilter}
import com.worksap.nlp.uzushio.lib.resources.{KenLM, Sudachi}
import com.worksap.nlp.uzushio.lib.utils.Paragraphs

class KenLMDocAvgPerplexity(
    sudachi: String,
    kenlm: String,
    outliers: Float = 0,
    override val high: Float = 1e6f,
    override val low: Float = 0f
) extends HighLowDocFilter {

  @transient
  private lazy val processor = KenLMEvaluator.make(sudachi, kenlm, outliers)

  override def checkDocument(doc: Document): Document = {
    val perplexity = measureDoc(doc)
    maybeFilter(doc, perplexity)
  }

  def measureDoc(doc: Document): Float = {
    var ppxSum = 0.0
    var charCnt = 0
    val paragraphs = doc.aliveParagraphs
    val proc = processor
    while (paragraphs.hasNext) {
      val p = paragraphs.next()
      val logProb = proc.scoreParagraph(p)
      val ppx = Math.pow(10, -logProb)
      ppxSum += ppx * p.text.length
      charCnt += p.text.length
    }
    (ppxSum / charCnt).toFloat
  }

  override def describeFilter: String = s"KenLMAvgDoc($outliers)"
}

class KenLMEvaluator(sudachi: String, kenlm: String) {
  private val dictionary: Dictionary = Sudachi.get(sudachi)
  final protected val tokenizer = dictionary.create()
  final protected val evaluator = KenLM.get(kenlm).bufferEvaluator(64 * 1024, 1024)

  def processParagraph(p: Paragraph): BufferEvaluator = {
    val tokens = tokenizer.tokenize(p.text)
    val ev = evaluator
    val iter = tokens.iterator()
    var continue = true
    ev.clear()
    while (iter.hasNext && continue) {
      val token = iter.next()
      if (acceptedToken(token)) {
        val remaining = ev.append(token.surface())
        continue = remaining > 0
      }
    }
    ev
  }

  def acceptedToken(x: Morpheme): Boolean = {
    if (x.normalizedForm() == " ") {
      return false
    }

    val s = x.surface()
    if (s.length == 1) {
      s.charAt(0) match {
        case Paragraphs.HTML_LINK_START | Paragraphs.HTML_LINK_END | '\n' => return false
        case _ => return true
      }
    }

    true
  }

  def extractScore(ev: BufferEvaluator): Double = ev.evaluate()

  def scoreParagraph(p: Paragraph): Double = {
    val e = processParagraph(p)
    extractScore(e)
  }
}

object KenLMEvaluator {
  def make(sudachi: String, kenlm: String, ratio: Float): KenLMEvaluator = {
    if (ratio < 1e-3) {
      new KenLMEvaluator(sudachi, kenlm)
    } else {
      new KenLMEvaluatorNoOutliers(sudachi, kenlm, ratio)
    }
  }
}

class KenLMEvaluatorNoOutliers(sudachi: String, kenlm: String, ratio: Float)
    extends KenLMEvaluator(sudachi, kenlm) {
  override def extractScore(ev: BufferEvaluator): Double = ev.evaluateNoOutliers(ratio)
}
