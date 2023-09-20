package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.{Document, Paragraph}
import com.worksap.nlp.uzushio.lib.filters.base.DocFilter
import com.worksap.nlp.uzushio.lib.utils.TrieNode

import java.io.{BufferedReader, InputStreamReader}
import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

/** Score documents using a word list and filter them if the score is more than the [[threshold]].
  *
  * Word lists are read from
  *   - Filesystem
  *   - com.worksap.nlp.uzushio.lib.filters package in classpath
  *   - root package in classpath
  *
  * @param list
  *   word list will be read from this resource
  * @param threshold
  *   documents with score larger than this value will be filtered out
  * @param full
  *   score for a full match
  * @param partial
  *   score for a partial match
  */
class WordInstances(list: String, threshold: Float = 3, full: Float = 1.0f, partial: Float = 0.1f)
    extends DocFilter {
  private val trie = WordInstances.readToTrie(list)
  override def checkDocument(doc: Document): Document = {
    val score = scoreDocument(doc) + 1e-3f
    doc.removeWhen(score >= threshold, this)
  }

  def scoreDocument(document: Document): Float = {
    var score = 0.0f
    val iter = document.aliveParagraphs
    while (iter.hasNext) {
      score += scoreParagraph(iter.next())
    }
    score
  }

  def scoreParagraph(paragraph: Paragraph): Float = {
    var score = 0.0f
    val text = paragraph.text
    var start = 0
    val len = text.length
    while (start < len) {
      val res = trie.findLongest(text, start)
      if (res.found) {
        start = res.end
        score += full
      } else {
        start += 1
      }
    }
    score
  }

  override def toString = s"WordInstances($list,$threshold,$full,$partial)"
}

object WordInstances {
  import scala.collection.JavaConverters._
  def readToTrie(name: String): TrieNode[Boolean] = {
    val p = Paths.get(name)
    if (Files.exists(p)) {
      return readToTrie(Files.lines(p))
    }

    val classRes = getClass.getResource(name)
    if (classRes != null) {
      return readToTrie(classRes)
    }

    val loaderRes = getClass.getClassLoader.getResource(name)
    if (loaderRes != null) {
      return readToTrie(classRes)
    }

    throw new IllegalArgumentException(s"could not find word list $name")
  }

  private def readToTrie(classRes: URL): TrieNode[Boolean] = {
    val reader = new InputStreamReader(classRes.openStream(), StandardCharsets.UTF_8)
    readToTrie(new BufferedReader(reader).lines())
  }

  private def readToTrie(
      s: java.util.stream.Stream[String]
  ): TrieNode[Boolean] = {
    try {
      TrieNode.make(s.iterator().asScala)
    } finally {
      s.close()
    }
  }
}
