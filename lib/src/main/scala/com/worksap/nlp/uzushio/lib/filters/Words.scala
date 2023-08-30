package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.{DocFilter, Document}
import com.worksap.nlp.uzushio.lib.utils.TrieNode

import java.io.{BufferedReader, InputStreamReader}
import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

class Words(list: String, minimum: Int) extends DocFilter {
  private val trie = Words.readToTrie(list)
  override def checkDocument(doc: Document): Document = {
    val total = doc.paragraphs.foldLeft(0) { case (cnt, p) =>
      cnt + countWords(p.text)
    }
    doc.removeWhen(total > minimum, this)
  }

  def countWords(data: CharSequence): Int = {
    var index = 0
    val len = data.length()
    var count = 0
    while (index < len) {
      val end = trie.findLongest(data, index)
      if (end < 0) {
        index += 1
      } else {
        index = end
        count += 1
      }
    }
    count
  }

  override def toString = s"Words($list, min=$minimum)"
}

object Words {
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

  private def readToTrie(s: java.util.stream.Stream[String]): TrieNode[Boolean] = {
    try {
      TrieNode.make(s.iterator().asScala)
    } finally {
      s.close()
    }
  }
}
