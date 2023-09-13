package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.Document
import com.worksap.nlp.uzushio.lib.filters.base.DocFilter
import com.worksap.nlp.uzushio.lib.utils.TrieNode
import it.unimi.dsi.fastutil.ints.IntOpenHashSet

import java.io.{BufferedReader, InputStreamReader}
import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

class Words(list: String, minimum: Int = 3) extends DocFilter {
  private val trie = Words.readToTrie(list)
  override def checkDocument(doc: Document): Document = {
    val total = doc.paragraphs.foldLeft(0) { case (cnt, p) =>
      cnt + coundWordInstances(p.text)
    }
    doc.removeWhen(total > minimum, this)
  }

  def coundWordInstances(data: CharSequence): Int = {
    var index = 0
    val len = data.length()
    val found = new IntOpenHashSet()
    while (index < len) {
      val res = trie.findLongest(data, index)
      if (res.found) {
        index += 1
      } else {
        index = res.end
        found.add(res.index)
      }
    }
    found.size()
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
    val reader =
      new InputStreamReader(classRes.openStream(), StandardCharsets.UTF_8)
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
