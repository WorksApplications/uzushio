package com.worksap.nlp.uzushio.lib.utils

import it.unimi.dsi.fastutil.chars.Char2ObjectOpenHashMap

final class TrieNode[T] extends Char2ObjectOpenHashMap[TrieNode[T]](4) {
  private var position: Int = -1

  def findLongest(str: CharSequence, offset: Int): SearchResult = {
    var idx = offset
    val len = str.length()
    var end = -1
    var value = -1
    var node = this
    while (idx < len && node != null) {
      val ch = str.charAt(idx)
      val next = node.get(ch)
      if (next != null && next.position != -1) {
        end = idx + 1
        value = next.position
      }
      node = next
      idx += 1
    }
    SearchResult(end, value)
  }
}

object TrieNode {
  def make(data: Iterable[CharSequence]): TrieNode[Boolean] = {
    make(data.iterator)
  }

  def make(data: Iterator[CharSequence]): TrieNode[Boolean] = {
    val root = new TrieNode[Boolean]()
    var index = 0
    while (data.hasNext) {
      val str = data.next()
      var node = root
      var i = 0
      val len = str.length()
      while (i < len) {
        val ch = str.charAt(i)
        var subnode = node.get(ch)
        if (subnode == null) {
          subnode = new TrieNode[Boolean]()
          node.put(ch, subnode)
        }
        node = subnode
        i += 1
      }
      node.position = index
      index += 1
    }
    root
  }
}

final class SearchResult(val carrier: Long) extends AnyVal {
  def end: Int = (carrier & 0xffffffff).toInt

  def index: Int = (carrier >>> 32).toInt

  def ==(o: SearchResult): Boolean = {
    o.carrier == carrier
  }

  def !=(o: SearchResult): Boolean = !(this == o)

  def found: Boolean = end > 0

  def failure: Boolean = !found

  override def toString: String = s"SearchResult($end, $index)"
}

object SearchResult {
  def apply(end: Int, index: Int): SearchResult = {
    val repr = ((index & 0xffffffffL) << 32) | (end & 0xffffffffL)
    new SearchResult(repr)
  }

  def empty(): SearchResult = apply(-1, -1)
}
