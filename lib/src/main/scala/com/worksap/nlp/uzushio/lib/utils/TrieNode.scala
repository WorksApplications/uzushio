package com.worksap.nlp.uzushio.lib.utils

import it.unimi.dsi.fastutil.chars.Char2ObjectOpenHashMap

class TrieNode[T] extends Char2ObjectOpenHashMap[TrieNode[T]] {
  private var innerValue: T = _

  def findLongest(str: CharSequence, offset: Int): Int = {
    var idx = offset
    val len = str.length()
    var last = -1
    var node = this
    while (idx < len && node != null) {
      val ch = str.charAt(idx)
      val next = node.get(ch)
      if (next != null && next.innerValue != null) {
        last = idx + 1
      }
      node = next
      idx += 1
    }
    last
  }
}

object TrieNode {
  def make(data: Iterable[CharSequence]): TrieNode[Boolean] = {
    make(data.iterator)
  }

  def make(data: Iterator[CharSequence]): TrieNode[Boolean] = {
    val root = new TrieNode[Boolean]()
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
      node.innerValue = true
    }
    root
  }
}