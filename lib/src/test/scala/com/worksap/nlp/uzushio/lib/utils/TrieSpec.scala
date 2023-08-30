package com.worksap.nlp.uzushio.lib.utils

import org.scalatest.freespec.AnyFreeSpec

class TrieSpec extends AnyFreeSpec {
  "TrieNode" - {
    "can be created" in {
      val trie = TrieNode.make(Seq("test", "tfst", "fist"))
      assert(trie != null)
    }

    "can find strings" in {
      val trie = TrieNode.make(Seq("test", "tfst", "fist"))
      assert(4 == trie.findLongest("testing", 0))
      assert(4 == trie.findLongest("testtfst", 0))
      assert(4 == trie.findLongest("tfsttest", 0))
      assert(-1 == trie.findLongest("tfest", 0))
    }

    "finds a longest substring" in {
      val trie = TrieNode.make(Seq("ab", "abc", "abcd"))
      assert(2 == trie.findLongest("abed", 0))
      assert(2 == trie.findLongest("abecd", 0))
      assert(4 == trie.findLongest("abcdf", 0))
      assert(3 == trie.findLongest("abcfd", 0))
    }
  }
}
