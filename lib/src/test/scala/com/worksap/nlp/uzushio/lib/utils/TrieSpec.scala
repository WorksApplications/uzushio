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
      assert(SearchResult(4, 0) == trie.findLongest("testing", 0))
      assert(SearchResult(4, 0) == trie.findLongest("testtfst", 0))
      assert(SearchResult(8, 1) == trie.findLongest("testtfst", 4))
      assert(SearchResult(4, 1) == trie.findLongest("tfsttest", 0))
      assert(SearchResult.empty() == trie.findLongest("tfest", 0))
    }

    "finds a longest substring" in {
      val trie = TrieNode.make(Seq("ab", "abc", "abcd"))
      assert(SearchResult(2, 0) == trie.findLongest("abed", 0))
      assert(SearchResult(2, 0) == trie.findLongest("abecd", 0))
      assert(SearchResult(4, 2) == trie.findLongest("abcdf", 0))
      assert(SearchResult(3, 1) == trie.findLongest("abcfd", 0))
    }
  }

  "SearchResult" - {
    "has correct fields for (0, 0)" in {
      val sr = SearchResult(0, 0)
      assert(0 == sr.end)
      assert(0 == sr.index)
    }

    "has correct fields for (1, 1)" in {
      val sr = SearchResult(1, 1)
      assert(1 == sr.end)
      assert(1 == sr.index)
    }

    "has correct fields for (100, 5000)" in {
      val sr = SearchResult(100, 5000)
      assert(100 == sr.end)
      assert(5000 == sr.index)
    }

    "has correct fields for (-1, -1)" in {
      val sr = SearchResult(-1, -1)
      assert(-1 == sr.end)
      assert(-1 == sr.index)
    }

    "has correct fields for (-100, -100)" in {
      val sr = SearchResult(-5, -100)
      assert(-5 == sr.end)
      assert(-100 == sr.index)
    }

    "has correct toString" in {
      assert(SearchResult(0, 0).toString == "SearchResult(0, 0)")
      assert(SearchResult(1, 1).toString == "SearchResult(1, 1)")
    }
  }
}
