package com.worksap.nlp.uzushio.lib.utils

import org.scalatest.freespec.AnyFreeSpec

class SentenceIteratorSpec extends AnyFreeSpec {
  "SentenceIterator" - {
    "indexOf" - {
      "returns correct value for a simple case" in {
        val seq = "this。 is a test"
        assert(4 == SentenceIterator.indexOfSeparator(seq, 0, seq.length))
      }

      "works with empty string" in {
        val seq = ""
        assert(-1 == SentenceIterator.indexOfSeparator(seq, 0, seq.length))
      }

      "works with last index of a string" in {
        val seq = "test"
        assert(-1 == SentenceIterator.indexOfSeparator(seq, 4, seq.length))
      }

      "works with not last index of a string not containing required characters" in {
        val seq = "test"
        assert(-1 == SentenceIterator.indexOfSeparator(seq, 2, seq.length))
      }
    }

    "produces correct sequence of sentences" in {
      val iter = new SentenceIterator("this。 is a test", 1024)
      assert(Seq("this。", " is a test") == iter.toSeq)
    }

  }
}
