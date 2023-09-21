package com.worksap.nlp.uzushio.lib.cleaning

import org.scalatest.freespec.AnyFreeSpec

class DocumentSpec extends AnyFreeSpec {
  "Document" - {
    "computes next double correctly" in {
      val docs = (1 to 1000).map(i => Document(Vector.empty, docId = ('a' + i).toChar.toString))
      val doubles = docs.map(_.randomDouble)
      for (d <- doubles) {
        assert(d < 1.0)
      }
      assert(doubles.distinct.size == 1000)
      val sum = doubles.sum
      assert((sum - 500).abs < 2)
    }
  }
}
