package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.{Document, Paragraph}
import org.scalatest.freespec.AnyFreeSpec

class CompressionRateSpec extends AnyFreeSpec {
  "CompressionRate" - {
    "correctly survives serialization" in {
      val doc = Document(Seq(Paragraph("", "test1 test2")))
      val f1 = new CompressionRate(0.1f, 1.2f)
      val b1 = f1.encodeDocContent(doc)
      val f2 = cloneViaSerialization(f1)
      val b2 = f2.encodeDocContent(doc)
      assert(!(b1 eq b2))
      assert(!(f1.High eq f2.High))
      assert(!(f1 eq f2))
      assert(f1.toString == f2.toString)
    }

    "computes ratio" in {
      val f1 = new CompressionRate(0.1f, 1.2f)
      val doc = testDoc("test1test1", "test2test2", "test5test5")
      val ratio = f1.compressionRatio(doc)
      assert(ratio < 1)
    }
  }

}
