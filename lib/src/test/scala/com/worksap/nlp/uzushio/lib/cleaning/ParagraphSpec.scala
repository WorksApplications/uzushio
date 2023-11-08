package com.worksap.nlp.uzushio.lib.cleaning

import org.scalatest.freespec.AnyFreeSpec

class ParagraphSpec extends AnyFreeSpec {
  "Paragraph" - {
    "return css selector strings" in {
      val par = Paragraph("body>p.text", "hello")
      assert(par.cssSelectors == Seq("body", "p.text"))
    }
  }
}
