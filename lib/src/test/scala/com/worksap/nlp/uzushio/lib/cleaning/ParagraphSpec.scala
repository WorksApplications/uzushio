package com.worksap.nlp.uzushio.lib.cleaning

import org.scalatest.freespec.AnyFreeSpec

class ParagraphSpec extends AnyFreeSpec {
  "Paragraph" - {
    "can return css selector strings" in {
      val par = Paragraph("body>p.text", "hello")
      assert(par.cssSelectors == Seq("body", "p.text"))
    }

    "can return designated tags in path without css selector" in {
      val par = Paragraph("body>p.text", "hello")
      assert(par.extractDescendantTag(Seq("p", "span")) == Some("p"))
    }

    "do not return designated tags in path" in {
      val par = Paragraph("body>p.text", "hello")
      assert(par.extractDescendantTag(Seq("span")) == None)
    }

    "can return true if the paragraph contains designated tags" in {
      val par = Paragraph("body>p.text", "hello")
      assert(par.containsTags(Seq("p", "span")))
    }

    "do not return true if the paragraph does not contain designated tags" in {
      val par = Paragraph("body>p.text", "hello")
      assert(!par.containsTags(Seq("span")))
    }
  }
}
