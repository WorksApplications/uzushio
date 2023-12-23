package com.worksap.nlp.uzushio.lib.utils

import org.scalatest.freespec.AnyFreeSpec

class ParagraphsSpec extends AnyFreeSpec {
  "correctly splits paragraphs" in {
    val doc = "test1\n\ntest2\ntest3"
    val pars = Paragraphs.extractCleanParagraphs(doc)
    assert(pars.length == 2)
    assert(pars == Seq("test1", "test2\ntest3"))
  }
}
