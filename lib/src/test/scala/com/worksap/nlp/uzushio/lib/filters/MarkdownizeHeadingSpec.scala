package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.Paragraph
import org.scalatest.freespec.AnyFreeSpec

class MarkdownizeHeadingSpec extends AnyFreeSpec {
  "MarkdownizeHeading" - {
    val filter = new MarkdownizeHeading()

    "do no operation for empty paragraph" in {
      val p = Paragraph("body>p.text", "")
      assert("" == filter.checkParagraph(p).text)
    }

    "do no operation for no heading paragraph" in {
      val p = Paragraph("body>p.text", "test")
      assert("test" == filter.checkParagraph(p).text)
    }

    "add markdown heading symbol for h1 paragraph" in {
       val p = Paragraph("body>h1.text", "test")
       assert("# test" == filter.checkParagraph(p).text)
    }

    "add markdown heading symbol for h2 paragraph" in {
       val p = Paragraph("body>h2.text", "test")
       assert("## test" == filter.checkParagraph(p).text)
    }

    "add markdown heading symbol for h3 paragraph" in {
       val p = Paragraph("body>h3.text", "test")
       assert("### test" == filter.checkParagraph(p).text)
    }

    "add markdown heading symbol for h4 paragraph" in {
       val p = Paragraph("body>h4.text", "test")
       assert("#### test" == filter.checkParagraph(p).text)
    }

    "add markdown heading symbol for h5 paragraph" in {
       val p = Paragraph("body>h5.text", "test")
       assert("##### test" == filter.checkParagraph(p).text)
    }

    "add markdown heading symbol for h6 paragraph" in {
       val p = Paragraph("body>h6.text", "test")
       assert("###### test" == filter.checkParagraph(p).text)
    }
  }
}
