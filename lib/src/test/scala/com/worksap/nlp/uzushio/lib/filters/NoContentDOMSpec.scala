package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.cleaning.Paragraph
import org.scalatest.freespec.AnyFreeSpec

class NoContentDOMSpec extends AnyFreeSpec {
  "NoContentDOM" - {
    val filter = new NoContentDOM()

    "do no operation for paragraph in tag that be able to have content" in {
      val p = Paragraph("body>article>p", "text")
      assert(filter.checkParagraph(p).remove == null)
    }

    "sign remove for header tag paragraph" in {
      val p = Paragraph("body>header>p", "test")
      assert(filter.checkParagraph(p).remove != null)
    }

    "sign remove for footer tag paragraph" in {
      val p = Paragraph("body>footer>p", "test")
      assert(filter.checkParagraph(p).remove != null)
    }

    "sign remove for aside tag paragraph" in {
      val p = Paragraph("body>aside>p", "test")
      assert(filter.checkParagraph(p).remove != null)
    }

    "sign remove for nav tag paragraph" in {
      val p = Paragraph("body>nav>p", "test")
      assert(filter.checkParagraph(p).remove != null)
    }

    "sign remove for div tag with header class paragraph" in {
      val p = Paragraph("body>div.header>p", "test")
      assert(filter.checkParagraph(p).remove != null)
    }

    "sign remove for div tag with header id paragraph" in {
      val p = Paragraph("body>div#header>p", "test")
      assert(filter.checkParagraph(p).remove != null)
    }
  }
}
