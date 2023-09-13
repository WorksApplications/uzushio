package com.worksap.nlp.uzushio.lib.filters

import com.worksap.nlp.uzushio.lib.utils.Paragraphs
import org.scalatest.freespec.AnyFreeSpec

class LinkCharRatioSpec extends AnyFreeSpec {
  def a(x: String): String =
    s"${Paragraphs.HTML_LINK_START}$x${Paragraphs.HTML_LINK_END}"

  "LinkCharRatio" - {
    val filter = new LinkCharRatio()
    "computes correct ratio for empty document" in {
      val doc = testDoc("")
      assert(0.0f == filter.calcLinkCharRatio(doc))
    }

    "computes correct ratio for non-empty document without links" in {
      val doc = testDoc("test")
      assert(0.0f == filter.calcLinkCharRatio(doc))
    }

    "computes correct ratio for non-empty document with links" in {
      val doc = testDoc(s"test${a("baka")}")
      assert(0.5f == filter.calcLinkCharRatio(doc))
    }
  }
}
