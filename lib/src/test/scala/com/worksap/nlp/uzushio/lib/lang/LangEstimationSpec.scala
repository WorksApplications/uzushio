package com.worksap.nlp.uzushio.lib.lang

import com.worksap.nlp.uzushio.lib.utils.ClasspathAccess
import org.scalatest.freespec.AnyFreeSpec

class LangEstimationSpec extends AnyFreeSpec with ClasspathAccess {
  "LangEstimation" - {
    val sniffer = new LangTagSniffer()
    "sniffs charset shift_jis fragment" in {
      val data = classpathBytes("lang/shift_jis.txt")
      val tags = sniffer.sniffTags(data, 0, data.length)
      assert("Shift-JIS" == tags.charset)
    }
  }
}
