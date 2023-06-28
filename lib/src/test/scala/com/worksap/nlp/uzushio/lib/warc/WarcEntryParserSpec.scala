package com.worksap.nlp.uzushio.lib.warc

import com.worksap.nlp.uzushio.lib.utils.ClasspathAccess
import org.scalatest.freespec.AnyFreeSpec

class WarcEntryParserSpec extends AnyFreeSpec with ClasspathAccess {
  "WarcEntryParser" - {
    val parser = new WarcEntryParser()
    "parses http header" in {
      val data = classpathBytes("lang/shift_jis.txt")
      val parsed = parser.parseHttpHeader(data)
      assert(parsed.isDefined)
      val Some((message, offset)) = parsed
      assertResult(206)(offset)
      assertResult("text/html")(message.getHeader("Content-Type").getValue)
      val date = WarcEntryParser.resolveEarliestDate("", message)
      assert("2012-12-29T16:50:56" == date)
    }
  }
}
