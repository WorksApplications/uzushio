package com.worksap.nlp.uzushio.lib.warc

import com.worksap.nlp.uzushio.lib.utils.ClasspathAccess
import org.scalatest.freespec.AnyFreeSpec

import java.util.UUID

class WarcEntryParserSpec extends AnyFreeSpec with ClasspathAccess {
  "WarcEntryParser" - {
    val parser = new WarcEntryParser()
    "parses http header" in {
      val data = classpathBytes("lang/shift_jis.txt")
      val parsed = parser.parseHttpHeader(data)
      assert(parsed.isDefined)
      val Some((message, offset)) = parsed
      assert(offset == 197)
      assertResult("text/html")(message.getHeader("Content-Type").getValue)
      val date = WarcEntryParser.resolveEarliestDate("", message)
      assert("2012-12-29T16:50:56" == date)
    }

    "parses UUID" - {
      "<urn:uuid:f1a9564a-ae00-40ef-838e-a4486a83fd1d>" in {
        val uuid = WarcEntryParser.parseWarcUuid(
          "<urn:uuid:f1a9564a-ae00-40ef-838e-a4486a83fd1d>"
        )
        assert(uuid == "f1a9564a-ae00-40ef-838e-a4486a83fd1d")
      }
    }
  }
}
