package com.worksap.nlp.uzushio.lib.filters

import org.scalatest.freespec.AnyFreeSpec

class WordInstancesSpec extends AnyFreeSpec {
  "WordInstances" - {
    "hojichar - adult" - {
      val filter = new WordInstances("hojichar/adult_keywords_ja.txt")
      "can score single paragraph document" in {
        val doc = testDoc("18禁 20禁 21禁")
        val score = filter.scoreDocument(doc)
        assert(score == 3.0f)
      }
    }


  }
}
