package com.worksap.nlp.uzushio.lib.cleaning

import com.typesafe.config.ConfigFactory
import com.worksap.nlp.uzushio.lib.filters.Words
import org.scalatest.freespec.AnyFreeSpec

class PipelineSpec extends AnyFreeSpec {
  "Pipeline" - {
    "can instantiate class fully specified" in {
      val cfg = ConfigFactory.parseString(
        """{class: "com.worksap.nlp.uzushio.lib.filters.Words", list: "ng_words.txt", minimum: 3}"""
      )
      val filter = Pipeline.instantiateFilter(cfg)
      assert(filter != null)
      assert(filter.isInstanceOf[Words])
    }

    "can instantiate class with default value" in {
      val cfg = ConfigFactory.parseString(
        """{class: "com.worksap.nlp.uzushio.lib.filters.Words", list: "ng_words.txt"}"""
      )
      val filter = Pipeline.instantiateFilter(cfg)
      assert(filter != null)
      assert(filter.isInstanceOf[Words])
    }

    "can instantiate pipeline from classpath" - {
      val pipeline = Pipeline.make("doc_len.conf")
      assert(pipeline != null)
    }
  }

}
