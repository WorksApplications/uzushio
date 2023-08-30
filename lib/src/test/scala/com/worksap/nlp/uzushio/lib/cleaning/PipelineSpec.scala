package com.worksap.nlp.uzushio.lib.cleaning

import com.typesafe.config.ConfigFactory
import com.worksap.nlp.uzushio.lib.filters.Words
import org.scalatest.freespec.AnyFreeSpec

class PipelineSpec extends AnyFreeSpec {
  "Pipeline" - {
    "can instantiate classes" in {
      val cfg = ConfigFactory.parseString(
        """{class: "com.worksap.nlp.uzushio.lib.filters.Words", list: "ng_words.txt", minimum: 3}"""
      )
      val filter = Pipeline.instantiateFilter(cfg)
      assert(filter != null)
      assert(filter.isInstanceOf[Words])
    }
  }

}
