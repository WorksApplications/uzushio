package com.worksap.nlp.uzushio.lib.cleaning

import com.typesafe.config.ConfigFactory
import com.worksap.nlp.uzushio.lib.filters.WordInstances
import com.worksap.nlp.uzushio.lib.filters.base.DocFilter
import org.scalatest.freespec.AnyFreeSpec

case class TestFilter(test: String) extends DocFilter {
  override def checkDocument(doc: Document): Document = Document(Paragraph("", test))
}

class PipelineSpec extends AnyFreeSpec {
  "Pipeline" - {
    "can instantiate class fully specified" in {
      val cfg = ConfigFactory.parseString(
        """{class: WordInstances, list: "ng_words.txt", minimum: 3}"""
      )
      val filter = Pipeline.instantiateFilter(cfg)
      assert(filter != null)
      assert(filter.isInstanceOf[WordInstances])
    }

    "can instantiate class with default value" in {
      val cfg = ConfigFactory.parseString(
        """{class: WordInstances, list: "ng_words.txt"}"""
      )
      val filter = Pipeline.instantiateFilter(cfg)
      assert(filter != null)
      assert(filter.isInstanceOf[WordInstances])
    }

    "can instantiate pipeline from classpath" - {
      val pipeline = Pipeline.make("doc_len.conf", ConfigFactory.empty())
      assert(pipeline != null)
    }

    "can instantiate filter with props" - {
      val cfg = ConfigFactory.parseString(
        """filters: [
           {class: "com.worksap.nlp.uzushio.lib.cleaning.TestFilter", test: ${a} }
        ]"""
      )
      val props = ConfigFactory.parseString("""a: value""")
      val pipeline = Pipeline.make(cfg, props)
      val result = pipeline.applyFilters(Document())
      assert(result.paragraphs.length == 1)
      assert(result.paragraphs.head.text == "value")
    }
  }

}
