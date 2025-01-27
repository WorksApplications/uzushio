package com.worksap.nlp.uzushio.lib.lang

import java.nio.charset.{Charset, StandardCharsets}
import org.scalatest.freespec.AnyFreeSpec

class LangEstimationSpec extends AnyFreeSpec {

  "LangEstimation" - {
    val estimator = new LangEstimation()

    "detects Japanese language from a simulated Wikipedia page about Japan" in {
      // 模拟维基百科介绍日本的 HTML 页面，并用日语书写，Shift-JIS 编码
      val htmlContent = """
        <html>
          <head>
            <title>日本 - Wikipedia</title>
          </head>
          <body>
            <h1>日本</h1>
            <p>日本（にっぽん、にほん）は、東アジアに位置する島国で、太平洋に面しています。日本は北海道、本州、四国、九州の四つの主要な島から構成されています。</p>
            <p>日本の首都は東京で、人口は世界でも有数の規模を誇ります。日本は高度に発展した国であり、技術、経済、文化など多くの分野で世界に影響を与えています。</p>
            <p>日本の歴史は古く、何世紀にもわたる様々な変革と発展を遂げてきました。現代の日本は、明治維新後に急速に産業化され、世界的な経済大国となりました。</p>
            <p>第二次世界大戦後、日本は驚異的な復興を遂げ、現在では世界で最も強力な経済の一つとして知られています。</p>
          </body>
        </html>
      """
      val data = htmlContent.getBytes("Shift_JIS")
      val result = estimator.estimateLang(data, 0, Charset.forName("Shift_JIS"))
      
      // 断言检测结果应该是日语
      assert(result.isInstanceOf[ProbableLanguage])
      assert(result.asInstanceOf[ProbableLanguage].lang == "ja") // 期待的结果是日语
    }

    "detects English language from a simulated Wikipedia page about Japan" in {
      // 模拟维基百科关于日本的英文页面，并用 UTF-8 编码
      val htmlContent = """
        <html>
          <head>
            <title>Japan - Wikipedia</title>
          </head>
          <body>
            <h1>Japan</h1>
            <p>Japan is an island country in East Asia, located in the northwest Pacific Ocean. It borders the Sea of Japan to the west, and extends from the Sea of Okhotsk in the north to the East China Sea and Taiwan in the south.</p>
            <p>Japan is a highly developed country, known for its advanced technology, strong economy, and rich culture. With a population of over 125 million, Japan is the world's eleventh most populous country, and Tokyo, its capital, is one of the most populous cities in the world.</p>
            <p>The country's history dates back to the 14th century BC, and over the centuries, it has evolved through various dynasties and periods. Modern Japan emerged in the late 19th century during the Meiji Restoration, which transformed it into an industrial and economic power.</p>
            <p>After World War II, Japan experienced rapid recovery and became one of the world's leading economies. Today, Japan is known for its influence in global technology, culture, and economy.</p>
          </body>
        </html>
      """
      val data = htmlContent.getBytes(StandardCharsets.UTF_8)
      val result = estimator.estimateLang(data, 0, StandardCharsets.UTF_8)
      
      // 断言检测结果应该是英语
      assert(result.isInstanceOf[ProbableLanguage])
      assert(result.asInstanceOf[ProbableLanguage].lang == "en") // 期待的结果是英语
    }
  }
}