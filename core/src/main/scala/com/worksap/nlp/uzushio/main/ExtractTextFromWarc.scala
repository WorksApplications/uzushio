package com.worksap.nlp.uzushio.main

import com.worksap.nlp.uzushio.lib.runners.{ExtractParagraphsFromWARC, WarcTextExtractionRaw}
import com.worksap.nlp.uzushio.lib.utils.Resources.AutoClosableResource
import org.apache.spark.sql.SparkSession

/** Extracts text from WARC files.
  *
  * @see
  *   [[WarcTextExtractionRaw.ConfigParser]]
  */
object ExtractTextFromWarc {
  def main(args: Array[String]): Unit = {
    val cfg = new WarcTextExtractionRaw.ConfigParser(args).asArgs()
    SparkSession.builder().appName(getClass.getSimpleName).getOrCreate().use { spark =>
      ExtractParagraphsFromWARC.run(cfg)(spark)
    }
  }
}
