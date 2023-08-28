package com.worksap.nlp.uzushio.main

import com.worksap.nlp.uzushio.lib.runners.{DeduplicateParagraphs => DedupTask}
import com.worksap.nlp.uzushio.lib.utils.Resources.AutoClosableResource
import org.apache.spark.sql.SparkSession

object DeduplicateParagraphs {
  def main(args: Array[String]): Unit = {
    val argObj = new DedupTask.ArgParser(args).toArgs
    SparkSession.builder().getOrCreate().use { spark =>
      new DedupTask(argObj, spark).process()
    }
  }
}
