package com.worksap.nlp.uzushio.lib.warc

import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object WarcLoader {
  /* Load WARC file as RDD. */
  def readWarcFiles(
      spark: SparkContext,
      name: String
  ): RDD[WarcRecord] = {
    spark.newAPIHadoopFile[LongWritable, WarcWritable, WarcInputFormat](
      name
    ).map { case (_, v) => v.getRecord }
  }
}
