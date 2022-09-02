package org.sample.corpus.warc

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object WarcLoader {
  /* Load WARC file as RDD. */
  def readFrom(
      spark: SparkSession,
      name: String
  ): RDD[WarcRecord] = {
    spark.sparkContext
      .newAPIHadoopFile(
        name,
        classOf[WarcInputFormat],
        classOf[LongWritableSerializable],
        classOf[WarcWritable]
      )
      .map { case (k, v) => v.getRecord() }
  }
}
