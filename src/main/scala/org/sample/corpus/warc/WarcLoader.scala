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

  /* Load WARC response records from file as RDD  */
  def readFullResponseFrom(
      spark: SparkSession,
      name: String
  ): RDD[WarcRecord] = {
    readFrom(spark, name).filter(arc => {
      val contentType = arc.headers.getOrElse("Content-Type", "")
      arc.isResponse && contentType.startsWith(
        "application/http"
      ) && !arc.isTruncated
    })
  }
}
