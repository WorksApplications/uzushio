package com.worksap.nlp.uzushio.lib.warc

import com.worksap.nlp.uzushio.lib.utils.WarcFileReader
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}

/** Hadoop InputFormat for WARC files.
  *
  * Key is 1-index LongWritable. Use get() method to take Long value.
  */
class WarcInputFormat extends FileInputFormat[LongWritable, WarcWritable] {

  /** Opens a WARC file (possibly compressed), and returns a RecordReader for accessing it.
    */
  override def createRecordReader(
      split: InputSplit,
      context: TaskAttemptContext
  ) = {
    new WarcRecordReader()
  }

  override def isSplitable(context: JobContext, filename: Path): Boolean = {
    // we cannot (sanely) split warc files, due to its variable-length records.
    false
  }
}

/** Wrapper class of [[WarcFileReader]] to implement RecordReader. */
class WarcRecordReader extends RecordReader[LongWritable, WarcWritable] {
  private val key = new LongWritable()
  private val value = new WarcWritable()

  private var reader: WarcFileReader = null

  override def initialize(
      split: InputSplit,
      context: TaskAttemptContext
  ): Unit = {
    reader = new WarcFileReader(
      context.getConfiguration,
      split.asInstanceOf[FileSplit].getPath
    )
  }

  override def nextKeyValue(): Boolean = {
    try {
      val record = reader.read()
      key.set(reader.getRecordsRead)
      value.setRecord(record)
      true
    } catch {
      case _: java.util.NoSuchElementException => false
    }
  }

  override def getCurrentKey: LongWritable = key

  override def getCurrentValue: WarcWritable = value

  override def getProgress: Float = reader.getProgress

  override def close(): Unit = {
    reader.close()
  }
}
