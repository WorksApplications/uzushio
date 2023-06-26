package com.worksap.nlp.uzushio.warc

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/* Hadoop InputFormat for WARC files.
 *
 * Key is 1-index LongWritable. Use get() method to take Long value.
 */
class WarcInputFormat
    extends FileInputFormat[LongWritableSerializable, WarcWritable] {

  /* Opens a WARC file (possibly compressed), and returns a RecordReader for accessing it. */
  override def createRecordReader(
      split: InputSplit,
      context: TaskAttemptContext
  ) = {
    new WarcRecordReader()
  }

  override def isSplitable(context: JobContext, filename: Path): Boolean = {
    // we cannot (sanely) split warc files, due to its variable-length records.
    return false
  }
}

/* Wrapper class of {@link WarcFileReader} to implement RecordReader. */
class WarcRecordReader
    extends RecordReader[LongWritableSerializable, WarcWritable] {
  private val key = new LongWritableSerializable();
  private val value = new WarcWritable();

  private var reader: WarcFileReader = null

  override def initialize(
      split: InputSplit,
      context: TaskAttemptContext
  ): Unit = {
    reader = new WarcFileReader(
      context.getConfiguration(),
      split.asInstanceOf[FileSplit].getPath
    );
  }

  override def nextKeyValue(): Boolean = {
    try {
      val record = reader.read();
      key.set(reader.getRecordsRead());
      value.setRecord(record);
    } catch {
      case e: java.util.NoSuchElementException => { return false }
    }

    return true;
  }

  override def getCurrentKey(): LongWritableSerializable = {
    return key;
  }

  override def getCurrentValue(): WarcWritable = {
    return value;
  }

  override def getProgress(): Float = {
    return reader.getProgress();
  }

  override def close(): Unit = {
    reader.close();
  }
}
