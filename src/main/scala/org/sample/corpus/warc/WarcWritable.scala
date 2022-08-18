package org.sample.corpus.warc

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Serializable
import org.apache.hadoop.io.Writable;

/* A mutable wrapper around a {@link WarcRecord} implementing the Hadoop
 * Writable and Serializable (for Spark) interfaces.
 */
class WarcWritable(var record: WarcRecord) extends Writable with Serializable {
  def this() = this(null)

  /* Returns the record currently wrapped by this writable. */
  def getRecord(): WarcRecord = {
    return record;
  }

  /* Updates the record held within this writable wrapper. */
  def setRecord(newRecord: WarcRecord): Unit = {
    record = newRecord;
  }

  /* Appends the current record to a {@link DataOutput} stream. */
  override def write(out: DataOutput): Unit = {
    // TODO: impl
    // if (record != null) record.write(out);
  }

  /* Parses a {@link WarcRecord} out of a {@link DataInput} stream, and make it
   * the current record.
   */
  override def readFields(in: DataInput): Unit = {
    // TODO: impl
    // record = new WarcRecord(in);
  }
}
