package com.worksap.nlp.uzushio.lib.warc

import org.apache.hadoop.io.Writable

import java.io.{DataInput, DataOutput, Serializable};

/** A mutable wrapper around a [[WarcRecord]] implementing the Hadoop Writable
  * and Serializable (for Spark) interfaces.
  */
class WarcWritable(private var record: WarcRecord = null)
    extends Writable
    with Serializable {

  /** Returns the record currently wrapped by this writable. */
  def getRecord: WarcRecord = record

  /** Updates the record held within this writable wrapper. */
  def setRecord(newRecord: WarcRecord): Unit = {
    record = newRecord;
  }

  /** Appends the current record to a [[DataOutput]] stream. */
  override def write(out: DataOutput): Unit = {
    // TODO: impl (not neccessary for current use case)
    // if (record != null) record.write(out);
  }

  /** Parses a [[WarcRecord]] out of a [[DataInput]] stream, and make it the
    * current record.
    */
  override def readFields(in: DataInput): Unit = {
    // TODO: impl (not neccessary for current use case)
    // record = new WarcRecord(in);
  }
}
