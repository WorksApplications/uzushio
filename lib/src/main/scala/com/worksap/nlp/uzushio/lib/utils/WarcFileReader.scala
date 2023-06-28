package com.worksap.nlp.uzushio.lib.utils

import com.worksap.nlp.uzushio.lib.warc.WarcRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.log4j.LogManager
import org.archive.io.warc.WARCReaderFactory

import java.io.{FilterInputStream, InputStream}
import scala.collection.JavaConverters._

/** Reads [[WarcRecord]]s from a WARC file using Hadoop filesystem APIs. */
class WarcFileReader(conf: Configuration, filePath: Path) {
  @transient private lazy val logger = LogManager.getLogger(this.getClass.getSimpleName)

  /** Opens a warc file and setup an iterator of records. */
  private val fs = filePath.getFileSystem(conf)
  private val fileSize = fs.getFileStatus(filePath).getLen
  private val fsin = new CountingInputStream(fs.open(filePath))
  private val reader = WARCReaderFactory.get(filePath.getName, fsin, true)
  private val recordIter = reader.iterator

  /** Init counters to report progress. */
  private var recordsRead: Long = 0
  private var bytesRead: Long = 0

  /** Closes the file and reader. */
  def close(): Unit = {
    reader.close()
    fsin.close()
  }

  /** Reads the next record from the iterator.
    */
  def read(): WarcRecord = {
    if (!recordIter.hasNext) {
      throw new java.util.NoSuchElementException()
    }

    try {
      val record = new WarcRecord(recordIter.next())
      recordsRead += 1
      record
    } catch {
      case e: java.io.EOFException =>
        logger.warn(s"error while iterating warc, try to skip: $filePath", e)
        read()
    }
  }

  /** Returns the number of records that have been read. */
  def getRecordsRead: Long = recordsRead

  /** Returns the number of bytes that have been read. */
  def getBytesRead: Long = bytesRead

  /** Returns the proportion of the file thet has been read. */
  def getProgress: Float = {
    if (fileSize <= 0) return 1.0f
    bytesRead.toFloat / fileSize.toFloat
  }

  /** InputStream that records the number of bytes read. */
  private class CountingInputStream(in: InputStream)
      extends FilterInputStream(in) {
    override def read(): Int = {
      val result = in.read()
      if (result != -1) bytesRead += 1
      result
    }

    override def read(b: Array[Byte], off: Int, len: Int): Int = {
      val result = in.read(b, off, len)
      if (result != -1) bytesRead += result
      result
    }

    override def skip(n: Long): Long = {
      val result = in.skip(n)
      bytesRead += result
      result
    }
  }
}
