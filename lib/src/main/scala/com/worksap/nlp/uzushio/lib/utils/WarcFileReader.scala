package com.worksap.nlp.uzushio.lib.utils

import com.google.common.io.CountingInputStream
import com.worksap.nlp.uzushio.lib.utils.WarcFileReader.MAX_RECORD_SIZE
import com.worksap.nlp.uzushio.lib.warc.WarcRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.log4j.LogManager
import org.archive.io.warc.WARCReaderFactory

import java.io.BufferedInputStream

/** Reads [[WarcRecord]]s from a WARC file using Hadoop filesystem APIs. */
class WarcFileReader(conf: Configuration, filePath: Path) {
  @transient private lazy val logger = LogManager.getLogger(this.getClass.getSimpleName)

  /** Opens a warc file and setup an iterator of records. */
  private def fs = filePath.getFileSystem(conf)
  private val fileSize = fs.getFileStatus(filePath).getLen
  private val fsin = {
    val rawStream = fs.open(filePath)
    val wrapped = if (rawStream.markSupported()) {
      rawStream
    } else new BufferedInputStream(rawStream)
    //noinspection UnstableApiUsage
    new CountingInputStream(wrapped)
  }
  private val reader = WARCReaderFactory.get(filePath.getName, fsin, true)
  private val recordIter = reader.iterator

  /** Init counters to report progress. */
  private var recordsRead: Long = 0

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
      val rec = recordIter.next()
      val length = rec.available()
      if (length > MAX_RECORD_SIZE) {
        rec.skip(length)
        logger.info(s"from $filePath skipped ${rec.getHeader}")
        recordsRead += 1
        read()
      } else {
        val record = new WarcRecord(rec, filePath)
        recordsRead += 1
        record
      }
    } catch {
      case e: java.io.EOFException =>
        logger.warn(s"error while iterating warc, try to skip: $filePath", e)
        read()
    }
  }

  /** Returns the number of records that have been read. */
  def getRecordsRead: Long = recordsRead

  /** Returns the number of bytes that have been read. */
  def bytesRead: Long = fsin.getCount

  /** Returns the proportion of the file that has been read. */
  def getProgress: Float = {
    if (fileSize <= 0) return 1.0f
    bytesRead.toFloat / fileSize.toFloat
  }
}

object WarcFileReader {
  final val MAX_RECORD_SIZE = 16 * 1024 * 1024 // 16MB
}
