package com.worksap.nlp.uzushio.lib.warc

import com.worksap.nlp.uzushio.lib.warc.WarcRecord.{RECORD_ACCESS_DATE, RECORD_ID, RECORD_TRUNCATED, RECORD_TYPE, RECORD_URL}
import org.apache.commons.io.IOUtils
import org.archive.format.warc.WARCConstants
import org.archive.io.ArchiveRecord

import java.io.Serializable

/** Serializable wrapper of ArchiveRecord, with body read in memory. */
class WarcRecord(record: ArchiveRecord) extends Serializable {
  // capture headers
  private val headers = record.getHeader.getHeaderFields
  // read body of request
  val content: Array[Byte] = IOUtils.toByteArray(record, record.available());

  def isResponse: Boolean = {
    // ref https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0
    val warcType = headers.getOrDefault(RECORD_TYPE, "")
    "response" == warcType
  }

  def isTruncated: Boolean = {
    headers.get(RECORD_TRUNCATED) match {
      case null => false
      case s: CharSequence => !s.isEmpty
      case _ => true
    }
  }

  def url: String = {
    headers.getOrDefault(RECORD_URL, "").toString
  }

  def accessDate: String = {
    headers.get(RECORD_ACCESS_DATE).toString
  }

  def docId: String = {
    headers.get(RECORD_ID).toString
  }
}

object WarcRecord {
  final val RECORD_TYPE = WARCConstants.HEADER_KEY_TYPE
  final val RECORD_TRUNCATED = WARCConstants.HEADER_KEY_TRUNCATED
  final val RECORD_URL = WARCConstants.HEADER_KEY_URI
  final val RECORD_ACCESS_DATE = WARCConstants.HEADER_KEY_DATE
  final val RECORD_ID = WARCConstants.HEADER_KEY_ID
}
