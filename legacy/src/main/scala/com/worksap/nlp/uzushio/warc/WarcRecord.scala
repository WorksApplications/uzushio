package com.worksap.nlp.uzushio.warc

import scala.jdk.CollectionConverters._
import java.io.Serializable
import org.apache.commons.io.IOUtils
import org.archive.io.ArchiveRecord

/* Serializable wrapper of ArchiveRecord, with its contents loaded. */
class WarcRecord(record: ArchiveRecord) extends Serializable {
  val headers: Map[String, String] = record
    .getHeader()
    .getHeaderFields()
    .asScala
    .map { case (k, v) => (k, v.toString) }
    .toMap

  /* read contents to safely step iterator forward. */
  val content = IOUtils.toByteArray(record, record.available());

  def isResponse(): Boolean = {
    // ref https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0
    val warcType = headers.getOrElse("WARC-Type", "")
    return warcType == "response"
  }

  def isTruncated(): Boolean = {
    val truncated = headers.get("WARC-Truncated")
    return truncated.nonEmpty
  }
}
