package org.sample.corpus.warc

import java.io.Serializable
import org.apache.commons.io.IOUtils
import org.archive.io.ArchiveRecord

/* Serializable wrapper of ArchiveRecord, with its contents loaded. */
class WarcRecord(record: ArchiveRecord) extends Serializable {
  val content = IOUtils.toByteArray(record, record.available());
}
