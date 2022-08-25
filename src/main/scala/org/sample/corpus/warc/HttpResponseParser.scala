package org.sample.corpus.warc

import java.io.ByteArrayInputStream
import java.io.InputStream
import java.io.SequenceInputStream
import java.io.Serializable
import org.apache.commons.io.IOUtils
import org.apache.hc.core5.http.impl.io.{
  DefaultHttpResponseParser,
  SessionInputBufferImpl
}
import org.apache.hc.core5.http.io.SessionInputBuffer

class HttpResponseParser(bufSize: Int) extends Serializable {
  private val responseParser = new DefaultHttpResponseParser()
  private val siBuffer = new SessionInputBufferImpl(bufSize)
  private val byteBuffer = Array.ofDim[Byte](bufSize)

  def this() = this(128 * 1024)

  /** Parses WarcRecord body as http response.
    *
    * Make sure that provided warc record has proper type.
    */
  def parseWarcRecord(warc: WarcRecord) = {
    val is = new ByteArrayInputStream(warc.content)

    val resp = responseParser.parse(siBuffer, is)
    val body = readBody(siBuffer, is);

    // TODO: error handling
    //   case e: java.io.IOException => {}
    //   case e: org.apache.hc.core5.http.HttpException => {}

    new HttpResponseSerializable(resp, body)
  }

  /** Read body bytes from buffers after headers are read. */
  private def readBody(isBuf: SessionInputBuffer, rest: InputStream) = {
    val emptyIs = new java.io.ByteArrayInputStream(Array.emptyByteArray)
    val restBytes = isBuf.read(byteBuffer, emptyIs)

    IOUtils.toByteArray(
      new SequenceInputStream(
        new ByteArrayInputStream(byteBuffer.slice(0, restBytes)),
        rest
      )
    )
  }
}
