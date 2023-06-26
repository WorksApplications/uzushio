package com.worksap.nlp.uzushio.warc

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
import org.apache.log4j.LogManager

/** Http response parser for warc record. */
class HttpResponseParser(bufSize: Int = 128 * 1024) extends Serializable {
  @transient lazy val logger = LogManager.getLogger(this.getClass.getSimpleName)

  private val responseParser = new DefaultHttpResponseParser()
  private val siBuffer = new SessionInputBufferImpl(bufSize)
  private val byteBuffer = Array.ofDim[Byte](bufSize)

  /** Parses WarcRecord body as http response.
    *
    * Make sure that provided warc record has proper type.
    */
  def parseWarcRecord(warc: WarcRecord) = {
    val is = new ByteArrayInputStream(warc.content)

    try {
      val resp = responseParser.parse(siBuffer, is)
      val body = readBody(siBuffer, is);
      new HttpResponseSerializable(resp, body)
    } catch {
      // TODO: data handling in the error cases
      case e: org.apache.hc.core5.http.HttpException => {
        logger.warn(s"error parsing http response: ${e}")
        new HttpResponseSerializable()
      }
    } finally {
      is.close()
    }
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
