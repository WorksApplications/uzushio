package org.sample.corpus.warc

import java.io.Serializable
import org.apache.hc.core5.http.ClassicHttpResponse

/** Seritalizable wrapper of ClassicHttpResponse. */
class HttpResponseSerializable(resp: ClassicHttpResponse, val body: Array[Byte])
    extends Serializable {
  def getHeader(name: String): Option[String] = {
    Option(resp.getHeader(name)).map(_.getValue)
  }

  def getHeaders(): Seq[(String, String)] = {
    resp.getHeaders().map(header => (header.getName(), header.getValue()))
  }
}
