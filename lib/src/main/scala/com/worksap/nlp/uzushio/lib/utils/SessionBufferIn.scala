package org.apache.hc.core5.http.impl.nio

import org.apache.hc.core5.http.nio.SessionInputBuffer

trait ResettableBuffer extends SessionInputBuffer {
  def clear(): Unit
  def putBytes(bytes: Array[Byte]): Unit

  def position(): Int
}

object SessionBufferAccess {
  def instance(size: Int, lineSize: Int): ResettableBuffer = new SessionInputBufferImpl(size, lineSize) with ResettableBuffer {
    override def putBytes(bytes: Array[Byte]): Unit = {
      val b = buffer()
      val totalSize = size.min(bytes.length)
      b.clear()
      b.put( bytes, 0, totalSize)
      b.position(totalSize)
      b.limit(b.capacity())
    }

    override def clear(): Unit = super.clear()

    override def position(): Int = buffer().position()
  }
}
