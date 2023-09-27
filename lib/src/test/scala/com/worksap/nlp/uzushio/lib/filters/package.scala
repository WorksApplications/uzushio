package com.worksap.nlp.uzushio.lib

import com.worksap.nlp.uzushio.lib.cleaning.{Document, Paragraph}
import com.worksap.nlp.uzushio.lib.filters.base.FilterBase

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  ObjectInputStream,
  ObjectOutputStream
}
import scala.annotation.varargs

package object filters {
  def cloneViaSerialization[T <: FilterBase](f: T): T = {
    val bytes = new ByteArrayOutputStream()
    val str = new ObjectOutputStream(bytes)
    str.writeObject(f)
    str.flush()
    val data = bytes.toByteArray
    val binput = new ByteArrayInputStream(data)
    val istr = new ObjectInputStream(binput)
    val obj = istr.readObject()
    f.getClass.cast(obj)
  }

  def testDoc(data: String*): Document = {
    Document(
      data.map { text =>
        Paragraph("", text)
      }.toIndexedSeq
    )
  }

  def testParagraphs(texts: Seq[String], nearFreqs: Seq[Int]): IndexedSeq[Paragraph] = {
      (texts, nearFreqs)
        .zipped
        .map ((text, freq) => Paragraph("", text, 0, 1, freq))
        .toIndexedSeq
  }
}
