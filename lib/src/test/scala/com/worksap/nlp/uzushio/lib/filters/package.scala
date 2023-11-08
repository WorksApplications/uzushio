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

  def testParagraphs(texts: Seq[String], nearFreqs: Seq[Int] = Seq(), exactFreqs: Seq[Int] = Seq(), paths: Seq[String] = Seq()): IndexedSeq[Paragraph] = {
    require(texts.length == nearFreqs.length || nearFreqs.isEmpty)
    require(texts.length == exactFreqs.length || exactFreqs.isEmpty)
    require(texts.length == paths.length || paths.isEmpty)

    val nearFreqs_ = if (!nearFreqs.isEmpty) nearFreqs else Seq.fill(texts.length)(1)
    val exactFreqs_ = if (!exactFreqs.isEmpty) exactFreqs else Seq.fill(texts.length)(1)
    val paths_ = if (!paths.isEmpty) paths else 0.to(texts.length).map(_ => "body>p.text")

    texts
      .zip(nearFreqs_)
      .zip(exactFreqs_)
      .zip(paths_)
      .map { case (((text, nearFreq), exactFreq), path) => Paragraph(path, text, 0, exactFreq, nearFreq) }
      .toIndexedSeq
  }
}
