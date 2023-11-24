package com.worksap.nlp.uzushio.lib.cleaning

import scala.collection.mutable.ArrayBuffer

case class PathSegment(tag: String, id: String, classes: Seq[String]) {
  override def toString: String = classes
    .mkString(tag + (if (classes.isEmpty) "" else "."), ".", if (id == null) "" else s"#$id")
}

object PathSegment {

  final private val EMPTY_PATH: Seq[PathSegment] = ArrayBuffer.empty

  def parsePath(path: String): Seq[PathSegment] = {
    var start = 0
    val end = path.length
    val result = new ArrayBuffer[PathSegment]()
    while (start < end) {
      val separator = path.indexOf('>', start)
      if (separator == -1) {
        result += parse(path, start, end)
        start = end
      } else {
        result += parse(path, start, separator)
        start = separator + 1
      }
    }
    if (result.isEmpty) EMPTY_PATH else result
  }

  final private val EMPTY_CLASSES: Seq[String] = new ArrayBuffer[String]()
  def parse(raw: String, start: Int, end: Int): PathSegment = {
    var dotIdx = raw.indexOf('.', start)
    var hashIdx = raw.indexOf('#', start)
    if (dotIdx > end) {
      dotIdx = -1
    }
    if (hashIdx > end) {
      hashIdx = -1
    }

    if (dotIdx == -1 && hashIdx == -1) {
      return PathSegment(raw.substring(start, end), null, EMPTY_CLASSES)
    }

    var tagEndIdx = end

    val id =
      if (hashIdx == -1) null
      else {
        tagEndIdx = hashIdx
        raw.substring(hashIdx + 1, end)
      }

    val classes =
      if (dotIdx == -1) {
        EMPTY_CLASSES
      } else {
        val classesEndIdx = tagEndIdx
        tagEndIdx = dotIdx
        var classesIdx = dotIdx
        val classes = new ArrayBuffer[String]()
        while (classesIdx < classesEndIdx) {
          var nextClassIdx = raw.indexOf('.', classesIdx + 1)
          if (nextClassIdx > end) {
            nextClassIdx = -1
          }
          if (nextClassIdx > 0) {
            classes += raw.substring(classesIdx + 1, nextClassIdx)
            classesIdx = nextClassIdx
          } else {
            if (classesIdx != classesEndIdx) {
              classes += raw.substring(classesIdx + 1, classesEndIdx)
            }
            classesIdx = classesEndIdx
          }
        }
        classes
      }

    PathSegment(
      tag = raw.substring(start, tagEndIdx),
      id = id,
      classes = classes
    )
  }

  def parse(raw: String): PathSegment = parse(raw, 0, raw.length)
}
