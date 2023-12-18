package com.worksap.nlp.uzushio.lib.utils

class SentenceIterator(input: String, maxLength: Int) extends Iterator[String] {

  private var start = 0

  override def hasNext: Boolean = start < input.length

  override def next(): String = {
    val curStart = start
    var curEnd = SentenceIterator.indexOfSeparator(input, curStart, input.length) match {
      case -1 => input.length
      case x => x + 1
    }

    val curLen = curEnd - curStart
    if (curLen > maxLength) {
      curEnd = curStart + maxLength
    }

    start = curEnd

    input.substring(curStart, curEnd)
  }
}

object SentenceIterator {
  private val SEPARATORS = "\n。、！？!?".toCharArray

  def indexOfSeparator(input: CharSequence, start: Int, end: Int): Int = {
    val seps = SEPARATORS
    val nseps = seps.length

    if (start < 0 || start > input.length()) {
      throw new IndexOutOfBoundsException()
    }

    if (end < 0 || end > input.length()) {
      throw new IndexOutOfBoundsException()
    }

    var i = start
    while (i < end) {
      val ch = input.charAt(i)
      var j = 0
      while (j < nseps) {
        val ch0 = seps(j)
        if (ch == ch0) {
          return i
        }
        j += 1
      }
      i += 1
    }
    -1
  }
}
