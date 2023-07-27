package com.worksap.nlp.uzushio.lib.stats

import com.worksap.nlp.uzushio.lib.stats.SimHashProcessor.addVector
import com.worksap.nlp.uzushio.lib.utils.Ziggurat
import it.unimi.dsi.util.XorShiftStarRandomGenerator

class NgramHashExtractor(private val minOrder: Int, private val maxOrder: Int)
    extends Serializable {
  require(minOrder > 0)
  require(maxOrder > 0)
  require(minOrder < maxOrder)

  @inline
  final def compute(data: CharSequence)(@inline fn: Long => Unit): Unit = {
    var i = 0
    val minOrder = this.minOrder - 1
    val maxOrder = this.maxOrder
    val end = data.length()
    while (i < end) {
      var order = 0
      var hashState = NgramHashExtractor.HASH_SEED
      while (order < maxOrder && i + order < end) {
        val c = data.charAt(i + order)
        if (c == '\n') {
          order = maxOrder
        } else {
          hashState = NgramHashExtractor.mix(hashState, c & 0xffffL)
          if (order >= minOrder) {
            val hash = NgramHashExtractor.mix(hashState, order)
            fn(hash): @inline
          }
        }

        order += 1
      }
      i += 1
    }
  }

}

object NgramHashExtractor {
  final val HASH_SEED = 15213125612L
  final val HASH_MULT = 6364136223846793005L
  final val HASH_ADD = 1442695040888963407L

  def mix(seed: Long, v: Long): Long = {
    val x = (v + HASH_ADD) ^ seed
    ror(x * HASH_MULT)
  }

  def ror(x: Long): Long = java.lang.Long.rotateRight(x, 23)

  def hashString(x: String): Long = {
    var state = 0xdeadbeeffeed133L
    val nchars = x.length
    var i = 0
    while (i < nchars) {
      state = mix(state, x.charAt(i) & 0xffffL)
      i += 1
    }
    mix(state, nchars)
  }
}

class SimHashProcessor(private val size: Int) extends Serializable {
  def init: Array[Float] = new Array[Float](size)

  def update(
      state: Array[Float],
      data: CharSequence,
      ngrams: NgramHashExtractor
  ): Unit = {
    ngrams.compute(data) { hash =>
      addVector(state, hash)
    }
  }

  def result(state: Array[Float]): Array[Byte] = {
    val len1 = state.length
    val resultLen = (len1 / 8) + (if ((len1 & 7) != 0) 1 else 0)
    val result = new Array[Byte](resultLen)
    var step = 0
    while (step < resultLen) {
      val offset = step * 8
      var i = 0
      var l = 0
      while (i < 8 && offset + i < len1) {
        val x = state(i + offset)
        if (x > 0) {
          l |= (1 << i)
        }
        i += 1
      }
      result(step) = l.toByte
      step += 1
    }
    result
  }
}

object SimHashProcessor {
  def addVector(state: Array[Float], hash: Long): Unit = {
    val rng = new XorShiftStarRandomGenerator(hash)

    var i = 0
    val len = state.length
    while (i < len) {
      state(i) = state(i) + Ziggurat.computeNextGaussian(rng).toFloat
      i += 1
    }

  }
}
