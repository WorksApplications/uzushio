package com.worksap.nlp.uzushio.lib.stats

import com.worksap.nlp.uzushio.lib.utils.MathUtil
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

import java.util.Random

case class CountMinSketchState(
    rows: Int,
    cols: Int,
    counts: Array[Long]
) {
  def update(hasher: Hasher, value: Long): Unit = {

  }
}

case class Hasher(
    coeffs: Array[Long]
                 ) {
  def hash(c1: Long, c2: Long, value: Long): Long = {
    val x = (value * c1) + c2 // mod 2^64
    java.lang.Long.rotateRight(x, 23)
  }
}

object Hasher {
  def make(num: Int): Hasher = {
    val rng = new Random(0xdeadbeef)
    Hasher(Array.fill(num * 2)(rng.nextLong()))
  }
}

class CountMinSketch(
    private val rows: Int,
    private val cols: Int,
    private val ngrams: NgramHashExtractor,
    private val hasher: Hasher,
                    )
    extends Aggregator[String, CountMinSketchState, CountMinSketchState] {
  override def zero: CountMinSketchState = CountMinSketchState(rows, cols, new Array[Long](rows * cols))

  override def reduce(b: CountMinSketchState, a: String): CountMinSketchState = {
    ngrams.compute(a) { hash =>
      b.update(hasher, hash)
    }
    b
  }


  override def merge(
      b1: CountMinSketchState,
      b2: CountMinSketchState
  ): CountMinSketchState = {
    val result = b1.copy()
    MathUtil.addArray(result.counts, b2.counts)
    result
  }

  override def finish(reduction: CountMinSketchState): CountMinSketchState =
    reduction

  override def bufferEncoder: Encoder[CountMinSketchState] = Encoders.product

  override def outputEncoder: Encoder[CountMinSketchState] = Encoders.product
}
