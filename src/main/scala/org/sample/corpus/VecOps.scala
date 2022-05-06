package org.sample.corpus

import org.apache.spark.ml.linalg.{Vector, Vectors, DenseVector, SparseVector}

object VecOps {
  def add(v1: Vector, v2: Vector): Vector = {
    require(
      v1.size == v2.size,
      s"Vector dimensions do not match: Dim(v1)=${v1.size} and Dim(v2)=${v2.size}."
    )

    (v1, v2) match {
      case (DenseVector(vs1), DenseVector(vs2)) => {
        Vectors.dense(vs1.zip(vs2).map(z => z._1 + z._2))
      }
      case (DenseVector(vs1), SparseVector(n, ids, vs2)) => {
        val newVs = vs1.clone
        ids.zip(vs2).foreach(z => newVs(z._1) += z._2)
        Vectors.dense(newVs)
      }
      case (SparseVector(n, ids, vs2), DenseVector(vs1)) => {
        val newVs = vs1.clone
        ids.zip(vs2).foreach(z => newVs(z._1) += z._2)
        Vectors.dense(newVs)
      }
      case (SparseVector(n1, ids1, vs1), SparseVector(n2, ids2, vs2)) => {
        val newIds = new Array[Int](ids1.length + ids2.length)
        val newVs = new Array[Double](ids1.length + ids2.length)
        var (cnt, i1, i2) = (0, 0, 0)
        while (i1 < ids1.length || i2 < ids2.length) {
          if (i1 >= ids1.length) {
            newIds(cnt) = ids2(i2)
            newVs(cnt) = vs2(i2)
            i2 += 1
          } else if (i2 >= ids2.length) {
            newIds(cnt) = ids1(i1)
            newVs(cnt) = vs1(i1)
            i1 += 1
          } else if (ids1(i1) == ids2(i2)) {
            newIds(cnt) = ids1(i1)
            newVs(cnt) = vs1(i1) + vs2(i2)
            i1 += 1; i2 += 2
          } else if (ids1(i1) < ids2(i2)) {
            newIds(cnt) = ids1(i1)
            newVs(cnt) = vs1(i1)
            i1 += 1
          } else { // (ids1(i1) > ids2(i2))
            newIds(cnt) = ids2(i2)
            newVs(cnt) = vs2(i2)
            i2 += 1
          }
          cnt += 1
        }
        Vectors.sparse(n1, newIds.take(cnt), newVs.take(cnt))
      }
      case _ =>
        throw new IllegalArgumentException(
          s"Do not support vector type ${v1.getClass}, ${v2.getClass}"
        )
    }
  }

  def sub(v1: Vector, v2: Vector): Vector = {
    (v1, v2) match {
      case (DenseVector(vs1), DenseVector(vs2)) => {
        Vectors.dense(vs1.zip(vs2).map(z => z._1 - z._2))
      }
      case (DenseVector(vs1), SparseVector(n, ids, vs2)) => {
        val newVs = vs1.clone
        ids.zip(vs2).foreach(z => newVs(z._1) -= z._2)
        Vectors.dense(newVs)
      }
      case (SparseVector(n, ids, vs1), DenseVector(vs2)) => {
        val newVs = vs2.map(-_)
        ids.zip(vs1).foreach(z => newVs(z._1) += z._2)
        Vectors.dense(newVs)
      }
      case (SparseVector(n1, ids1, vs1), SparseVector(n2, ids2, vs2)) => {
        val newIds = new Array[Int](ids1.length + ids2.length)
        val newVs = new Array[Double](ids1.length + ids2.length)
        var (cnt, i1, i2) = (0, 0, 0)
        while (i1 < ids1.length || i2 < ids2.length) {
          if (i1 >= ids1.length) {
            newIds(cnt) = ids2(i2)
            newVs(cnt) = -vs2(i2)
            i2 += 1
          } else if (i2 >= ids2.length) {
            newIds(cnt) = ids1(i1)
            newVs(cnt) = vs1(i1)
            i1 += 1
          } else if (ids1(i1) == ids2(i2)) {
            newIds(cnt) = ids1(i1)
            newVs(cnt) = vs1(i1) - vs2(i2)
            i1 += 1; i2 += 2
          } else if (ids1(i1) < ids2(i2)) {
            newIds(cnt) = ids1(i1)
            newVs(cnt) = vs1(i1)
            i1 += 1
          } else { // (ids1(i1) > ids2(i2))
            newIds(cnt) = ids2(i2)
            newVs(cnt) = -vs2(i2)
            i2 += 1
          }
          cnt += 1
        }
        Vectors.sparse(n1, newIds.take(cnt), newVs.take(cnt))
      }
      case _ =>
        throw new IllegalArgumentException(
          s"Do not support vector type ${v1.getClass}, ${v2.getClass}"
        )
    }
  }

  def mul(v1: Vector, a: Double): Vector = {
    v1 match {
      case DenseVector(vs)          => Vectors.dense(vs.map(_ * a))
      case SparseVector(n, ids, vs) => Vectors.sparse(n, ids, vs.map(_ * a))
      case v =>
        throw new IllegalArgumentException(
          s"Do not support vector type ${v.getClass}"
        )
    }
  }

  def div(v1: Vector, a: Double): Vector = {
    v1 match {
      case DenseVector(vs)          => Vectors.dense(vs.map(_ / a))
      case SparseVector(n, ids, vs) => Vectors.sparse(n, ids, vs.map(_ / a))
      case v =>
        throw new IllegalArgumentException(
          s"Do not support vector type ${v.getClass}"
        )
    }
  }

  def neg(v1: Vector): Vector = { mul(v1, -1.0) }

  def normalize(v1: Vector, p: Double = 2): Vector = {
    val norm = Vectors.norm(v1, p)
    if (norm == 0.0) { v1 }
    else { div(v1, norm) }
  }

  def cosineSimilarity(v1: Vector, v2: Vector): Double = {
    val denom = Vectors.norm(v1, 2) * Vectors.norm(v2, 2)
    v1.dot(v2) / denom
  }
}
