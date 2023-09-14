package com.worksap.nlp.uzushio

import scala.util.Random

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol, HasSeed}
import org.apache.spark.ml.param.{IntParam, Params, ParamMap, ParamValidators}
import org.apache.spark.ml.util.Identifiable

/* Reimplementation of spark.ml.feature.MinHashLSH, but in/out types are Array[Long].
 *
 * Has r buckets of hashes, each contains b hashes.
 */

trait MinHashParams extends Params with HasInputCol with HasOutputCol {
  val b: IntParam = new IntParam(this, "b", "num and-amplification", ParamValidators.gt(0))
  def getB: Int = $(b)

  val r: IntParam = new IntParam(this, "r", "num or-amplification", ParamValidators.gt(0))
  def getR: Int = $(r)

  setDefault(b -> 1, r -> 1)

  final protected[this] def validateAndTransformSchema(
      schema: StructType
  ): StructType = {
    val typeCandidates = Set(new ArrayType(LongType, true), new ArrayType(LongType, false))
    val inputType = schema($(inputCol)).dataType
    require(
      typeCandidates.exists(inputType.equals),
      s"Column ${$(inputCol)} must be of type equal to one of the following types: " +
        s"${typeCandidates.map(_.catalogString).mkString("[", ", ", "]")} but was actually of type " +
        s"${inputType.catalogString}"
    )
    require(
      !schema.fieldNames.contains($(outputCol)),
      s"Column ${$(outputCol)} already exists."
    )
    val outputType = new ArrayType(LongType, false)
    val outputField = StructField($(outputCol), outputType, nullable = false)
    StructType(schema.fields :+ outputField)
  }
}

class MinHash(override val uid: String)
    extends Estimator[MinHashModel]
    with MinHashParams
    with HasSeed {
  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)
  def setB(value: Int): this.type = set(b, value)
  def setR(value: Int): this.type = set(r, value)
  def setSeed(value: Long): this.type = set(seed, value)

  def this() = this(Identifiable.randomUID("minhash"))

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def fit(dataset: Dataset[_]): MinHashModel = {
    transformSchema(dataset.schema, logging = true)
    val rand = new Random($(seed))
    val randCoefs: Array[(Int, Int)] = Array.fill($(b) * $(r)) {
      (
        1 + rand.nextInt(MinHash.HASH_PRIME - 1),
        rand.nextInt(MinHash.HASH_PRIME - 1)
      )
    }

    val model = new MinHashModel(uid, randCoefs).setParent(this)
    copyValues(model)
  }
}

object MinHash {
  // A large prime smaller than sqrt(2^63 − 1)
  val HASH_PRIME = 2038074743
}

class MinHashModel(
    override val uid: String,
    randCoefficients: Array[(Int, Int)]
) extends Model[MinHashModel]
    with MinHashParams {

  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def copy(extra: ParamMap): MinHashModel = {
    val copied = new MinHashModel(uid, randCoefficients).setParent(parent)
    copyValues(copied, extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema)
    val transformUDF = udf(
      hashFunction(0, randCoefficients.length, _: Seq[Long])
    )
    dataset.withColumn($(outputCol), transformUDF(dataset($(inputCol))))
  }

  def transformBucket(bucketIdx: Int, dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema)
    val transformUDF = udf(
      hashFunction(bucketIdx * $(b), (bucketIdx + 1) * $(b), _: Seq[Long])
    )
    dataset.withColumn($(outputCol), transformUDF(dataset($(inputCol))))
  }

  def hashFunction(from: Int, until: Int, elems: Seq[Long]): Array[Long] = {
    require(elems.length > 0, "Must have at least 1 non zero entry.")

    randCoefficients.slice(from, until).map { case (a, b) =>
      elems.map(v => ((1L + v) * a.toLong + b.toLong) % MinHash.HASH_PRIME).min
    }
  }
}
