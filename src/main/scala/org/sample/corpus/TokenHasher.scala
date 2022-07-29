package org.sample.corpus

import collection.JavaConverters._

import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Row}
import org.apache.spark.sql.types._
import org.apache.spark.ml.{UnaryTransformer}
import org.apache.spark.ml.util.{Identifiable}
import org.apache.spark.ml.linalg.{Vectors, VectorUDT}

/**/
class TokenHasher(override val uid: String)
    extends UnaryTransformer[Seq[String], Seq[Long], TokenHasher] {
  def this() = this(Identifiable.randomUID("TokenHasher"))

  override protected def outputDataType: DataType =
    new ArrayType(LongType, false)

  override protected def createTransformFunc: Seq[String] => Seq[Long] = {
    _.iterator.map(hashString).toSet.toSeq
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(
      inputType == ArrayType(StringType, true) ||
        inputType == ArrayType(StringType, false),
      s"Input type must be ${ArrayType(StringType).catalogString} but got " +
        inputType.catalogString
    )
  }

  def hashString(s: String): Long = {
    /* long version of scala String.hashCode */
    s.foldLeft(0L) { case (code, c) => 31 * code + c }
  }
}
