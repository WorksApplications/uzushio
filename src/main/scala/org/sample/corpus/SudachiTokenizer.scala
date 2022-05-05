package org.sample.corpus

import collection.JavaConverters._

import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}
import org.apache.spark.sql.{Row}
import org.apache.spark.sql.types.{
  ArrayType,
  StringType,
  StructType,
  StructField
}
import org.apache.spark.ml.{Transformer}
import org.apache.spark.ml.util.{Identifiable}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}

class SudachiTokenizer(override val uid: String)
    extends Transformer
    with HasInputCol
    with HasOutputCol {
  def this() = this(Identifiable.randomUID("sudachiTokenizer"))

  override def copy(extra: ParamMap) = defaultCopy(extra)

  def outputDataType = new ArrayType(StringType, true)

  def setInputCol(value: String) = set(inputCol, value)
  def setOutputCol(value: String) = set(outputCol, value)

  // sudachi split mode.
  val splitMode: Param[String] =
    new Param(
      this,
      "splitMode",
      "sudachi split mode (A/B/C)",
      (c: String) => { c.length == 1 && "aAbBcC".contains(c) }
    )
  def setSplitMode(value: String): this.type = set(splitMode, value)
  def getSplitMode: String = $(splitMode)

  setDefault(splitMode -> "C")

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(
      inputType == StringType,
      s"Input type must be ${StringType.catalogString} type but got ${inputType.catalogString}."
    )

    if (schema.fieldNames.contains($(outputCol))) {
      throw new IllegalArgumentException(
        s"Output column ${$(outputCol)} already exists."
      )
    }
    val outputFields = schema.fields :+
      StructField($(outputCol), outputDataType, nullable = false)
    StructType(outputFields)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)

    val mode = Sudachi.parseSplitMode($(splitMode))
    val tokenized = dataset.toDF.rdd
      .mapPartitions(iter => {
        val tok = Sudachi.setupSudachiTokenizer()

        iter.map(row => {
          val tokens = row
            .getAs[String]($(inputCol))
            .split("\n")
            .flatMap(sent => tok.tokenize(mode, sent).asScala.map(_.surface()))

          Row(row.toSeq :+ tokens: _*)
        })
      })

    dataset.sparkSession.createDataFrame(tokenized, outputSchema)
  }
}
