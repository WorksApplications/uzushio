package com.worksap.nlp.uzushio.lib.runners

import com.worksap.nlp.uzushio.lib.utils.Resources.AutoClosableResource
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.rogach.scallop.ScallopConf

object MergeDedupStats {

  def run(spark: SparkSession, arg: Args): Unit = {
    import spark.implicits._

    val inputData = spark.read.parquet(arg.input(): _*)

    val clampLongToInt = udf((x: Long) => math.min(x, Int.MaxValue).toInt).asNonNullable()

    val combined = inputData.groupBy("hash").agg(
      clampLongToInt(sum($"exactFreq".cast(LongType))).as("exactFreq"),
      clampLongToInt(sum($"nearFreq".cast(LongType))).as("nearFreq"),
      collect_list($"reprHash").as("reprHashes")
    ).persist()

    val notUnique = combined.where(size($"reprHashes") > 1)

    val remapReprHashes = notUnique.select("reprHashes").select(
      array_min($"reprHashes").as("newReprHash"),
      explode($"reprHashes").as("oldReprHash")
    ).where($"newReprHash" =!= $"oldReprHash").distinct()

    val intermediate = combined.select(
      $"hash",
      $"exactFreq",
      $"nearFreq",
      array_min($"reprHashes").as("reprHash")
    ).persist()

    val correctHashes = intermediate
      .join(remapReprHashes, $"reprHash" === $"oldReprHash", "left").select(
        $"hash",
        when($"oldReprHash".isNotNull, $"newReprHash").otherwise($"reprHash").as("reprHash"),
        $"exactFreq",
        $"nearFreq"
      ).persist()

    val correctFreqs = correctHashes.groupBy("reprHash").agg(
      max($"nearFreq").as("nearFreq")
    )

    val merged = correctHashes.select("hash", "reprHash", "exactFreq")
      .join(correctFreqs, "reprHash")

    val filtered =
      if (arg.noOnes()) {
        merged
      } else merged.where($"nearFreq" > 1)

    filtered.write.option("compression", "zstd").mode(SaveMode.Overwrite).parquet(arg.output())
  }

  class Args(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[String]]()
    val output = opt[String]()
    val master = opt[String]()
    val noOnes = toggle(default = Some(false))
    verify()
  }

  def main(args: Array[String]): Unit = {
    val arg = new Args(args)
    val bldr = SparkSession.builder()
    arg.master.toOption.foreach(bldr.master)
    bldr.getOrCreate().use { spark =>
      run(spark, arg)
    }
  }

}
