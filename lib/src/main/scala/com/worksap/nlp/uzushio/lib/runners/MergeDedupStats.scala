package com.worksap.nlp.uzushio.lib.runners

import com.worksap.nlp.uzushio.lib.utils.Resources.AutoClosableResource
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.rogach.scallop.ScallopConf

import scala.annotation.tailrec

object MergeDedupStats {

  def run(spark: SparkSession, arg: Args): Unit = {
    import spark.implicits._

    val inputData = spark.read.parquet(arg.input(): _*)

    val remaps = arg.remaps.getOrElse(5)
    val merged = mergeStatisticDatasets(spark, inputData, remaps)

    val filtered =
      if (arg.noOnes()) {
        merged.where($"nearFreq" > 1)
      } else merged

    val partitioned = filtered.repartition(arg.partitions(), $"hash")
      .sortWithinPartitions($"reprHash", $"hash")

    partitioned.write.option("compression", "zstd").mode(SaveMode.Overwrite).parquet(arg.output())
  }

  private[this] val clampLongToInt = udf((x: Long) => math.min(x, Int.MaxValue).toInt)
    .asNonNullable()

  @tailrec
  def propagateRemaps(
      spark: SparkSession,
      groups: DataFrame,
      updates: DataFrame,
      iter: Int,
      maxIters: Int
  ): DataFrame = {
    import spark.implicits._
    val merged = groups.join(updates, groups.col("newReprHash") === updates.col("srcHash"), "left")

    val newMapping = merged.select(
      $"initReprHash",
      coalesce($"tgtHash", $"newReprHash").as("newReprHash"),
    ).groupBy("initReprHash").agg(
      min("newReprHash").as("newReprHash")
    ).where($"initReprHash" =!= $"newReprHash").persist()

    if (iter >= maxIters) {
      newMapping.select(
        $"initReprHash".as("srcReprHash"),
        $"newReprHash".as("tgtReprHash")
      )
    } else {
      propagateRemaps(spark, newMapping, updates, iter + 1, maxIters)
    }
  }

  def mergeStatisticDatasets(spark: SparkSession, df: DataFrame, maxRemaps: Int = 10): DataFrame = {
    import spark.implicits._

    // Step 1: Compute new frequencies that arise via merging
    // Entries with the same hash are summed up
    val aggregated = df.groupBy("hash").agg(
      min("reprHash").as("newReprHash"),
      clampLongToInt(sum($"exactFreq".cast(LongType))).as("newExactFreq"),
      clampLongToInt(sum($"nearFreq".cast(LongType))).as("newNearFreq"),
    ).persist()

    // newReprHashes are incorrect at this point because transitive set merging is not applied yet

    // Step 2: Compute mapping for transitive set merging
    // We consider entries to contain updates if the "reprHash" value was changed during the first aggregation
    val seedPreUpdates = df.join(aggregated, "hash").where($"reprHash" =!= $"newReprHash").select(
      $"reprHash".as("srcHash"),
      $"newReprHash".as("tgtHash")
    ).groupBy("srcHash").agg(
      collect_set($"tgtHash").as("tgtAll"),
      min($"tgtHash").as("tgtMin")
    )

    // Step 2a: collect updates (e.g. we have hash 8 with reprs 1, 3, 8)
    // first part collects the direct updates (e.g. 8 -> 1)
    val seedUpdates = seedPreUpdates.select($"srcHash", $"tgtMin".as("tgtHash")).union(
      // second part contains the internal updates
      // second part will add 3 -> 1
      seedPreUpdates.select(
        explode($"tgtAll").as("srcHash"),
        $"tgtMin".as("tgtHash")
      )
    ).where($"srcHash" =!= $"tgtHash").distinct().persist()

    // Step 2b: collect all repr hash candidates to consider for updating
    // find all repr hashes which have distinct hashes
    val seedGroupsA = df.groupBy("reprHash").count().where($"count" > 1).select(
      $"reprHash".as("initReprHash")
    )

    // find all reprs if hash has several of them in the merged dataset
    val seedGroupsB = {
      val nonUnique = df.groupBy("hash")
        .agg(countDistinct("reprHash").as("count"))
        .where($"count" > 1)

      df.join(nonUnique, "hash")
        .select($"reprHash".as("initReprHash"))
    }

    val seedGroups = seedGroupsA.union(seedGroupsB).distinct().select(
      $"initReprHash",
      $"initReprHash".as("newReprHash")
    ).persist()

    // compute the correct remaps themselves iteratively
    // this will have false positives, but hopefully not much
    // most merges should be correct after 5 iterations in non-degenerate cases
    val remaps = propagateRemaps(spark, seedGroups, seedUpdates, 0, maxRemaps)

    // finally apply remaps
    val remappedHashes = aggregated.join(remaps, $"newReprHash" === $"srcReprHash", "left").select(
      $"hash",
      coalesce($"tgtReprHash", $"newReprHash").as("reprHash"),
      $"newExactFreq".as("exactFreq"),
      $"newNearFreq".as("nearFreq")
    ).persist()

    // Step 3: Correct nearFreq as max(nearFreq) inside the newly computed groups
    val nearFreqCorrection = remappedHashes.groupBy("reprHash").agg(
      max("nearFreq").as("nearFreq")
    )

    remappedHashes.select(
      $"hash",
      $"reprHash",
      $"exactFreq"
    ).join(nearFreqCorrection, "reprHash")
  }

  // noinspection TypeAnnotation
  class Args(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[String]]()
    val output = opt[String]()
    val master = opt[String]()
    val partitions = opt[Int](default = Some(500))
    val noOnes = toggle(default = Some(false))
    val remaps = opt[Int](default = Some(5))
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
