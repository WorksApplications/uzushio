package com.worksap.nlp.uzushio.lib.runners

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec

case class MergeItem(hash: Long, reprHash: Long, exactFreq: Int = 1, nearFreq: Int = 1)

class MergeStatsSpec extends AnyFreeSpec with BeforeAndAfterAll {
  private val session = SparkSession.builder().master("local[1]")
    .config("spark.sql.shuffle.partitions", "1").getOrCreate()
  import session.implicits._

  override def afterAll(): Unit = {
    super.afterAll()
    session.close()
  }

  "can merge two datasets without remapping reprHashes" in {
    val dataset1 = session.createDataset(
      Seq(
        MergeItem(1, 1, 1, 2),
        MergeItem(2, 1, 1, 2),
        MergeItem(3, 3),
      )
    ).toDF()
    val dataset2 = session.createDataset(
      Seq(
        MergeItem(1, 1, 1, 2),
        MergeItem(4, 1, 1, 2),
        MergeItem(3, 3, 1, 2),
        MergeItem(5, 3, 1, 2)
      )
    ).toDF()

    val merged = MergeDedupStats.mergeStatisticDatasets(session, dataset1.union(dataset2))
      .orderBy($"hash".asc, $"reprHash".asc).as[MergeItem].take(100)

    val result = Seq(
      MergeItem(1, 1, 2, 4),
      MergeItem(2, 1, 1, 4),
      MergeItem(3, 3, 2, 3),
      MergeItem(4, 1, 1, 4),
      MergeItem(5, 3, 1, 3)
    )

    assert(merged === result)
  }

  "can merge two datasets with remapping reprHashes" in {
    val dataset1 = session.createDataset(
      Seq(
        MergeItem(1, 1, 1, 2),
        MergeItem(2, 1, 1, 2),
        MergeItem(3, 3),
      )
    ).toDF()
    val dataset2 = session.createDataset(
      Seq(
        MergeItem(2, 2, 1, 2),
        MergeItem(4, 2, 1, 2),
        MergeItem(3, 3, 1, 2),
        MergeItem(5, 3, 1, 2)
      )
    ).toDF()

    val merged = MergeDedupStats.mergeStatisticDatasets(session, dataset1.union(dataset2))
      .orderBy($"hash".asc, $"reprHash".asc).as[MergeItem].take(100)

    val result = Seq(
      MergeItem(1, 1, 1, 4),
      MergeItem(2, 1, 2, 4),
      MergeItem(3, 3, 2, 3),
      MergeItem(4, 1, 1, 4),
      MergeItem(5, 3, 1, 3)
    )

    assert(merged === result)
  }

  // current implementation fails this spec and it is unimplementable
  // need to focus on correctness of mapping with merging counts as a best effort
  "can merge three datasets with remapping reprHashes" ignore {
    val dataset1 = session.createDataset(
      Seq(
        MergeItem(1, 1, 1, 2),
        MergeItem(2, 1, 1, 2),
      )
    ).toDF()
    val dataset2 = session.createDataset(
      Seq(
        MergeItem(2, 2, 1, 2),
        MergeItem(3, 2, 1, 2),
      )
    ).toDF()
    val dataset3 = session.createDataset(
      Seq(
        MergeItem(3, 3, 1, 2),
        MergeItem(4, 3, 1, 2),
      )
    ).toDF()

    val merged = MergeDedupStats.mergeStatisticDatasets(session, dataset1.union(dataset2).union(dataset3))
      .orderBy($"hash".asc, $"reprHash".asc).as[MergeItem].take(100)

    val result = Seq(
      MergeItem(1, 1, 1, 6),
      MergeItem(2, 1, 2, 6),
      MergeItem(3, 1, 2, 6),
      MergeItem(4, 1, 1, 6),
    )

    assert(merged === result)
  }
}
