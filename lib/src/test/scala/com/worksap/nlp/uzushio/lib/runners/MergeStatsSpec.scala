package com.worksap.nlp.uzushio.lib.runners

import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec

case class MergeItem(hash: Long, reprHash: Long, exactFreq: Int = 1, nearFreq: Int = 1)

class MergeStatsSpec extends AnyFreeSpec with BeforeAndAfterAll {

  private lazy val session = SparkSession.builder().master("local[1]")
    .config("spark.sql.shuffle.partitions", "1").getOrCreate()
  import session.implicits._

  private def df[T <: Product: Encoder](vals: T*): DataFrame = {
    session.createDataset(vals).toDF()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    assert(session != null)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    session.close()
  }

  "can merge two datasets without remapping reprHashes" in {
    val dataset1 = df(
      MergeItem(1, 1, 1, 2),
      MergeItem(2, 1, 1, 2),
      MergeItem(3, 3),
    )
    val dataset2 = df(
      MergeItem(1, 1, 1, 2),
      MergeItem(4, 1, 1, 2),
      MergeItem(3, 3, 1, 2),
      MergeItem(5, 3, 1, 2)
    )

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

  "two datasets with a bridge" in {
    val dataset1 = session.createDataset(
      Seq(
        MergeItem(3, 3, 1, 4),
        MergeItem(4, 3, 1, 4),
        MergeItem(5, 5, 1, 4),
        MergeItem(6, 5, 1, 4),
      )
    ).toDF()
    val dataset2 = session.createDataset(
      Seq(
        MergeItem(4, 4, 1, 3),
        MergeItem(5, 4, 1, 3),
        MergeItem(6, 4, 1, 3),
      )
    ).toDF()

    val merged = MergeDedupStats.mergeStatisticDatasets(session, dataset1.union(dataset2))
      .orderBy($"hash".asc, $"reprHash".asc).as[MergeItem].take(100)

    val result = Seq(
      MergeItem(3, 3, 1, 7),
      MergeItem(4, 3, 2, 7),
      MergeItem(5, 3, 2, 7),
      MergeItem(6, 3, 2, 7),
    )

    assert(merged === result)
  }

  "propagates nearHash from other dataset" in {
    val dataset1 = session.createDataset(
      Seq(
        MergeItem(7, 1, 1, 2),
        MergeItem(8, 2, 1, 2),
        MergeItem(7, 5, 1, 3),
        MergeItem(8, 5, 1, 3),
        MergeItem(7, 7, 1, 4),
        MergeItem(8, 8, 1, 4),
      )
    ).toDF()
    val dataset2 = session.createDataset(
      Seq(
        MergeItem(1, 1, 1, 4),
        MergeItem(2, 2, 1, 4),
        MergeItem(5, 5, 1, 4),
        MergeItem(6, 6, 1, 1),
        MergeItem(9, 6, 3, 3),
      )
    ).toDF()

    val merged = MergeDedupStats.mergeStatisticDatasets(session, dataset1.union(dataset2).coalesce(1))
      .orderBy($"hash".asc, $"reprHash".asc).as[MergeItem].take(100)

    val result = Seq(
      MergeItem(1, 1, 1, 9),
      MergeItem(2, 1, 1, 9),
      MergeItem(5, 1, 1, 9),
      MergeItem(6, 6, 1, 3),
      MergeItem(7, 1, 3, 9),
      MergeItem(8, 1, 3, 9),
      MergeItem(9, 6, 3, 3),
    )

    assert(merged === result)
  }

  // current implementation fails this spec and it is unimplementable
  // need to focus on correctness of mapping with merging counts as a best effort
  "can merge three datasets with remapping reprHashes" in {
    val ds1 = df(
      MergeItem(1, 1, 1, 2),
      MergeItem(2, 1, 1, 2),
    )

    val ds2 = df(
      MergeItem(2, 2, 1, 2),
      MergeItem(3, 2, 1, 2),
    )

    val ds3 = df(
      MergeItem(3, 3, 1, 2),
      MergeItem(4, 3, 1, 2),
    )

    val merged = MergeDedupStats.mergeStatisticDatasets(
      spark = session,
      df = ds1.union(ds2).union(ds3).coalesce(1),
      maxRemaps = 2
    ).orderBy($"hash".asc, $"reprHash".asc).as[MergeItem].take(100)

    val result = Seq(
      MergeItem(1, 1, 1, 4),
      MergeItem(2, 1, 2, 4),
      MergeItem(3, 1, 2, 4),
      MergeItem(4, 1, 1, 4),
    )

    assert(merged === result)
  }
}
