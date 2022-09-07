package org.sample.corpus.cleaning

import com.typesafe.config.ConfigObject
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.monotonically_increasing_id

/** Deduplicate elements of sequences, keeping seq order. */
class DeduplicateElement extends Transformer {
  override def transform(ds: Dataset[Seq[String]]): Dataset[Seq[String]] = {
    import ds.sparkSession.implicits._

    // add indices: (doc_id, elem_id, txt)
    val indexed = ds
      .withColumn("did", monotonically_increasing_id)
      .flatMap(r =>
        r.getSeq[String](0).zipWithIndex.map(z => (r.getLong(1), z._2, z._1))
      )
    // drop duplicate paragraphs
    val dedup = indexed.dropDuplicates("_3")
    // reconstruct documents
    dedup
      .groupByKey(_._1)
      .mapGroups((k, itr) => itr.toSeq.sortBy(_._2).map(_._3))
  }
}

object DeduplicateElement extends FromConfig {
  override def fromConfig(conf: ConfigObject): Transformer =
    new DeduplicateElement
}
