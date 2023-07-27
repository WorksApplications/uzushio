package com.worksap.nlp.uzushio.lib.dupes

import com.worksap.nlp.uzushio.lib.runners.{CandidateRowProcessor, DuplicateCandidateRow}
import com.worksap.nlp.uzushio.lib.stats.{NgramHashExtractor, SimHashProcessor}
import org.apache.spark.sql.catalyst.expressions.XXH64
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.freespec.AnyFreeSpec

object RowCandidate {
  private val ngram = new NgramHashExtractor(2, 4)
  private val simhasher = new SimHashProcessor(128)

  def apply(x: String): DuplicateCandidateRow = {
    val utf8Str = UTF8String.fromString(x)
    val hash = XXH64.hashUTF8String(utf8Str, 42L)
    val simhashState = simhasher.init
    simhasher.update(simhashState, x, ngram)
    DuplicateCandidateRow(
      x,
      simhasher.result(simhashState),
      1,
      hash,
      hash
    )
  }
}

class CandidateRowProcessorSpec extends AnyFreeSpec {
  "stuff (1) is processed correctly" in {
    val pars = Seq(
      RowCandidate("docomo STYLE series N-01C"),
      RowCandidate("docomo STYLE series SH-03E"),
      RowCandidate("4位docomo STYLE series N-01E"),
      RowCandidate("5位docomo STYLE series N-03D"),
    )
    val proc = new CandidateRowProcessor(1024 * 1024, 70, pars.iterator)
    val result = proc.toArray
    assert(result.length == 4)
    assert(result.map(_.reprHash).toSet.size == 1)
  }

  "stuff (2) is processed correctly" in {
    val pars = Seq(
      RowCandidate("らくらくホン ベーシック3 [ゴールド]"),
      RowCandidate("らくらくホン ベーシック3 [ネイビー]"),
      RowCandidate("らくらくホン ベーシック3 [ピンク]"),
      RowCandidate("> らくらくホン ベーシック3 [ホワイト]"),
      RowCandidate("らくらくホン ベーシック3 [ホワイト]"),
      RowCandidate("らくらくホン ベーシック3 [ホワイト] のクチコミ掲示板"),
    )
    val proc = new CandidateRowProcessor(1024 * 1024, 70, pars.iterator)
    val result = proc.toArray
    assert(result.length == 6)
    assert(result.map(_.reprHash).toSet.size == 2)
  }
}
