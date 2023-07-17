package com.worksap.nlp.uzushio.lib.runners

import com.worksap.nlp.uzushio.lib.runners.DuplicateCandidateRow.{BIT_MASK, BYTE_MASK, NGRAM_SIG_LEN, TEXT_NGRAM_MATCHING_THRESHOLD}
import com.worksap.nlp.uzushio.lib.stats.{NgramHashExtractor, SimHashProcessor}
import com.worksap.nlp.uzushio.lib.utils.MathUtil
import com.worksap.nlp.uzushio.lib.utils.Resources.AutoClosableResource
import it.unimi.dsi.fastutil.ints.IntArrays
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.rogach.scallop.ScallopConf
import spire.std.LevenshteinDistance

import java.util
import java.util.Random

case class RowResult(
    text: String,
    signature: Array[Byte],
    freq: Long,
    hash: Long,
    reprHash: Long
)

case class DuplicateCandidateRow(
    text: String,
    signature: Array[Byte],
    freq: Long,
    hash: Long,
    var reprHash: Long
) {

  val sign1: Long = MathUtil.longFromBytes(signature, 0)
  val sign2: Long = MathUtil.longFromBytes(signature, 8)

  // estimate object size
  def sizeInBytes: Int = {
    12 + // header
      12 + signature.length + // suppose that arrays also have 12-byte header
      36 + text.length * 2 + // String fields, header + string content header + content data
      8 * 6 + // fields
      (if (text.length < TEXT_NGRAM_MATCHING_THRESHOLD) 0 else NGRAM_SIG_LEN * 8) // ngram signature bitmap
  }

  def matchingSignatureBits(other: DuplicateCandidateRow): Int = {
    val c1 = MathUtil.matchingBits(sign1, other.sign1)
    val c2 = MathUtil.matchingBits(sign2, other.sign2)
    c1 + c2
  }

  lazy val ngramBitset: Array[Long] = {
    val bitset = new Array[Long](NGRAM_SIG_LEN)
    DuplicateCandidateRow.ngrams.compute(text) { hcode =>
      val idx = ((hcode >>> 32) ^ hcode).toInt
      val bitIdx = idx & BIT_MASK
      val byteIdx = (idx & BYTE_MASK) >>> 6
      bitset(byteIdx) = bitset(byteIdx) | (1L << bitIdx)
    }
    bitset
  }

  // computes Jaccard coefficient on ngram bit signature
  def matchingNgramSignatureBits(other: DuplicateCandidateRow): Float = {
    val s1 = ngramBitset
    val s2 = other.ngramBitset
    require(s1.length == NGRAM_SIG_LEN)
    require(s2.length == NGRAM_SIG_LEN)
    var i = 0
    var intersectionBits = 0
    var unionBits = 0
    while (i < NGRAM_SIG_LEN) {
      val l1 = s1(i)
      val l2 = s2(i)
      intersectionBits += java.lang.Long.bitCount(l1 & l2)
      unionBits += java.lang.Long.bitCount(l1 | l2)
      i += 1
    }
    intersectionBits.toFloat / unionBits.toFloat
  }

  def toRow: RowResult = RowResult(text, signature, freq, hash, reprHash)
}

object DuplicateCandidateRow {
  private val ngrams = new NgramHashExtractor(3, 4)
  final val NGRAM_SIG_LEN = 512
  final val BITS_IN_LONG = 64
  final val BIT_MASK = BITS_IN_LONG - 1
  final val BYTE_MASK = (NGRAM_SIG_LEN * BITS_IN_LONG - 1) ^ BIT_MASK
  final val MAX_BITS = NGRAM_SIG_LEN * BITS_IN_LONG
  final val TEXT_NGRAM_MATCHING_THRESHOLD = 50
}

class CandidateRowProcessor(
    limit: Int,
    matchThreshold: Int,
    iter: Iterator[DuplicateCandidateRow]
) extends Iterator[RowResult] {

  private val queue = new util.ArrayDeque[DuplicateCandidateRow]()
  private var currentBytes = 0

  private def fixHashesInBuffer(oldHash: Long, newHash: Long): Unit = {
    val iter = queue.iterator()
    while (iter.hasNext) {
      val o = iter.next()
      if (o.reprHash == oldHash) {
        o.reprHash = newHash
      }
    }
  }

  // keep queue size small by not growing it when the prefixes of the signature are distant enough
  private def prefixesAreSimilar(): Boolean = {
    val q = queue
    if (q.size() < 2) return true
    val first = q.peekFirst()
    val last = q.peekLast()
    val dist = first.sign1.toByte - last.sign1.toByte
    // can wrap around, absolute value is OK for our use case
    dist.abs < 4
  }

  private def textOverlaps(
      r1: DuplicateCandidateRow,
      r2: DuplicateCandidateRow
  ): Boolean = {
    val t1 = r1.text
    val t2 = r2.text

    // sequences are distinct enough in length
    if ((t1.length - t2.length).abs > 50) {
      return false
    }

    val avgLen = (t1.length + t2.length) / 2
    if (avgLen > TEXT_NGRAM_MATCHING_THRESHOLD) { // use approximate ngram matching for longer texts
      val ngramSigRatio = r1.matchingNgramSignatureBits(r2)
      return ngramSigRatio >= 0.7f
    }

    // short texts are compared using levenshtein distance (most paragraphs are short)
    val dist = LevenshteinDistance.distance(r1.text, r2.text).toDouble
    val len = r1.text.length.min(r2.text.length).toDouble
    (dist / len ) < 0.3
  }

  private def checkReprHashes(row: DuplicateCandidateRow): Unit = {
    val iter = queue.iterator()
    while (iter.hasNext) {
      val o = iter.next()
      val nbits = row.matchingSignatureBits(o)
      if (nbits >= matchThreshold && textOverlaps(row, o)) {
        val reprHash = java.lang.Long.min(row.reprHash, o.reprHash)
        if (o.reprHash != reprHash) {
          fixHashesInBuffer(o.reprHash, reprHash)
        } else if (row.reprHash != reprHash) {
          row.reprHash = reprHash
        }
      }
    }

  }

  private def fillCandidateBuffer(): Unit = {
    while (currentBytes < limit && prefixesAreSimilar() && iter.hasNext) {
      currentBytes += consumeRow(iter.next()).sizeInBytes
    }
  }

  private def consumeRow(obj: DuplicateCandidateRow): DuplicateCandidateRow = {
    checkReprHashes(obj)
    queue.add(obj)
    obj
  }

  override def hasNext: Boolean = queue.size() > 0 || iter.hasNext

  override def next(): RowResult = {
    fillCandidateBuffer()
    val obj = queue.poll()
    currentBytes -= obj.sizeInBytes
    obj.toRow
  }
}

case class FilteredDoc()
class DocsFilter(args: DeduplicateParagraphs.Args) extends (Row => List[FilteredDoc]) with Serializable {
  override def apply(row: Row): List[FilteredDoc] = {
    Nil
  }
}

object DeduplicateParagraphs {
  def process(args: Args, spark: SparkSession): Unit = {
    import spark.implicits._

    val rawData = spark.read.parquet(args.inputs: _*)

    // posexplode must be in different select operation than split
    val splitDocs = rawData
      .select(split($"text", "\n\n").as("text"))
      .select(posexplode($"text").as(Seq("pos", "text")))

    val basicData = prepareData(splitDocs, args)

    val propagated = args.shiftIndices.foldLeft(basicData) { (bd, i) =>
      propagateReprHashes(bd, i, args)
    }

    val cols = propagated.select("hash", "reprHash", "freq").checkpoint()

    val totalReprHashes = cols.groupBy("reprHash").agg(
      sum("freq").as("freq"),
    )

    val counts = cols.select("hash", "reprHash")
      .join(totalReprHashes, "reprHash")
      .select("hash", "freq")
      .dropDuplicates("hash")

    // this time keep original columns intact
    val cookedDocs = rawData
      .withColumn("text", split($"text", "\n\n"))
      .withColumn("text", posexplode($"text").as(Seq("pos", "text")))
      .withColumn("hash", longHash($"text"))

    // join paragraph frequencies
    val paragraphsWithFreq = cookedDocs.join(counts, "hash")

    val filteredDocs = filterDuplicateDocs(paragraphsWithFreq, args)

    val sorted = counts.coalesce(1).sort($"freq".desc)

    sorted.write.mode(SaveMode.Overwrite).json(args.output)
  }

  // compile full documents from paragraphs
  // paragraphs are shuffled because of join with freqs,
  // groupBy op merges them back together, and we use an udf to perform the actual filtering
  def filterDuplicateDocs(ds: DataFrame, args: Args): DataFrame = {
    import ds.sqlContext.implicits._
    val docParts = Seq("text", "pos", "freq")

    val allColumns = ds.columns

    val passthroughColumns = allColumns
      .toSeq
      .filterNot(_ == "docId")
      .filterNot(docParts.contains(_))
    val aggQueryBasicColumns = passthroughColumns
      .map(colName => first(colName).as(colName))
    val aggColumns = docParts.map(collect_list)

    val aggOpFirst :: aggOpRest = (aggColumns ++ aggQueryBasicColumns).toList

    val aggOpResult = ds.groupBy("docId").agg(aggOpFirst, aggOpRest: _*)

    val convertUdf = udf((text: Array[String], pos: Array[Int], freq: Array[Long]) => {
      val parts = (pos, text, freq).zipped
      val sorted = parts.toBuffer.sortBy(_._1)
      processDocumentParts(args, sorted)
    })

    val transformCols = Seq(
      $"docId",
      convertUdf(docParts.map(column) : _*).as("text")
    ) ++ passthroughColumns.map(column)

    aggOpResult.select(
       transformCols: _*
    ).filter($"text".isNotNull)
  }

  private val longHash = udf((s: String) => NgramHashExtractor.hashString(s))

  def prepareData(ds: DataFrame, args: Args): DataFrame = {
    import ds.sqlContext.implicits._
    val simHasher = new SimHashProcessor(args.simHashSize)
    val ngrams = new NgramHashExtractor(args.minNgramSize, args.maxNgramSize)

    val simHash = udf((s: String) => {
      val x = simHasher.init
      simHasher.update(x, s, ngrams)
      simHasher.result(x)
    })

    val basicData = ds
      .groupBy("text")
      .agg(
        count("pos").name("freq")
      )
      .withColumns(
        Map(
          "signature" -> simHash($"text"),
          "hash" -> longHash($"text")
        )
      )
      .withColumn("reprHash", $"hash")

    basicData
  }

  // propagate repr hashes between documents (paragraphs) which are similar
  def propagateReprHashes(ds: DataFrame, shift: Int, args: Args): DataFrame = {
    import ds.sqlContext.implicits._
    val shiftSignature =
      udf((x: Array[Byte]) => MathUtil.rotateBitsRight(x, shift))

    ds.withColumn("signature", shiftSignature(ds.col("signature")))
      .sort($"signature".asc)
      .as[DuplicateCandidateRow]
      .mapPartitions(iter =>
        new CandidateRowProcessor(
          args.bufferSizeInBytes,
          args.minBitsToMatch,
          iter
        )
      )
      .toDF()
  }

  private def processDocumentParts(args: Args, indices: Seq[(Int, String, Long)]): String = {
    ???
  }

  // noinspection TypeAnnotation
  class ArgParser(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[String]]()
    val output = opt[String]()
    val numShifts = opt[Int](default = Some(-1))
    val checkpoints = opt[String]()
    verify()

    def toArgs: Args = Args(
      inputs = input(),
      output = output(),
      checkpoints = checkpoints(),
      partitions = 1000,
      simHashSize = 128,
      minNgramSize = 2,
      maxNgramSize = 4,
      numShifts = numShifts()
    )
  }

  case class Args(
      inputs: Seq[String],
      output: String,
      checkpoints: String,
      partitions: Int,
      simHashSize: Int,
      minNgramSize: Int,
      maxNgramSize: Int,
      numShifts: Int = -1,
      bufferSizeInBytes: Int = 10000000,
      preFilterRatio: Double = 0.6
  ) {
    def minBitsToMatch: Int = (simHashSize * preFilterRatio).toInt

    val shiftIndices: Array[Int] = {
      val base = Array.range(0, simHashSize)
      IntArrays.shuffle(base, new Random(0xfeedbeefL))
      val nshifts = if (numShifts <= 0) simHashSize else numShifts
      val arraySlice = base.take(nshifts).sorted
      // encode values with delta encoding, so shifts could be chained
      var start = 0
      var i = 0
      while (i < nshifts) {
        val x = arraySlice(i)
        arraySlice(i) = x - start
        start = x
        i += 1
      }
      arraySlice
    }
  }

  def main(args: Array[String]): Unit = {
    val argParser = new ArgParser(args)
    val argObj = argParser.toArgs

    SparkSession.builder().master("local[*]").getOrCreate().use { spark =>
      spark.sparkContext.setCheckpointDir("e:/data/nlp/corpora/cc/checkpoints")
      process(argObj, spark)
    }
  }

}
