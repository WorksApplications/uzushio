package com.worksap.nlp.uzushio.lib.runners

import com.worksap.nlp.uzushio.lib.cleaning.{Document, Paragraph, Pipeline}
import com.worksap.nlp.uzushio.lib.runners.DuplicateCandidateRow._
import com.worksap.nlp.uzushio.lib.stats.{
  NgramBitSignatures,
  NgramHashExtractor,
  SimHashProcessor
}
import com.worksap.nlp.uzushio.lib.utils.Resources.AutoClosableResource
import com.worksap.nlp.uzushio.lib.utils.{MathUtil, Paragraphs, RowBuffer}
import it.unimi.dsi.fastutil.ints.{Int2ObjectOpenHashMap, IntArrays}
import org.apache.commons.codec.binary.Hex
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.rogach.scallop.ScallopConf
import spire.std.LevenshteinDistance

import java.util
import java.util.{Comparator, Random}

case class RowResult(
    text: String,
    signature: Array[Byte],
    freq: Long,
    hash: Long,
    reprHash: Long
)

//noinspection jol
final case class DuplicateCandidateRow(
    text: String,
    signature: Array[Byte],
    freq: Long,
    hash: Long,
    var reprHash: Long
) {
  val sign1: Long = MathUtil.longFromBytes(signature, 0)
  val sign2: Long = MathUtil.longFromBytes(signature, 8)

  private var collection: RowBuffer[DuplicateCandidateRow] = _
  private var indexInCollection: Int = -1

  def registerInBuffer(
      buffer: RowBuffer[DuplicateCandidateRow],
      index: Int
  ): Unit = {
    collection = buffer
    indexInCollection = index
  }

  def removeItselfFromBuffer(): Unit = {
    val newItem = collection.removeElementAt(indexInCollection)
    newItem.indexInCollection = indexInCollection
  }

  def registerInsteadOf(other: DuplicateCandidateRow): Unit =
    registerInBuffer(other.collection, other.indexInCollection)

  def wasIn(group: RowBuffer[DuplicateCandidateRow]): Boolean =
    collection eq group

  // estimate object size
  // this some fields are lazy and can not be instantiated at all, but we compute their sizes
  // based on heuristics
  def sizeInBytes: Int = {
    val textLen = text.length
    var objectSize =
      HEADER_SIZE + // header
        HEADER_SIZE + signature.length + // suppose that arrays also have 12-byte header
        36 + textLen * 2 + // String fields, header + string content header + content data
        8 * 8 // fields

    if (textLen >= TEXT_NGRAM_MATCHING_THRESHOLD) {
      objectSize += (HEADER_SIZE + NGRAM_SIG_LEN * 8)
    }

    if (textLen <= 40) {
      objectSize += HEADER_SIZE // array header
      objectSize += textLen match {
        case _ if textLen <= 16 =>
          NgramBitSignatures.UnigramUpTo16Chars.SIG_LEN * 8 // unigrams only
        case _ =>
          (NgramBitSignatures.UnigramBigramMoreThan16Chars.SIG_LEN1 + NgramBitSignatures.UnigramBigramMoreThan16Chars.SIG_LEN2) * 8
      }
    }

    objectSize
  }

  def matchingSignatureBits(other: DuplicateCandidateRow): Int = {
    val c1 = MathUtil.matchingBits(sign1, other.sign1)
    val c2 = MathUtil.matchingBits(sign2, other.sign2)
    c1 + c2
  }

  private var shortNgramBitsetField: Array[Long] = _
  def shortNgramBitset: Array[Long] = {
    var current = shortNgramBitsetField
    if (current == null) {
      current = NgramBitSignatures.computeShortSignature(text)
      shortNgramBitsetField = current
    }
    current
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

  def allowedLength(itemLength: Int): Boolean = {
    val myLength = text.length
    math.abs(itemLength - myLength) <= MAX_MATCHING_LENGTH
  }

  override def toString: String = {
    s"[${Hex.encodeHexString(signature)}, $freq, ${hash.toHexString}, ${reprHash.toHexString}, $text]"
  }
}

object DuplicateCandidateRow {
  private val ngrams = new NgramHashExtractor(3, 4)
  final val NGRAM_SIG_LEN = 128
  final val BITS_IN_LONG = 64
  final val BIT_MASK = BITS_IN_LONG - 1
  final val BYTE_MASK = (NGRAM_SIG_LEN * BITS_IN_LONG - 1) ^ BIT_MASK
  final val MAX_BITS = NGRAM_SIG_LEN * BITS_IN_LONG
  final val TEXT_NGRAM_MATCHING_THRESHOLD = 30

  /** size of JVM object/array header */
  final val HEADER_SIZE = 16
  final val MAX_MATCHING_LENGTH = 50
}

class CandidateRowProcessor(
    limit: Int,
    matchThreshold: Int,
    iter: Iterator[DuplicateCandidateRow]
) extends Iterator[RowResult] {

  private val queue = new util.ArrayDeque[DuplicateCandidateRow]()
  private val lengthBuckets =
    new Int2ObjectOpenHashMap[RowBuffer[DuplicateCandidateRow]]()
  private val groups =
    new RowBuffer[RowBuffer[DuplicateCandidateRow]]()
  private var currentBytes = 0

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

    val avgLen = (t1.length + t2.length) / 2
    if (avgLen > TEXT_NGRAM_MATCHING_THRESHOLD) { // use approximate ngram matching for longer texts
      val ngramSigRatio = r1.matchingNgramSignatureBits(r2)
      return ngramSigRatio >= 0.7f
    }

    val ratio2 = NgramBitSignatures.computeSignatureOverlap(
      r1.shortNgramBitset,
      r2.shortNgramBitset
    )
    if (ratio2 < 0.60) {
      return false
    }

    // very short texts are compared using levenshtein distance if unigram/bigram prefilter passes
    val dist = LevenshteinDistance.distance(r1.text, r2.text).toDouble
    val len = avgLen.toDouble
    (dist / len) < 0.3
  }

  private def checkTextSimilarity(
      row: DuplicateCandidateRow,
      o: DuplicateCandidateRow
  ): Boolean = {
    val nbits = row.matchingSignatureBits(o)
    nbits >= matchThreshold && textOverlaps(row, o)
  }

  private def checkSimilarityGroup(
      group: RowBuffer[DuplicateCandidateRow],
      item: DuplicateCandidateRow
  ): Boolean = {
    val iter = group.iterator()
    val itemLength = item.text.length
    while (iter.hasNext) {
      val other = iter.next()
      if (other.allowedLength(itemLength) && checkTextSimilarity(item, other)) {
        return true
      }
    }
    false
  }

  private def checkLengthBucket(
      items: RowBuffer[DuplicateCandidateRow],
      item: DuplicateCandidateRow,
      initGroup: RowBuffer[DuplicateCandidateRow]
  ): RowBuffer[DuplicateCandidateRow] = {
    val iter = items.deletingIterator()
    var group: RowBuffer[DuplicateCandidateRow] = initGroup
    while (iter.hasNext) {
      val other = iter.next()
      if (checkTextSimilarity(item, other)) {
        iter.removeElement().registerInsteadOf(other)
        if (group == null) {
          group = RowBuffer.single(item)
          item.registerInBuffer(group, 0)
        }
        addRowToSimGroup(other, group)
      }
    }
    group
  }

  private def addRowToSimGroup(
      row: DuplicateCandidateRow,
      group: RowBuffer[DuplicateCandidateRow]
  ): Unit = {
    val first = group.get(0)
    val reprHash = math.min(row.reprHash, first.reprHash)
    if (row.reprHash == reprHash) {
      val iter = group.iterator()
      while (iter.hasNext) {
        iter.next().reprHash = reprHash
      }
    } else {
      row.reprHash = reprHash
    }
    val idx = group.addToBuffer(row)
    row.registerInBuffer(group, idx)
  }

  private def checkSimilarityGroups(
      row: DuplicateCandidateRow
  ): RowBuffer[DuplicateCandidateRow] = {
    val groupsIterator = groups.deletingIterator()
    while (groupsIterator.hasNext) {
      val group = groupsIterator.next()
      if (group.isEmpty) {
        groupsIterator.removeElement()
      }
      if (checkSimilarityGroup(group, row)) {
        addRowToSimGroup(row, group)
        return group
      }
    }
    null
  }

  private def checkReprHashes(row: DuplicateCandidateRow): Unit = {
    val rowLength = row.text.length
    val rowLenIndex = rowLength / 10
    val rowMaxDiff = math.min((rowLength.toLong * 3 + 9) / 10, 50).toInt
    val minLenIndex = math.max(rowLength - rowMaxDiff, 0) / 10
    val maxLenIndex = (rowLength + rowMaxDiff) / 10

    // 1. look matching entry in similarity groups
    val initSimGroup = checkSimilarityGroups(row)

    var simGroup = initSimGroup
    // 2. check lengths groups
    var lenIdx = minLenIndex
    while (lenIdx <= maxLenIndex) {
      val bucket = lengthBuckets.get(lenIdx)
      if (bucket != null) {
        simGroup = checkLengthBucket(bucket, row, simGroup)
        if (bucket.isEmpty) {
          lengthBuckets.remove(lenIdx)
        }
      }
      lenIdx += 1
    }

    if (simGroup != null) {
      if (initSimGroup == null) { // newly created simgroup, need to register it
        groups.add(simGroup)
      }
    } else { // need to put item in the length bucket
      var bucket = lengthBuckets.get(rowLenIndex)
      if (bucket == null) {
        bucket = new RowBuffer[DuplicateCandidateRow]()
        lengthBuckets.put(rowLenIndex, bucket)
      }
      val idx = bucket.addToBuffer(row)
      row.registerInBuffer(bucket, idx)
    }
  }

  private def removeLengthBucketIfEmpty(row: DuplicateCandidateRow): Unit = {
    val rowLength = row.text.length
    val rowLenIndex = rowLength / 10
    val group = lengthBuckets.get(rowLenIndex)
    // group can be null if item came from simgroups
    // or length bucket was deleted when item was moved to a simgroup
    if (group != null && row.wasIn(group) && group.isEmpty) {
      lengthBuckets.remove(rowLenIndex)
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
    obj.removeItselfFromBuffer()
    removeLengthBucketIfEmpty(obj)
    currentBytes -= obj.sizeInBytes
    obj.toRow
  }
}

class DeduplicateParagraphs(
    args: DeduplicateParagraphs.Args,
    spark: SparkSession
) {
  import spark.implicits._

  private def prepareBasicData(rawData: DataFrame): DataFrame = {
    val cleanParagraphs =
      udf((x: String) => Paragraphs.extractCleanParagraphs(x))

    val splitDocs = rawData
      .select(
        posexplode(cleanParagraphs(rawData.col("text"))).as(Seq("pos", "text"))
      )

    prepareDataset(splitDocs)
  }

  private def computeReprHashes(basicData: DataFrame): DataFrame = {
    args.shiftIndices.foldLeft(basicData) { (bd, i) =>
      propagateReprHashes(bd, i)
    }
  }

  // propagate repr hashes between documents (paragraphs) which are similar
  def propagateReprHashes(ds: DataFrame, shift: Int): DataFrame = {
    val args = this.args // do not capture outer object
    val shiftSignature =
      udf((x: Array[Byte]) => MathUtil.rotateBitsRight(x, shift))

    ds.withColumn("signature", shiftSignature(ds.col("signature")))
      // need to persist datasets otherwise mapPartitions is called two times :/
      // to fix it cleanly, it is probably required to get into spark sql internals and
      // write a custom generator (probably) which will call our logic
      .persist()
      // sort does not allow to specify number of partitions, so use this sequence of operations
      .repartitionByRange(args.propagatePartitions, $"signature".asc)
      .sortWithinPartitions($"signature".asc)
      .as[DuplicateCandidateRow]
      .mapPartitions(iter => // all logic is in the CandidateRowProcessor class
        new CandidateRowProcessor(
          args.bufferSizeInBytes,
          args.minBitsToMatch,
          iter
        )
      )
      .toDF()
  }

  def prepareDataset(ds: DataFrame): DataFrame = {
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
          "hash" -> xxhash64($"text")
        )
      )
      .withColumn("reprHash", $"hash")

    basicData
  }

  private def saveStats(statistics: DataFrame) = {
    if (args.debug) {
      statistics
        .filter($"hash" =!= $"reprHash")
        .coalesce(args.partitions)
        .persist()
        .sort($"reprHash".asc)
        .write
        .mode(SaveMode.Overwrite)
        .json(args.output)
    } else {
      statistics.write
        .mode(SaveMode.Overwrite)
        .option("compression", "zstd")
        .format("parquet")
        .save(args.output)
    }
  }

  private def debugStats(stats: DataFrame, preparedData: DataFrame) = {
    val statsCols = stats.select("hash", "reprHash", "nearFreq", "exactFreq")
    val dataCols =
      preparedData.select($"text", $"freq" as "rawFreq", $"hash", $"signature")

    val filtered = if (args.intermediate) {
      statsCols
    } else {
      statsCols.where($"nearFreq" > 1)
    }

    val joined = filtered.join(dataCols, "hash")

    joined
      .repartitionByRange(args.partitions, $"nearFreq".desc, $"reprHash".asc)
      .sortWithinPartitions(
        $"nearFreq".desc,
        $"reprHash".asc,
        $"exactFreq".desc,
        $"hash".asc
      )
      .withColumns(
        Map(
          "text" -> regexp_replace($"text", "\n", "\\n"),
          "signature" -> hex($"signature")
        )
      )
      .write
      .mode(SaveMode.Overwrite)
      .csv(args.output)
  }

  private val clampLongToInt =
    udf((x: Long) => math.min(x, Int.MaxValue).toInt).asNonNullable()

  private def computeStats(
      reprHashes: DataFrame,
      filterOnes: Boolean
  ): DataFrame = {
    val cols = reprHashes.select("hash", "reprHash", "freq").persist()

    val totalReprHashes = cols
      .groupBy("reprHash")
      .agg(
        sum("freq").as("nearFreq")
      )
      .select(
        $"reprHash",
        clampLongToInt($"nearFreq").as("nearFreq")
      )

    val filtered =
      if (filterOnes) totalReprHashes.filter($"nearFreq" > 1)
      else totalReprHashes

    cols
      .join(filtered, "reprHash")
      .dropDuplicates("hash")
      .select(
        $"hash",
        $"reprHash",
        clampLongToInt($"freq") as "exactFreq",
        $"nearFreq"
      )
  }

  private def prepareParagraphsForFiltering(
      raw: DataFrame,
      stats: DataFrame
  ): DataFrame = {

    val explodeCols = raw.columns.map {
      case "text" =>
        posexplode(split(raw.col("text"), "\n\n")).as(Seq("pos", "text"))
      case col => raw.col(col)
    }

    val exploded = raw.select(explodeCols.toIndexedSeq: _*)

    val cleanParUdf = udf((s: String) => Paragraphs.extractCleanParagraph(s))

    val cookedDocs = exploded
      .withColumn("cleanText", cleanParUdf($"text"))
      .withColumn("parHash", xxhash64($"cleanText"))

    val joined = cookedDocs.join(stats, $"parHash" === $"hash", "left")

    val basicCols = (if (args.debug) {
                       joined.columns.filter {
                         case "parHash"                => false
                         case "exactFreq" | "nearFreq" => false
                         case _                        => true
                       }
                     } else {
                       joined.columns.filter {
                         case "hash" | "reprHash" | "parHash" | "cleanText" =>
                           false
                         case "exactFreq" | "nearFreq" => false
                         case _                        => true
                       }
                     }).map(joined.col)

    val computedCols = Seq( // common newly computed columns
      when($"exactFreq".isNotNull, $"exactFreq")
        .otherwise(lit(1))
        .as("exactFreq"),
      when($"nearFreq".isNotNull, $"nearFreq")
        .otherwise(lit(1))
        .as("nearFreq")
    )

    joined.select(
      basicCols.toIndexedSeq ++ computedCols: _*
    )
  }

  // compile full documents from paragraphs
  // paragraphs are shuffled because of join with freqs,
  // groupBy op merges them back together, and we use an udf to perform the actual filtering
  private def filterDuplicateDocs(ds: DataFrame): DataFrame = {
    val docParts = Seq("text", "pos", "exactFreq", "nearFreq")

    val allColumns = ds.columns

    val passthroughColumns = allColumns.toSeq
      .filterNot(_ == "docId")
      .filterNot(docParts.contains(_))

    val aggQueryBasicColumns = passthroughColumns
      .map(colName => first(colName).as(colName))
    val aggColumns = docParts.map(x => collect_list(x).as(x))

    val aggOpFirst :: aggOpRest =
      (aggColumns ++ aggQueryBasicColumns).toList: @unchecked

    val aggOpResult = ds.groupBy("docId").agg(aggOpFirst, aggOpRest: _*)

    val args = this.args
    val convertUdf =
      udf(
        (
            text: Array[String],
            pos: Array[Int],
            exactFreq: Array[Int],
            nearFreq: Array[Int]
        ) => {
          val sorted =
            DeduplicateParagraphs.collectDocParts(
              text,
              pos,
              exactFreq,
              nearFreq
            )
          DeduplicateParagraphs.processDocumentParts(args, sorted.toIndexedSeq)
        }
      ).asNonNullable()

    val transformCols = Seq(
      $"docId",
      convertUdf(docParts.map(aggOpResult.col): _*).as("text")
    ) ++ passthroughColumns.map(aggOpResult.col)

    aggOpResult
      .select(
        transformCols: _*
      )
      .filter(octet_length($"text") > 0)
  }

  private def saveReassembled(ds: DataFrame) = {
    val cols = ds.columns.flatMap {
      case "text" | "date" | "charset" => None
      case x                           => Some(ds.col(x))
    }

    ds.select(cols.toIndexedSeq: _*)
      .repartition(args.partitions, $"docId")
      .sortWithinPartitions($"docId", $"url", $"pos")
      .write
      .mode(SaveMode.Overwrite)
      .json(args.output)
  }

  def process(): Unit = {
    val rawData = spark.read.parquet(args.inputs: _*)

    val basicData = prepareBasicData(rawData)

    val reprParagraphs = if (args.hasStage("reprHashes")) {
      computeReprHashes(basicData)
    } else {
      spark.read.parquet(args.cache.get)
    }

    if (args.hasStage("saveReprHashes")) {
      saveStats(reprParagraphs)
      return
    }

    val stats = if (args.hasStage("stats")) {
      computeStats(
        reprParagraphs,
        filterOnes = args.intermediate && args.hasStage("saveStats")
      )
    } else {
      spark.read.parquet(args.cache.get)
    }

    if (args.hasStage("saveStats")) {
      if (args.debug) {
        debugStats(stats, basicData)
      } else {
        saveStats(stats)
      }
      return
    }

    val paragraphsWithFreqs = prepareParagraphsForFiltering(rawData, stats)

    if (args.hasStage("saveReassembled")) {
      saveReassembled(paragraphsWithFreqs)
      return
    }

    val filtered = filterDuplicateDocs(paragraphsWithFreqs)

    // filtered.queryExecution.debug.toFile("""e:\data\nlp\corpora\cc\dups\CC-MAIN-2013-20\codegen""")

    filtered
      .coalesce(args.partitions)
      .write
      .mode(SaveMode.Overwrite)
      .format(args.format)
      .option("compression", args.compression)
      .save(args.output)
  }
}

object DeduplicateParagraphs {

  def collectDocParts(
      text: Array[String],
      pos: Array[Int],
      exactFreq: Array[Int],
      nearFreq: Array[Int]
  ): Array[Paragraph] = {
    val len = text.length
    val result = new Array[Paragraph](len)
    var i = 0
    while (i < len) {
      val (path, par) = Paragraphs.splitPath(text(i))
      result.update(
        i,
        Paragraph(
          path = path,
          text = par,
          index = pos(i),
          exactFreq = exactFreq(i),
          nearFreq = nearFreq(i)
        )
      )
      i += 1
    }
    java.util.Arrays.sort(result, ParagraphIndexCompare)
    result
  }

  private object ParagraphIndexCompare extends Comparator[Paragraph] {
    override def compare(o1: Paragraph, o2: Paragraph): Int =
      java.lang.Integer.compare(o1.index, o2.index)
  }

  private def processDocumentParts(
      args: Args,
      parts: IndexedSeq[Paragraph]
  ): String = {
    val doc = Document(parts)
    val filtered = args.pipeline.applyFilters(doc)
    filtered.copy(paragraphs = doc.paragraphs.filter(_.remove != null)).render()
  }

  // noinspection TypeAnnotation,ScalaWeakerAccess
  class ArgParser(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[String]]()
    val output = opt[String]()
    val numShifts = opt[Int](default = Some(5))
    val cache = opt[String]()
    val partitions = opt[Int](
      descr = "number of partitions for output, default=100",
      default = Some(100)
    )
    val propagatePartitions = opt[Int](
      descr =
        "how many partitions use to propagate (use ~100MB per partition, e.g. 30 for 3GB dataset)",
      default = Some(64)
    )
    val execution =
      opt[String](descr = "stages to execute", default = Some("all"))
    val debug = toggle(default = Some(false))
    val format = opt[String](default = Some("parquet"))
    val compression = opt[String](default = Some("zstd"))
    val intermediate = toggle(default = Some(false))
    val filters = opt[String](
      descr = "filter pipeline configuration",
      default = Some("all_duplicate_paragraphs.conf")
    )
    verify()

    def toArgs: Args = Args(
      inputs = input(),
      output = output(),
      cache = cache.toOption,
      partitions = partitions(),
      simHashSize = 128,
      minNgramSize = 2,
      maxNgramSize = 4,
      numShifts = numShifts(),
      propagatePartitions = propagatePartitions(),
      execution = execution(),
      stages = makeStages(),
      debug = debug(),
      format = format(),
      compression = compression(),
      intermediate = intermediate(),
      pipeline = Pipeline.make(filters())
    )

    def makeStages(): Set[String] = execution.toOption match {
      case None    => Set("")
      case Some(s) => s.split(",").map(_.trim).toSet
    }

  }

  // noinspection jol
  case class Args(
      inputs: Seq[String],
      output: String,
      cache: Option[String],
      partitions: Int,
      simHashSize: Int,
      minNgramSize: Int,
      maxNgramSize: Int,
      execution: String,
      stages: Set[String],
      debug: Boolean,
      intermediate: Boolean,
      format: String,
      compression: String,
      numShifts: Int = -1,
      bufferSizeInBytes: Int = 10000000,
      preFilterRatio: Double = 0.6,
      propagatePartitions: Int = 64,
      pipeline: Pipeline
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

    def hasStage(stage: String): Boolean = stages.contains(stage)
  }

  def main(args: Array[String]): Unit = {
    val argParser = new ArgParser(args.toIndexedSeq)
    val argObj = argParser.toArgs

    SparkSession.builder().master("local[*]").getOrCreate().use { spark =>
      // argObj.cache.foreach(v => spark.sparkContext.setCheckpointDir(v))
      new DeduplicateParagraphs(argObj, spark).process()
    }
  }

}
