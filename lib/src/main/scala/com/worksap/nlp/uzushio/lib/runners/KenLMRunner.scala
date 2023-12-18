package com.worksap.nlp.uzushio.lib.runners

import com.worksap.nlp.uzushio.lib.resources.{KenLM, Sudachi}
import com.worksap.nlp.uzushio.lib.utils.Paragraphs
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{explode, udf}
import org.rogach.scallop.ScallopConf

object KenLMRunner {

  class Args(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[String]](required = true)
    val output = opt[String](required = true)
    val sudachiDict = opt[String]()
    val kenlmModel = opt[String]()
    val master = opt[String]()
    this.verify()
  }

  class LMPerplexity(sudachi: String, kenlm: String) extends Serializable {

    @transient
    private lazy val tokenizer = Sudachi.get(sudachi).create()

    @transient
    private lazy val evaluator = KenLM.get(kenlm).bufferEvaluator(64 * 1024, 1024)

    def process(par: String): Double = {
      val tokens = tokenizer.tokenize(par)
      val proc = evaluator

      proc.clear()

      val iter = tokens.iterator()
      var continue = true
      while (iter.hasNext && continue) {
        val token = iter.next()
        if (token.normalizedForm() != " ") {
          continue = proc.append(token.surface()) > 0
        }
      }

      val prob = proc.evaluateNoOutliers(0.02f)
      Math.pow(10, -prob)
    }

    def asUdf: UserDefinedFunction = udf((x: String) => process(x))
  }

  def main(args: Array[String]): Unit = {
    val opts = new Args(args)

    val scb = SparkSession.builder()
    opts.master.toOption.foreach(scb.master)

    val sc = scb.getOrCreate()

    val inputs = sc.read.parquet(opts.input(): _*)

    import sc.implicits._

    val splitPars = udf((x: String) => Paragraphs.extractCleanParagraphs(x))

    val pars = inputs.select(explode(splitPars($"text")).as("text")).distinct()

    val ppx = new LMPerplexity(opts.sudachiDict(), opts.kenlmModel())

    val probs = pars.withColumn("perplexity", ppx.asUdf($"text"))
      .repartitionByRange(20, $"perplexity".desc).sortWithinPartitions($"perplexity".desc)

    probs.write.json(opts.output())
  }

}
