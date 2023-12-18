package com.worksap.nlp.uzushio.lib.runners

import com.worksap.nlp.uzushio.lib.cleaning.Paragraph
import com.worksap.nlp.uzushio.lib.filters.KenLMEvaluator
import com.worksap.nlp.uzushio.lib.resources.{KenLM, Sudachi}
import com.worksap.nlp.uzushio.lib.utils.Paragraphs
import org.apache.spark.sql.{SaveMode, SparkSession}
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
    private lazy val evaluator = KenLMEvaluator.make(sudachi, kenlm, 0.1f)

    def process(par: String): Double = {
      val prob = evaluator.scoreParagraph(Paragraph("body", par))
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

    probs.write.mode(SaveMode.Overwrite).json(opts.output())
  }

}
