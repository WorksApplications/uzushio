package com.worksap.nlp.uzushio.lib.runners

import com.worksap.nlp.uzushio.lib.warc.{WarcEntryParser, WarcLoader}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf
import org.slf4j.LoggerFactory

object WarcTextExtraction {
  private val logger = LoggerFactory.getLogger("WarcTextExtraction")

  case class Args(
      input: Seq[String],
      output: String
                 )

  def run(args: Args)(spark: SparkSession): Unit = {
    import spark.implicits._

    val warcData = WarcLoader.readWarcFiles(spark.sparkContext, args.input.mkString(","))
    val crawlResponses = warcData.filter(x => x.isResponse && !x.isTruncated)

    val inputDocuments = spark.sparkContext.longAccumulator("inputDocs")
    val convertedDocs = spark.sparkContext.longAccumulator("convertedDocs")

    val items = crawlResponses.mapPartitions({ iter =>
      val converter = new WarcEntryParser()
      iter.flatMap { item =>
        inputDocuments.add(1)
        converter.convert(item).map { x =>
          convertedDocs.add(1)
          x
        }
      }
    }, preservesPartitioning = true)

    val frame = items.toDF()
    frame.write.parquet(args.output)
    logger.info(s"input docs=${inputDocuments.value}, processed=${convertedDocs.value}")

  }
}

object WarcTextExtractionRaw {

  //noinspection TypeAnnotation
  private class Cfg(args: Seq[String]) extends ScallopConf(args) {
    val input = opt[List[String]](required = true)
    val output = opt[String](required = true)
    val languages = opt[List[String]](default = Some(List("ja")))
    verify()

    def asArgs(): WarcTextExtraction.Args = WarcTextExtraction.Args(
      input = input.apply(),
      output = output.apply()
    )
  }

  def main(args: Array[String]): Unit = {
    import com.worksap.nlp.uzushio.lib.utils.Resources._

    val cfg = new Cfg(args)
    SparkSession.builder()
      .master("local")
      .appName("WarcTextExtractor")
      .getOrCreate().use { sc =>
      WarcTextExtraction.run(cfg.asArgs())(sc)
    }

  }
}



