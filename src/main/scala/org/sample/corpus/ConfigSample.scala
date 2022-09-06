package org.sample.corpus

import java.nio.file.{Path, Paths}
import org.rogach.scallop.ScallopConf
import org.apache.spark.sql.SparkSession

import com.typesafe.config.ConfigFactory

import org.sample.corpus.cleaning.CleanerFactory

object ConfigSample {
  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    // val input = opt[List[Path]](required = true)
    // val output = opt[Path](default = Some(Paths.get("./out")))

    // provide name or path
    val setting = opt[String](default = Some("chitra"))
    verify()
  }

  def run(spark: SparkSession, conf: Conf): Unit = {
    val cf = CleanerFactory.from(conf.setting())

    cf.buildViaCompanion()
    // println(cf.toString())

    // val fconf = setting.confFile.toOption match {
    //   case Some(path) => { ConfigFactory.load(path) }
    //   case None       => { ConfigFactory.load() }
    // }

    // // println(s"${fconf}")
    // println(s"value: ${fconf.getString("normalizer")}")
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val spark =
      SparkSession.builder().appName(this.getClass.getSimpleName).getOrCreate()

    try { run(spark, conf) }
    finally { spark.stop() }
  }
}
