package com.worksap.nlp.uzushio.cleaning

import collection.JavaConverters._
import java.nio.file.{Path, Paths, Files}
import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import org.apache.spark.sql.Dataset
import java.nio.channels.Pipe

/** Sequencially apply multiple transformers.
  *
  * @param stages
  *   list of transformers to apply
  */
class Pipeline(private var stages: Seq[Transformer] = Seq()) extends Transformer {

  def setStages(value: Seq[Transformer]): Pipeline = {
    stages = value
    this
  }

  override def transform(ds: Dataset[Seq[String]]): Dataset[Seq[String]] = {
    stages.foldLeft(ds)((ds, tr) => tr.transform(ds))
  }

  override def toString(): String = {
    s"Pipeline($stages)"
  }
}

object Pipeline {
  def fromConfig(conf: Config): Pipeline = {
    val stageConfs = conf.getObjectList("stages").asScala.map(_.asInstanceOf[ConfigObject])
    val stages = getStagesFromCompanion(stageConfs)
    new Pipeline(stages)
  }

  /** Instantiate stages based on the config. Use constructor. */
  private def getStagesFromConstructor(confObjs: Seq[ConfigObject]) = {
    confObjs.map(co => {
      val name = co.get("class").unwrapped.asInstanceOf[String]
      getConstructorOf(name).newInstance(co)
    })
  }

  /** Instantiate stages based on the config. Use companion object. */
  private def getStagesFromCompanion(confObjs: Seq[ConfigObject]) = {
    confObjs.map(co => {
      val name = co.get("class").unwrapped.asInstanceOf[String]
      getCompanionOf(name).asInstanceOf[FromConfig].fromConfig(co)
    })
  }

  /** Get a constructor of a class from the given name. */
  private def getConstructorOf(name: String) = {
    val clz = Class.forName(withClassPrefix(name))
    clz.getConstructor(Class.forName("com.typesafe.config.ConfigObject"))
  }

  /** Get a companion object of a class from the given name. */
  private def getCompanionOf(name: String) = {
    val clz = Class.forName(withClassPrefix(name))
    clz.getClassLoader.loadClass(clz.getName + "$").getField("MODULE$").get(null)
  }

  private val classname = this.getClass.getName()
  private val classPrefix = classname.take(classname.lastIndexOf("."))

  /** Append class name prefix if not exists.
    *
    * Note: This assume each transformer classes belong to the same package to this class.
    */
  private def withClassPrefix(name: String): String = {
    if (name.startsWith(classPrefix)) { name }
    else {
      s"$classPrefix.$name"
    }
  }

}
