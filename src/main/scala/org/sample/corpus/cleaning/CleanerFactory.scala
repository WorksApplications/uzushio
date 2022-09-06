package org.sample.corpus.cleaning

import collection.JavaConverters._
import java.nio.file.{Path, Paths, Files}
import com.typesafe.config.{Config, ConfigFactory, ConfigObject}

class CleanerFactory(conf: Config) {
  conf.checkValid(ConfigFactory.defaultReference(), "stages")

  val stageConfs =
    conf.getObjectList("stages").asScala.map(_.asInstanceOf[ConfigObject])

  def build() = {
    val stages = stageConfs.map(co => {
      val name = co.get("class").unwrapped.asInstanceOf[String]
      val companion = CleanerFactory
        .findCompanionOf(name)
        .asInstanceOf[FromConf]
      companion(co)
    })

    println(s"build: ${stages}")
  }

  override def toString(): String = {
    val sstr = stageConfs.map(_.get("class").unwrapped).mkString(", ")

    s"stages: ${sstr}"
  }
}

object CleanerFactory {
  // todo: way to get this list?
  val providedSettings = Set("chitra", "sudachiDictCorpus", "rmTemplate")

  def from(nameOrPath: String) = {
    if (providedSettings.contains(nameOrPath)) {
      new CleanerFactory(ConfigFactory.load(nameOrPath))
    } else {
      fromFile(Paths.get(nameOrPath))
    }
  }

  def fromFile(path: Path) = {
    if (!Files.exists(path)) {
      throw new java.nio.file.NoSuchFileException(path.toString())
    }
    new CleanerFactory(ConfigFactory.parseFile(path.toFile))
  }

  private def withClassPrefix(name: String): String = {
    val thisname = this.getClass.getName()
    val prefix = thisname.take(thisname.lastIndexOf("."))
    if (name.startsWith(prefix)) { name }
    else { s"${prefix}.${name}" }
  }

  private def rmClassPrefix(name: String): String = {
    name.split(raw"\.").last
  }

  def findCompanionOf(name: String) = {
    val clz = Class.forName(withClassPrefix(name))
    clz.getClassLoader
      .loadClass(clz.getName + "$")
      .getField("MODULE$")
      .get(null)
  }
}
