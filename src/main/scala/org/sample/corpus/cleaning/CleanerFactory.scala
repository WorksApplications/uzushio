package org.sample.corpus.cleaning

import collection.JavaConverters._
import java.nio.file.{Path, Paths, Files}
import com.typesafe.config.{Config, ConfigFactory, ConfigObject}

class CleanerFactory(conf: Config) {
  conf.checkValid(ConfigFactory.defaultReference(), "stages")

  val stageConfs =
    conf.getObjectList("stages").asScala.map(_.asInstanceOf[ConfigObject])

  def buildViaConstructor() = {
    val stages = stageConfs.map(co => {
      val name = co.get("class").unwrapped.asInstanceOf[String]
      val constructor = CleanerFactory.getConstructorOf(name)
      constructor.newInstance(co)
    })

    println(s"build: ${stages}") // for debug
  }

  def buildViaCompanion() = {
    val stages = stageConfs.map(co => {
      val name = co.get("class").unwrapped.asInstanceOf[String]
      CleanerFactory
        .getCompanionOf(name)
        .asInstanceOf[FromConfig]
        .fromConfig(co)
    })

    println(s"build: ${stages}") // for debug
  }

  override def toString(): String = {
    val sstr = stageConfs.map(_.get("class").unwrapped).mkString(", ")

    s"stages: ${sstr}"
  }
}

object CleanerFactory {
  // todo: way to get this list?
  val providedSettings =
    Set("chitra", "sudachiDictCorpus", "rmTemplate", "warc")

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

  private val classname = this.getClass.getName()
  private val classPrefix = classname.take(classname.lastIndexOf("."))

  private def withClassPrefix(name: String): String = {
    if (name.startsWith(classPrefix)) { name }
    else { s"${classPrefix}.${name}" }
  }

  private val configObjClass = Class.forName("com.typesafe.config.ConfigObject")

  def getConstructorOf(name: String) = {
    val clz = Class.forName(withClassPrefix(name))
    clz.getConstructor(configObjClass)
  }

  def getCompanionOf(name: String) = {
    val clz = Class.forName(withClassPrefix(name))
    clz.getClassLoader
      .loadClass(clz.getName + "$")
      .getField("MODULE$")
      .get(null)
  }
}
