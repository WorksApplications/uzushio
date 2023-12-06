package com.worksap.nlp.uzushio.lib.resources

import com.github.jbaiter.kenlm.Model
import com.worksap.nlp.sudachi.{Config, Dictionary, DictionaryFactory}
import org.apache.spark.SparkFiles

import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.ConcurrentHashMap

trait CachedLocalResource[T] {
  final private val cache = new ConcurrentHashMap[Path, T]()

  def create(p: Path): T

  def get(dict: String): T = {
    val p = resolveLocalPath(dict).orElse(resolveSparkPath(dict)).getOrElse(
      throw new IllegalArgumentException(s"could not find file: $dict")
    )

    cache.computeIfAbsent(
      p,
      p1 => create(p1)
    )
  }

  def resolveLocalPath(str: String): Option[Path] = {
    val p = Paths.get(str)
    if (Files.exists(p) && Files.isRegularFile(p)) {
      Some(p)
    } else None
  }

  def resolveSparkPath(str: String): Option[Path] = {
    resolveLocalPath(SparkFiles.get(str))
  }
}

object Sudachi extends CachedLocalResource[Dictionary] {
  override def create(p: Path): Dictionary = {
    val cfg = Config.defaultConfig().systemDictionary(p)
    new DictionaryFactory().create(cfg)
  }
}

object KenLM extends CachedLocalResource[Model] {
  override def create(p: Path): Model = new Model(p)
}
