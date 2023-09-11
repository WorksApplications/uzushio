import sbt._
import sbt.Keys._
import sbtassembly.AssemblyKeys._
import sbtassembly.PathList
import sbtassembly.MergeStrategy
import com.eed3si9n.jarjarabrams.ShadeRule

object Build {
  val commonScalacOpts = Seq(
    "-feature",
    "-deprecation",
    "-unchecked",
    "-encoding",
    "utf-8"
  )
  lazy val noPublishSettings = Seq(
    publish := (()),
    publishLocal := (()),
    publishTo := None
  )
  val V = new {
    val scala212 = "2.12.18"
    val scala213 = "2.13.12"
    val scala3 = "3.3.1"
    val spark = "3.3.2"
    val sudachi = "0.7.3"
  }
  val lintSettings = Def.settings {
    scalacOptions ++= (
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, 12)) =>
          Seq(
            "-Xlint"
          )
        case Some((2, 13)) =>
          Seq(
            "-Xlint",
            "-Wdead-code",
            "-Wextra-implicit",
            "-Wnumeric-widen",
            "-Wunused",
            "-Wvalue-discard"
          )
        case _ => Seq.empty
      }
    )
  }

  val coreDependencies = Seq(
    "com.worksap.nlp" % "sudachi" % V.sudachi
  )
  val sparkDependencies = Seq(
    ("org.apache.spark" %% "spark-sql" % V.spark)
      .cross(CrossVersion.for3Use2_13)
      .exclude("org.scala-lang.modules", "scala-xml_2.13"),
    ("org.apache.spark" %% "spark-mllib" % V.spark)
      .cross(
        CrossVersion.for3Use2_13
      )
      .exclude("org.scala-lang.modules", "scala-xml_2.13")
  )
  val libdependencies = Seq(
    ("org.rogach" %% "scallop" % "4.1.0"),
    "org.netpreserve.commons" % "webarchive-commons" % "1.1.9" // org.archive.io
      exclude ("org.apache.hadoop", "hadoop-core")
      exclude ("com.googlecode.juniversalchardet", "juniversalchardet"),
    "org.apache.httpcomponents.core5" % "httpcore5" % "5.2-beta2", // parse http response in warc
    "org.apache.tika" % "tika-core" % "2.8.0",
    "org.apache.tika" % "tika-parser-html-module" % "2.8.0",
    "com.typesafe" % "config" % "1.4.2",
    "com.optimaize.languagedetector" % "language-detector" % "0.6", // language detection
    "com.github.albfernandez" % "juniversalchardet" % "2.4.0", // charset detection
    "org.apache.logging.log4j" % "log4j-core" % "2.20.0" % Optional,
    "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.20.0" % Optional,
    "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.20.0" % Optional,
    ("com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2" % Optional)
      .cross(CrossVersion.for3Use2_13),
    "org.scalatest" %% "scalatest" % "3.2.16" % Test,
    ("org.scala-lang.modules" %% "scala-xml" % "2.2.0").cross(
      CrossVersion.for3Use2_13
    )
  )
  lazy val scalaCompatSettings = Def.settings(
    libraryDependencies ++= (
      if (scalaVersion.value.startsWith("2.")) {
        Seq(
          "org.scala-lang.modules" %% "scala-collection-compat" % "2.11.0"
        ),
      } else {
        Seq.empty
      }
    )
  )

  lazy val assemblySettings = Def.settings(
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) =>
        MergeStrategy.filterDistinctLines
      case PathList("META-INF", _*)      => MergeStrategy.discard
      case PathList("module-info.class") => MergeStrategy.discard
      case PathList("org", "apache", "commons", "logging", _*) =>
        MergeStrategy.first
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    assembly / assemblyShadeRules := Seq(
      ShadeRule
        .rename(
          "com.google.common.**" -> "shaded.com.google.common.@1"
        )
        .inAll
    )
  )
}
