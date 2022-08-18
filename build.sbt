ThisBuild / version := "0.1"
ThisBuild / scalaVersion := "2.12.15"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("module-info.class") => MergeStrategy.discard
  case PathList("org", "apache", "commons", "logging", xs @ _*) =>
    MergeStrategy.first
  case x => {
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
  }
}

lazy val root = (project in file("."))
  .settings(
    name := "CorpusCleaning",
    libraryDependencies ++= Seq(
      "org.apache.httpcomponents.core5" % "httpcore5" % "5.2-beta2",
      "org.apache.spark" %% "spark-sql" % "3.2.1" % Provided,
      "org.apache.spark" %% "spark-mllib" % "3.2.1" % Provided,
      "org.apache.hadoop" % "hadoop-client" % "3.2.4" % Provided,
      "org.apache.tika" % "tika-core" % "2.4.1",
      "org.apache.tika" % "tika-parsers-standard-package" % "2.4.1",
      // "com.martinkl.warc" % "warc-hadoop" % "0.1.0",
      "org.netpreserve.commons" % "webarchive-commons" % "1.1.9" exclude ("org.apache.hadoop", "hadoop-core"), // for org.archive.io
      "org.rogach" %% "scallop" % "4.1.0",
      // "com.softwaremill.sttp.client3" %% "core" % "3.7.2",
      "com.worksap.nlp" % "sudachi" % "0.5.3"
    )
  )
