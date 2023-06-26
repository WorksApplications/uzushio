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
    name := "uzushio",
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % "3.2.4" % Provided, // load warc file using hadoop api
      "org.apache.httpcomponents.core5" % "httpcore5" % "5.2-beta2", // parse http response in warc
      "org.apache.spark" %% "spark-sql" % "3.2.4" % Provided,
      "org.apache.spark" %% "spark-mllib" % "3.2.4" % Provided,
      "org.apache.tika" % "tika-core" % "2.7.0",
      "org.apache.tika" % "tika-parsers-standard-package" % "2.7.0",
      "org.netpreserve.commons" % "webarchive-commons" % "1.1.9" exclude ("org.apache.hadoop", "hadoop-core"), // org.archive.io
      "org.rogach" %% "scallop" % "4.1.0",
      "com.typesafe" % "config" % "1.4.2",
      "com.worksap.nlp" % "sudachi" % "0.7.3"
    )
  )
