ThisBuild / version := "0.1-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case PathList("module-info.class") => MergeStrategy.discard
  case PathList("org", "apache", "commons", "logging", _*) =>
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
      "org.apache.httpcomponents.core5" % "httpcore5" % "5.2-beta2", // parse http response in warc
      "org.apache.spark" %% "spark-sql" % "3.2.4" % Provided,
      "org.apache.spark" %% "spark-mllib" % "3.2.4" % Provided,
      "org.apache.tika" % "tika-core" % "2.7.0",
      "org.apache.tika" % "tika-parsers-standard-package" % "2.7.0",
      "org.netpreserve.commons" % "webarchive-commons" % "1.1.9" // org.archive.io
        exclude("org.apache.hadoop", "hadoop-core")
        exclude("com.googlecode.juniversalchardet", "juniversalchardet")
      ,
      "org.rogach" %% "scallop" % "4.1.0",
      "com.typesafe" % "config" % "1.4.2",
      "com.worksap.nlp" % "sudachi" % "0.7.3"
    )
  )
