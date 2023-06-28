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

val spark = Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "org.apache.spark" %% "spark-mllib" % "3.3.2",
)

lazy val root = (project in file("."))
  .dependsOn(lib)
  .settings(
    name := "uzushio",
    libraryDependencies ++= Seq(
      "com.typesafe" % "config" % "1.4.2",
      "com.worksap.nlp" % "sudachi" % "0.7.3",
    ),
    libraryDependencies ++= spark.map(_ % Provided),
  )

lazy val lib = (project in file("lib"))
  .settings(
    name := "uzushio-lib",
    libraryDependencies ++= spark.map(_ % Optional),
    libraryDependencies ++= Seq(
      "org.rogach" %% "scallop" % "4.1.0",
      "org.netpreserve.commons" % "webarchive-commons" % "1.1.9" // org.archive.io
        exclude("org.apache.hadoop", "hadoop-core")
        exclude("com.googlecode.juniversalchardet", "juniversalchardet")
      ,
      "org.apache.httpcomponents.core5" % "httpcore5" % "5.2-beta2", // parse http response in warc
      "org.apache.tika" % "tika-core" % "2.8.0",
      "org.apache.tika" % "tika-parser-html-module" % "2.8.0",
      "com.github.pemistahl" % "lingua" % "1.2.2", // language detection
      "com.github.albfernandez" % "juniversalchardet" % "2.4.0", // charset detection
      "org.apache.logging.log4j" % "log4j-core" % "2.20.0" % Optional,
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.20.0" % Optional,
      "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.20.0" % Optional,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.14.2" % Optional,
      "org.scalatest" %% "scalatest" % "3.2.16" % Test
    )
  )