ThisBuild / version := "0.1"
ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "CorpusProcessing",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.2.1" % Provided,
      "org.apache.spark" %% "spark-mllib" % "3.2.1" % Provided,
      "org.rogach" %% "scallop" % "4.1.0",
      "com.worksap.nlp" % "sudachi" % "0.5.3"
    )
  )
