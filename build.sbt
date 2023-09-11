import Build._

// reload sbt when build definition changes
Global / onChangedBuildSource := ReloadOnSourceChanges

inThisBuild(
  Seq(
    scalaVersion := V.scala212,
    organization := "com.worksap",
    homepage := Some(url("https://github.com/WorksApplications/uzushio")),
    versionScheme := Some("early-semver"),
    developers := List(
      Developer(
        "eiennohito",
        "Arseny Tolmachev",
        "arseny@kotonoha.ws",
        url("https://github.com/eiennohito")
      )
    )
  )
)

lazy val commonSettings = Seq(
  crossScalaVersions := Seq(V.scala212, V.scala213),
  scalacOptions ++= Seq(
    "-feature",
    "-deprecation",
    "-unchecked",
    "-encoding",
    "utf-8"
  ),
  javacOptions ++= Seq(
    "-encoding",
    "utf8",
    "-Xlint:all",
    "-source",
    "1.8",
    "-target",
    "1.8"
  )
)

lazy val root = (project in file("."))
  .aggregate(
    lib,
    core,
    legacy
  )
  .settings(noPublishSettings)
  .settings(commonSettings)

lazy val core = (project in file("core"))
  .settings(
    name := "uzushio",
    libraryDependencies ++= coreDependencies ++ sparkDependencies.map(
      _ % Provided
    )
  )
  .settings(commonSettings)
  .settings(lintSettings)
  .settings(assemblySettings)
  .dependsOn(lib)

lazy val legacy = (project in file("legacy"))
  .dependsOn(lib)
  .settings(commonSettings)
  .settings(lintSettings)
  .settings(
    libraryDependencies ++= sparkDependencies.map(_ % Provided)
  )

lazy val lib = (project in file("lib"))
  .settings(
    name := "uzushio-lib",
    libraryDependencies ++= sparkDependencies.map(_ % Optional),
    libraryDependencies ++= libdependencies,
    scalacOptions ++= Seq("-opt:l:inline", "-opt-inline-from:classpath"),
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
  )
  .settings(commonSettings)
  .settings(lintSettings)
