import Build._

inThisBuild(
  Seq(
    scalaVersion := V.scala212,
    organization := "com.worksap",
    organizationName := "Works Applications",
    startYear := Some(2023),
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
  crossScalaVersions := Seq(V.scala212),
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

disablePlugins(sbtassembly.AssemblyPlugin)

lazy val root = (project in file("."))
  .aggregate(
    lib,
    core,
    legacy
  )
  .settings(
    name := "uzushio-root"
  )
  .settings(noPublishSettings)
  .settings(commonSettings)

lazy val legacy = (project in file("legacy"))
  .disablePlugins(sbtassembly.AssemblyPlugin)
  .dependsOn(lib)
  .settings(
    libraryDependencies ++= sparkDependencies.map(_ % Provided)
  )

lazy val core = (project in file("core"))
  .enablePlugins(sbtassembly.AssemblyPlugin)
  .settings(
    name := "uzushio",
    libraryDependencies ++= sparkDependencies.map(
      _ % Provided
    )
  )
  .settings(commonSettings)
  .settings(lintSettings)
  .settings(assemblySettings)
  .dependsOn(lib)

lazy val lib = (project in file("lib"))
  .disablePlugins(sbtassembly.AssemblyPlugin)
  .settings(
    name := "uzushio-lib",
    libraryDependencies ++= sparkDependencies.map(_ % Optional),
    libraryDependencies ++= libdependencies,
    scalacOptions ++= (
      if (scalaVersion.value.startsWith("2.")) {
        Seq("-opt:l:inline", "-opt-inline-from:classpath")
      } else {
        Seq.empty
      }
    ),
  )
  .settings(commonSettings)
  .settings(lintSettings)
  .settings(scalaCompatSettings)

lazy val bench = (project in file("bench"))
  .disablePlugins(sbtassembly.AssemblyPlugin)
  .enablePlugins(JmhPlugin)
  .settings(commonSettings)
  .settings(noPublishSettings)
  .dependsOn(lib)
