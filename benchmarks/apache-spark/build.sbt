lazy val renaissanceCore = RootProject(uri("../../renaissance-core"))

// Note: these versions should not be changed here.
// If we want to target a different Spark version, we should instead clone this subproject.
// That way, we make it convenient to run the old versions,
// and avoid the benchmarking of a moving target.
val sparkScalaVersion = "2.12.8"

val sparkVersion = "3.0.0"

lazy val apacheSpark = (project in file("."))
  .settings(
    name := "apache-spark",
    version := (version in renaissanceCore).value,
    organization := (organization in renaissanceCore).value,
    scalafmtConfig := Some(file(".scalafmt.conf")),
    scalaVersion := sparkScalaVersion,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-mllib" % sparkVersion,
      "com.nvidia" %% "rapids-4-spark" % "0.2.0",
      "ai.rapids" % "cudf" % "0.15",
      "commons-io" % "commons-io" % "2.6"
    )
  )
  .dependsOn(
    renaissanceCore
  )
