ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"

lazy val root = (project in file("."))
  .settings(
    name := "scala"
  )
