ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.1.2"

lazy val root = (project in file("."))
  .settings(
    name := "scala5"
  )

libraryDependencies += "org.scalaj" % "scalaj-http_2.11" % "2.4.2"
