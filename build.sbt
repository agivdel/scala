ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.1.2"

lazy val root = (project in file("."))
  .settings(
    name := "scala5"
  )

libraryDependencies += "org.scalaj" % "scalaj-http_2.11" % "2.4.2"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "4.0.5"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.36"
