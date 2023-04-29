// https://www.scala-sbt.org/release/docs/Scala-Files-Example.html
import Dependencies._

ThisBuild / scalaVersion     := "2.12.17"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val serverDependecies = Seq(
  sparkCore,
  sparkSql
)

lazy val root = (project in file("."))
  .settings(
    name := "Scala Project",
    libraryDependencies += scalaTest % Test,
    libraryDependencies ++= serverDependecies
  )
