name := "IndeedAssignment"

version := "0.1"

scalaVersion := "2.11.12"

organization := "com.leobenkel"

lazy val core = RootProject(file("../CommonLib"))

dependsOn(core)

libraryDependencies += "org.rogach" %% "scallop" % "3.1.2"

libraryDependencies += "com.sksamuel.elastic4s" %% "elastic4s-core" % "6.2.3"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.0"
