
organization := "com.david"
version := "0.1-SNAPSHOT"
name := "urban-areas"

val sparkVersion = "2.4.1"
scalaVersion := "2.12.8"

val commonDependencies: Seq[ModuleID] = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.0.4",
  "org.slf4j" % "slf4j-log4j12" % "1.7.10",
  "log4j" % "log4j" % "1.2.17"
)
unmanagedJars in Compile += file("lib/challenge-urban-forest-jar-assembly-0.0.1.jar")
val meta = """META.INF(.)*""".r
val assemblySettings=Seq(assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.first

})

val root = (project in file(".")).
  settings(
    libraryDependencies ++= commonDependencies,
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-Ypartial-unification",
      "-language:_"
    )
  )
