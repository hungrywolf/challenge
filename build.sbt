name := "challenge"

version := "1.0"

scalaVersion := "2.11.8"

//spark
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0" % "provided"


//test
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.4"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"