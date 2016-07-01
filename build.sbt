name := "intentPredict"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.1" % "provided"

// http://mvnrepository.com/artifact/org.apache.spark/spark-mllib_2.11
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "1.6.1" % "provided"

// http://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "1.6.1" % "provided"

libraryDependencies += "org.json" % "json" % "20160212"

libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.0" % "provided"