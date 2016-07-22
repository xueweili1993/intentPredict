name := "intentPredict"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.2" % "provided"

// http://mvnrepository.com/artifact/org.apache.spark/spark-mllib_2.11
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "1.6.2" % "provided"

// http://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "1.6.2" % "provided"

libraryDependencies += "org.json" % "json" % "20160212"

libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.0" % "provided"

//libraryDependencies += "com.github.universal-automata" % "liblevenshtein" % "3.0.0"


// https://mvnrepository.com/artifact/com.rockymadden.stringmetric/stringmetric-core_2.11
libraryDependencies += "com.rockymadden.stringmetric" % "stringmetric-core_2.11" % "0.27.4"


// https://mvnrepository.com/artifact/redis.clients/jedis
libraryDependencies += "redis.clients" % "jedis" % "2.8.1"

libraryDependencies += "com.sanoma.cda" %% "maxmind-geoip2-scala" % "1.5.1"

libraryDependencies += "com.snowplowanalytics"  %% "scala-maxmind-iplookups"  % "0.2.0"


