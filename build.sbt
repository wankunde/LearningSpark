name := "LearningSpark"

version := "1.0"

fork := true

// only relevant for Java sources --
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-unchecked", "-deprecation")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.0"

libraryDependencies += "com.hankcs" % "hanlp" % "portable-1.3.4"

libraryDependencies += "com.alibaba" % "fastjson" % "1.2.7"

libraryDependencies += "com.hadoop.gplcompression" % "hadoop-lzo" % "0.4.19"

libraryDependencies += "com.aliyun.emr" % "emr-core" % "1.4.1"
libraryDependencies += "com.aliyun.emr" % "emr-mns_2.10" % "1.4.1"

libraryDependencies += "org.json4s" %% "json4s-jackson" % "{latestVersion}"

// needed to make the hiveql examples run at least on Linux
javaOptions in run += "-XX:MaxPermSize=128M"

scalacOptions += "-target:jvm-1.8"

// note: tested directly using sbt with -java-home pointing to a JDK 1.8