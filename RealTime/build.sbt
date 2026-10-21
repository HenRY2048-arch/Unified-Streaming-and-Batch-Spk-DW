name := "realtime-consumer"

version := "0.1"

scalaVersion := "2.12.15"

val sparkVersion = "3.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
)

// 全局排除冲突的日志实现
excludeDependencies ++= Seq(
  ExclusionRule("org.apache.logging.log4j", "log4j-slf4j-impl"),
  ExclusionRule("org.slf4j", "slf4j-log4j12")
)

// 显式指定我们需要的日志实现
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.36"
