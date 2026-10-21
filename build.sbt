// 说明：**正式构建以 pom.xml（Maven + scalac）为准**（环境已容器化，镜像内只有 Java 11、没有 sbt）。
// 本文件保留给 IDE / sbt 用户，版本与 pom.xml 保持一致，避免两套构建打架。
ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "crt-warehouse",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.3" % Provided,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.3" % Provided,
      "io.delta" %% "delta-spark" % "3.2.1" % Provided,
      "redis.clients" % "jedis" % "5.1.2" % Provided,
      "org.scala-lang" % "scala-reflect" % scalaVersion.value
    ),
    run / fork := true,
    run / javaOptions ++= Seq(
      "-Dfile.encoding=UTF-8"
    )
  )
