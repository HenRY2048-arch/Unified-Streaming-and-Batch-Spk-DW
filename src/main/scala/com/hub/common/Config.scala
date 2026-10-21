package com.hub.common

import java.io.{FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.Properties

import scala.collection.JavaConverters._

/**
 * 配置中心：与生成器保持同一套覆盖语义。
 *
 * 优先级（后者覆盖前者）：classpath `warehouse.properties` → `-Dconfig.file=<path>` → **任意 `-Dkey=value`**。
 * 之所以允许任意 `-D`：跑一次实验不该改文件（例如 `-Dbatch.recompute.days=7`）。
 */
object Config {

  private val props: Properties = load()

  private def load(): Properties = {
    val p = new Properties()
    // 1) classpath 默认值
    Option(getClass.getClassLoader.getResourceAsStream("warehouse.properties")).foreach { in =>
      try p.load(new InputStreamReader(in, StandardCharsets.UTF_8)) finally in.close()
    }
    // 2) 外部配置文件
    sys.props.get("config.file").foreach { path =>
      val in = new FileInputStream(path)
      try p.load(new InputStreamReader(in, StandardCharsets.UTF_8)) finally in.close()
    }
    // 3) -Dkey=value（只取形如 `x.y` 的键，避免把 JVM 参数误当配置）
    sys.props.toMap
      .filter { case (k, _) => k.contains(".") && !k.startsWith("java.") && !k.startsWith("sun.") && !k.startsWith("os.") }
      .foreach { case (k, v) => p.setProperty(k, v) }
    p
  }

  def get(key: String, default: String): String = Option(props.getProperty(key)).map(_.trim).filter(_.nonEmpty).getOrElse(default)
  def getInt(key: String, default: Int): Int = get(key, default.toString).toInt
  def getLong(key: String, default: Long): Long = get(key, default.toString).toLong
  def getDouble(key: String, default: Double): Double = get(key, default.toString).toDouble
  def getBool(key: String, default: Boolean): Boolean = get(key, default.toString).toBoolean

  // ---- 存储 ----
  val lakeRoot: String = get("lake.root", "s3a://crt-lake").stripSuffix("/")
  val checkpointRoot: String = get("checkpoint.root", s"$lakeRoot/_checkpoint")

  // ---- Kafka ----
  val kafkaBootstrap: String = get("kafka.bootstrap.servers", "kafka:19092")
  val topicEvents: String = get("kafka.topic.events", "content_events")
  val topicTruth: String = get("kafka.topic.truth", "content_groundtruth")
  val startingOffsets: String = get("kafka.starting.offsets", "earliest")
  val maxOffsetsPerTrigger: Long = getLong("kafka.max.offsets.per.trigger", 20000L)

  // ---- Spark ----
  val shufflePartitions: Int = getInt("spark.sql.shuffle.partitions", 8)
  val sessionTimeZone: String = get("spark.sql.session.timeZone", "Asia/Shanghai")

  // ---- Redis ----
  val redisHost: String = get("redis.host", "redis")
  val redisPort: Int = getInt("redis.port", 6379)
  val redisKeyPrefix: String = get("redis.key.prefix", "crt:")

  // ---- 批处理 ----
  val recomputeDays: Int = getInt("batch.recompute.days", 2)
  /** 生成器种子目录（Spark 容器内挂载点；见 docker-compose 的 ../RealTime 挂载）。 */
  val seedDir: String = get("mock.data.dir", "/opt/crt/RealTime/data_files")

  // ---- Tier0（流②）----
  val heatWindowsMinutes: Seq[Int] = get("heat.windows.minutes", "60,1440").split(",").map(_.trim.toInt).toSeq
  val heatWeight: Map[String, Double] = Map(
    "exposure" -> getDouble("heat.w.exposure", 0.1),
    "click" -> getDouble("heat.w.click", 1.0),
    "like" -> getDouble("heat.w.like", 3.0),
    "fav" -> getDouble("heat.w.fav", 4.0),
    "comment" -> getDouble("heat.w.comment", 6.0),
    "share" -> getDouble("heat.w.share", 8.0))
  val heatDwellFactor: Double = getDouble("heat.dwell.factor", 0.02)
  val heatDwellCapSeconds: Double = getDouble("heat.dwell.cap.seconds", 30.0)
  val heatTopN: Int = getInt("heat.top.n", 200)
  /** 在线活跃的"近 N 分钟"（事件时间）。 */
  val onlineMinutes: Int = getInt("online.window.minutes", 15)
  /** Redis TTL：缓存不是真值，过期回源重算。 */
  val redisTtlSeconds: Int = getInt("redis.ttl.seconds", 900)

  /** S3A 等 Hadoop 侧参数：所有以 `fs.` 开头的键原样传给 SparkSession。 */
  val hadoopOptions: Map[String, String] =
    props.asScala.toMap.filter { case (k, _) => k.startsWith("fs.") || k.startsWith("mapreduce.") }

  /** 启动时打印一次，便于在日志里确认"这次跑的是哪套配置"。 */
  def summary: String =
    s"""Config(
       |  lake=$lakeRoot,
       |  kafka=$kafkaBootstrap topics=${topicEvents}/${topicTruth},
       |  timezone=$sessionTimeZone shufflePartitions=$shufflePartitions,
       |  redis=$redisHost:$redisPort,
       |  recomputeDays=$recomputeDays,
       |  s3a=${hadoopOptions.keys.mkString(",")}
       |)""".stripMargin
}
