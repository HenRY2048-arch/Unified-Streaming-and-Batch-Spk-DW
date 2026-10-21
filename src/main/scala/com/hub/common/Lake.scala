package com.hub.common

import org.apache.spark.sql.SparkSession

/**
 * 湖上的目录契约（与 `Warehouse_Design.md` §3.2 一一对应）。
 *
 * 只在此处拼路径：表名散落在各处时，重命名会变成一场"全局搜索 + 祈祷"。
 */
object Lake {
  private def p(rel: String): String = s"${Config.lakeRoot}/$rel"

  // ODS（流①写入）
  val odsEvents: String = p("ods/events")
  val odsTruth: String = p("ods/groundtruth")

  // Tier0 流侧快照（流②写入；V-02 的唯一对账依据）
  val tier0Heat: String = p("tier0/heat_snapshot")
  val tier0Cheat: String = p("tier0/cheat_events")

  // DIM（批）
  val dimUser: String = p("dim/user")
  val dimItem: String = p("dim/item")
  val dimEdge: String = p("dim/edge")

  // DWD（批）
  val dwdExposure: String = p("dwd/exposure")
  val dwdClick: String = p("dwd/click")
  val dwdConsume: String = p("dwd/consume")
  val dwdInteraction: String = p("dwd/interaction")
  val dwdRelation: String = p("dwd/relation")
  val dwdPublish: String = p("dwd/publish")
  val dwdSession: String = p("dwd/session")

  // DWS（批）
  val dwsFunnelHourly: String = p("dws/funnel_hourly")
  val dwsChannelHourly: String = p("dws/channel_hourly")
  val dwsContentDaily: String = p("dws/content_daily")
  val dwsAuthorDaily: String = p("dws/author_daily")
  val dwsRelationDaily: String = p("dws/relation_daily")
  val dwsSessionDaily: String = p("dws/session_daily")
  val dwsUserDaily: String = p("dws/user_daily")
  val dwsQualityDaily: String = p("dws/quality_daily")
  val dwsPositionDaily: String = p("dws/position_daily")
  val dwsRetentionDaily: String = p("dws/retention_daily")
  val dwsColdStartDaily: String = p("dws/cold_start_daily")

  // 自检 / 语义层
  val verifyAttribution: String = p("verify/attribution")
  val verifyHeat: String = p("verify/heat")
  val semanticMetricDict: String = p("semantic/metric_dict")

  // 编排 / 信任表（P3.8）
  val opsRunManifest: String = p("ops/run_manifest")
  val opsPartitionStatus: String = p("ops/partition_status")
  val opsReports: String = p("ops/reports")

  def checkpoint(name: String): String = s"${Config.checkpointRoot}/$name"
}

/**
 * SparkSession 工厂：把 Delta、S3A、时区这些"环境级"配置集中一次，
 * 三个入口（Stream/Batch/Verify）共用，避免各处配置漂移。
 */
object Spark {
  def session(appName: String): SparkSession = {
    val builder = SparkSession.builder()
      .appName(appName)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.session.timeZone", Config.sessionTimeZone)
      .config("spark.sql.shuffle.partitions", Config.shufflePartitions)
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

    // 注意前缀：Hadoop/S3A 参数必须以 `spark.hadoop.` 开头才会被 Spark 采纳；
    // 直接传 `fs.s3a.endpoint` 会被忽略 → S3A 去连真实 AWS，在无网环境里"静默挂住"（踩过）。
    Config.hadoopOptions.foreach { case (k, v) => builder.config(s"spark.hadoop.$k", v) }

    val spark = builder.getOrCreate()
    // spark-shell / 已有会话的场景：builder 的配置会被忽略，这里把 S3A 等参数补到活动会话上
    Config.hadoopOptions.foreach { case (k, v) =>
      spark.sparkContext.hadoopConfiguration.set(k, v)
    }
    // 日志级别可配：排查"卡在 start()"这类问题时用 `-Dspark.log.level=INFO`（Kafka 的重试都在 INFO 里）
    spark.sparkContext.setLogLevel(Config.get("spark.log.level", "WARN"))
    spark
  }
}
