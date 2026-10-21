package com.hub.stream

import com.hub.common._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 流②：ODS → Tier0（秒级服务）。三个指标：
 *   F-07 内容热度（1h / 24h）→ Redis + `tier0/heat_snapshot`（供 V-02 对账）
 *   Q-01 作弊判定（A1 五连评 / A2 五秒十五赞）→ `tier0/cheat_events`（轨迹落库，可审计）
 *   U-04 在线活跃（近 N 分钟有会话的用户数）→ Redis
 *
 * **为什么从 ODS 读、而不是直读 Kafka**：证据层唯一。批与流同源，V-02 的差才只反映"算法差"。
 *
 * **为什么每个微批重算最近窗口、而不是维护状态**：本项目是"小体量验证机制"，重算 24h 的 ODS
 * 在本地只要秒级；换来的是"流侧定义与批侧完全一致"（同一套 SQL/算子），V-02 才有意义。
 * 真实体量下应改为状态化增量（说明见 Warehouse_Design §2）。
 */
object Tier0 {

  def start(spark: SparkSession): StreamingQuery = {
    spark.readStream
      .format("delta")
      .load(Lake.odsEvents)
      .writeStream
      .foreachBatch { (batch: DataFrame, batchId: Long) => process(spark, batch, batchId) }
      .option("checkpointLocation", Lake.checkpoint("stream/tier0"))
      .queryName("tier0")
      .start()
  }

  private def process(spark: SparkSession, batch: DataFrame, batchId: Long): Unit = {
    val maxTs = batch.select(max(col("event_ts"))).collect().head.getTimestamp(0)
    if (maxTs == null) return

    val maxMs = maxTs.getTime
    val since = new java.sql.Timestamp(maxMs - 24L * 3600 * 1000)
    // 重算窗口内的"证据"：只读 ODS，不读 DWD（Tier0 与批侧同源）
    val recent = DeltaIO.read(spark, Lake.odsEvents)
      .filter(col("event_ts") >= lit(since) && col("event_ts") <= lit(maxTs))

    val snapshot = heatSnapshot(recent, maxTs, batchId)
    writeSnapshot(spark, snapshot)

    // 只把聚合后的 top-N 带回 driver 写 Redis（不是明细！）
    val snap = snapshot.collect()
    val idxItem = snapshot.schema.fieldIndex("item_id")
    val idxWin = snapshot.schema.fieldIndex("window_minutes")
    val idxHeat = snapshot.schema.fieldIndex("heat")
    val w1 = Config.heatWindowsMinutes.head
    val w2 = if (Config.heatWindowsMinutes.size > 1) Config.heatWindowsMinutes(1) else w1
    val heatRows = snap.groupBy(_.getLong(idxItem)).map { case (id, rs) =>
      def heatOf(w: Int) = rs.find(_.getInt(idxWin) == w).map(_.getDouble(idxHeat)).getOrElse(0.0)
      (id, heatOf(w1), heatOf(w2))
    }.toSeq.sortBy(r => -r._2).take(Config.heatTopN)
    RedisSink.writeHeat(heatRows, maxMs)

    val onlineCut = new java.sql.Timestamp(maxMs - Config.onlineMinutes.toLong * 60 * 1000)
    val online = recent.filter(col("event_ts") >= lit(onlineCut))
      .select(col("user_id")).distinct().count()
    RedisSink.writeOnline(online, maxMs)

    val cheats = detectCheats(recent)
      .withColumn("dt", date_format(col("window_start"), "yyyy-MM-dd"))
      .withColumn("hour", date_format(col("window_start"), "HH"))
    val cheatCount = cheats.count()
    if (cheatCount > 0) DeltaIO.write(cheats, Lake.tier0Cheat, Seq("dt", "hour"), "append")

    println(s"[Tier0] batch=$batchId maxTs=$maxTs items=${snapshot.count()} online=$online cheats=$cheatCount")
    System.out.flush()
  }

  // ---------------- F-07 热度 ----------------

  private def heatSnapshot(recent: DataFrame, maxTs: java.sql.Timestamp, batchId: Long): DataFrame = {
    Config.heatWindowsMinutes.map { minutes =>
      // 与批侧共用同一份公式（HeatMath），否则 V-02 比的不是"流近似误差"
      HeatMath.heatFor(recent, maxTs, minutes)
        .withColumn("computed_at", lit(new java.sql.Timestamp(maxTs.getTime)))
        .withColumn("batch_id", lit(batchId))
        .withColumn("dt", date_format(lit(new java.sql.Timestamp(maxTs.getTime)), "yyyy-MM-dd"))
        .withColumn("hour", date_format(lit(new java.sql.Timestamp(maxTs.getTime)), "HH"))
    }.reduce(_ unionByName _)
  }

  private def writeSnapshot(spark: SparkSession, snapshot: DataFrame): Unit = {
    // append-only：同一 dt/hour 可能多次写入，对账时按 computed_at 取最新一条
    DeltaIO.write(snapshot, Lake.tier0Heat, Seq("dt", "hour"), "append")
  }

  // ---------------- Q-01 作弊 ----------------

  private def detectCheats(recent: DataFrame): DataFrame = {
    val a1 = recent.filter(col("event_type") === "comment")
      .groupBy(col("user_id"), col("item_id"), window(col("event_ts"), "60 seconds"))
      .agg(count(lit(1)).as("cnt"))
      .filter(col("cnt") >= 5)
      .select(lit("A1").as("rule"), col("user_id"), col("item_id"),
        col("window.start").as("ws"), col("cnt"))

    val a2 = recent.filter(col("event_type") === "like")
      .groupBy(col("user_id"), window(col("event_ts"), "5 seconds"))
      .agg(count(lit(1)).as("cnt"), countDistinct(col("item_id")).as("items"))
      .filter(col("cnt") >= 15 && col("items") >= 10)
      .select(lit("A2").as("rule"), col("user_id"), lit(null).cast("long").as("item_id"),
        col("window.start").as("ws"), col("cnt"))

    a1.unionByName(a2)
      .dropDuplicates("rule", "user_id", "item_id", "ws")
      .withColumnRenamed("ws", "window_start")
  }
}
