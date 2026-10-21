package com.hub.batch

import com.hub.common._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 明细层（批）：ODS → DWD。
 *
 * 这一层的"物理地基"是**曝光展开**：`exposure_batch.items[]` → 一行一条曝光，
 * 之后所有比率的分母（曝光）都从这里来。
 *
 * 口径提醒：
 *  - `dwd/consume.duration_sec` **取 `leave_item` 的字段**（模型真值），不用 `leave_ts−enter_ts`；
 *  - 维表 join 一律 `broadcast`（维表小），不允许把明细拉到 driver；
 *  - 每张表按 `dt` 用 `replaceWhere` 重算窗口内的分区（批为真值）。
 */
object DwdBuilder {

  def buildAll(spark: SparkSession, sinceDt: String): Unit = {
    // 批为真值：Kafka 至少一次 + checkpoint 重置/重放会产生重复事件，这里按 event_id 去重
    val events = DeltaIO.read(spark, Lake.odsEvents).filter(col("dt") >= sinceDt)
      .dropDuplicates("event_id")
    val items = DeltaIO.read(spark, Lake.dimItem)
      .select(col("item_id"), col("author_id"), col("category"))

    val exposure = buildExposure(events, items)
    write(spark, exposure, Lake.dwdExposure, sinceDt, "exposure")

    val click = buildClick(events, items)
    write(spark, click, Lake.dwdClick, sinceDt, "click")

    val consume = buildConsume(events, items)
    write(spark, consume, Lake.dwdConsume, sinceDt, "consume")

    val interaction = buildInteraction(events, items)
    write(spark, interaction, Lake.dwdInteraction, sinceDt, "interaction")

    val relation = buildRelation(events)
    write(spark, relation, Lake.dwdRelation, sinceDt, "relation")

    val publish = buildPublish(events)
    write(spark, publish, Lake.dwdPublish, sinceDt, "publish")

    val session = buildSession(events)
    write(spark, session, Lake.dwdSession, sinceDt, "session")
  }

  private def write(spark: SparkSession, df: DataFrame, path: String, sinceDt: String, name: String): Unit = {
    val predicate = s"dt >= '$sinceDt'"
    if (DeltaIO.exists(spark, path)) {
      DeltaIO.overwriteWhere(df, path, predicate)
    } else {
      DeltaIO.write(df, path, Seq("dt"), "overwrite")
    }
    println(s"[DWD] $name rows=${df.count()} (since $sinceDt)")
  }

  // ---------------- 曝光展开（一行一条曝光） ----------------

  private def buildExposure(events: DataFrame, items: DataFrame): DataFrame =
    events.filter(col("event_type") === "exposure_batch")
      .select(col("batch_id"), col("user_id"), col("session_id"), col("event_ts").as("ts"),
        col("channel"), col("dt"), col("hour"), explode(col("items")).as("it"))
      .select(col("batch_id"), col("user_id"), col("session_id"), col("ts"), col("channel"),
        col("dt"), col("hour"),
        col("it.item_id").as("item_id"),
        col("it.position").as("position"),
        col("it.rank_score").as("rank_score"))
      .join(broadcast(items), Seq("item_id"), "left")
      .select("batch_id", "user_id", "session_id", "ts", "channel", "item_id", "position",
        "rank_score", "author_id", "category", "dt", "hour")

  // ---------------- 点击（未闭环也算） ----------------

  private def buildClick(events: DataFrame, items: DataFrame): DataFrame =
    events.filter(col("event_type") === "enter_item")
      .select(col("event_id"), col("user_id"), col("session_id"), col("event_ts").as("ts"),
        col("item_id"), col("channel"), col("batch_id"), col("position"), col("dt"))
      .withColumn("hour", date_format(col("ts"), "HH"))
      .join(broadcast(items), Seq("item_id"), "left")
      .select("event_id", "user_id", "session_id", "ts", "item_id", "author_id", "category",
        "channel", "batch_id", "position", "hour", "dt")

  // ---------------- 消费片段（enter ↔ leave 配对） ----------------

  private def buildConsume(events: DataFrame, items: DataFrame): DataFrame = {
    val enters = events.filter(col("event_type") === "enter_item")
      .select(col("user_id"), col("session_id"), col("item_id"), col("dt"),
        col("event_ts").as("ts"), lit("E").as("kind"),
        col("event_id").as("enter_event_id"),
        col("channel"), col("batch_id"), col("position"))

    val leaves = events.filter(col("event_type") === "leave_item")
      .select(col("user_id"), col("session_id"), col("item_id"), col("dt"),
        col("event_ts").as("ts"), lit("L").as("kind"),
        col("event_id").as("leave_event_id"),
        col("duration_sec"))

    val ordered = enters
      .withColumn("leave_event_id", lit(null).cast("string"))
      .withColumn("duration_sec", lit(null).cast("double"))
      .unionByName(leaves
        .withColumn("enter_event_id", lit(null).cast("string"))
        .withColumn("channel", lit(null).cast("string"))
        .withColumn("batch_id", lit(null).cast("string"))
        .withColumn("position", lit(null).cast("int")))

    // 每个 leave 向上找"最近一次 enter"（同一 session + item），这就是配对规则
    val w = Window.partitionBy("session_id", "item_id").orderBy("ts")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    ordered
      .withColumn("m_enter", last(col("enter_event_id"), ignoreNulls = true).over(w))
      .withColumn("m_channel", last(col("channel"), ignoreNulls = true).over(w))
      .withColumn("m_batch", last(col("batch_id"), ignoreNulls = true).over(w))
      .withColumn("m_position", last(col("position"), ignoreNulls = true).over(w))
      .withColumn("m_enter_ts", last(when(col("kind") === "E", col("ts")), ignoreNulls = true).over(w))
      .filter(col("kind") === "L" && col("m_enter").isNotNull)
      .select(col("m_enter").as("enter_event_id"), col("leave_event_id"),
        col("user_id"), col("session_id"), col("item_id"),
        col("m_channel").as("channel"), col("m_batch").as("batch_id"),
        col("m_position").as("position"),
        col("m_enter_ts").as("enter_ts"), col("ts").as("leave_ts"),
        coalesce(col("duration_sec"), lit(0.0)).as("duration_sec"), col("dt"))
      .withColumn("hour", date_format(col("enter_ts"), "HH"))
      .join(broadcast(items), Seq("item_id"), "left")
      .select("enter_event_id", "leave_event_id", "user_id", "session_id", "item_id",
        "author_id", "category", "channel", "batch_id", "position",
        "enter_ts", "leave_ts", "duration_sec", "dt", "hour")
  }

  // ---------------- 互动 / 关系 / 产出 / 会话 ----------------

  private val interactionTypes = Seq("like", "fav", "share", "comment", "dislike")

  private def buildInteraction(events: DataFrame, items: DataFrame): DataFrame = {
    // 交互事件本身不带 channel（只有 batch_id）；渠道要从"那一批曝光"反查。
    // 这也是 last-touch 归因能成立的前提：事件的来源 = 产生它的那次分发。
    val batchChannel = events.filter(col("event_type") === "exposure_batch")
      .select(col("batch_id"), col("channel").as("batch_channel"))
      .dropDuplicates("batch_id")

    events.filter(col("event_type").isin(interactionTypes: _*))
      .select(col("event_id"), col("user_id"), col("session_id"), col("event_ts").as("ts"),
        col("item_id"), col("event_type").as("action"), col("channel"), col("batch_id"), col("dt"))
      .withColumn("hour", date_format(col("ts"), "HH"))
      .join(broadcast(batchChannel), Seq("batch_id"), "left")
      .withColumn("channel", coalesce(col("channel"), col("batch_channel")))
      .drop("batch_channel")
      .join(broadcast(items), Seq("item_id"), "left")
      .select("event_id", "user_id", "session_id", "ts", "item_id", "author_id", "category",
        "action", "channel", "batch_id", "dt", "hour")
  }

  private def buildRelation(events: DataFrame): DataFrame =
    events.filter(col("event_type").isin("follow", "unfollow", "dm", "enter_profile"))
      .select(col("event_id"), col("user_id"), col("session_id"), col("event_ts").as("ts"),
        col("event_type").as("action"), col("target_user_id"),
        col("channel"), col("source_item_id"), col("source_user_id"), col("dt"))

  private def buildPublish(events: DataFrame): DataFrame =
    events.filter(col("event_type") === "item_published")
      .select(col("event_id"), col("item_id"), col("author_id"), col("category"),
        split(col("tags"), ",").as("tags"), col("content_len"),
        col("published_ts").cast("timestamp").as("published_ts"),
        col("event_ts").as("ts"), col("dt"))

  private def buildSession(events: DataFrame): DataFrame =
    events.groupBy("session_id", "user_id", "dt")
      .agg(
        min("event_ts").as("start_ts"),
        max("event_ts").as("end_ts"),
        sum(when(col("event_type") === "leave_item", coalesce(col("duration_sec"), lit(0.0)))
          .otherwise(lit(0.0))).as("dwell_sum_sec"),
        sum(when(col("event_type") === "exposure_batch", size(col("items"))).otherwise(lit(0)))
          .as("exposure_cnt"),
        count(when(col("event_type") === "enter_item", lit(1))).as("click_cnt"),
        count(when(col("event_type") === "leave_item", lit(1))).as("consume_cnt"),
        count(when(col("event_type").isin(interactionTypes: _*), lit(1))).as("interact_cnt"),
        count(when(col("event_type") === "follow", lit(1))).as("follow_cnt"),
        count(lit(1)).as("event_cnt"))
      .withColumn("duration_sec",
        unix_timestamp(col("end_ts")) - unix_timestamp(col("start_ts")))
      .select("session_id", "user_id", "start_ts", "end_ts", "duration_sec", "dwell_sum_sec",
        "exposure_cnt", "click_cnt", "consume_cnt", "interact_cnt", "follow_cnt", "event_cnt", "dt")
}
