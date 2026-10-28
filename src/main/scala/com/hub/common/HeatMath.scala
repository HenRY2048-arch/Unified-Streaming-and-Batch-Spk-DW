package com.hub.common

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * 热度公式的**唯一实现**：流②（Tier0）与批侧（V-02 对账）都调这里。
 *
 * 如果两边各写一份，"对账"比的就是"两份代码的差异"，而不是"流近似的误差"——那就没有意义了。
 *
 * 公式：`heat = Σ w_e × count_e + 0.02 × Σ min(dwell, 30)`，窗口为 `(anchor − minutes, anchor]`。
 */
object HeatMath {

  def heatFor(events: DataFrame, anchor: java.sql.Timestamp, minutes: Int): DataFrame = {
    val since = new java.sql.Timestamp(anchor.getTime - minutes.toLong * 60000)
    // Kafka 至少一次语义与 checkpoint 重放会在 ODS 留下重复证据；计算指标前必须按事件主键去重。
    val inWindow = events.filter(col("event_ts") > lit(since) && col("event_ts") <= lit(anchor))
      .dropDuplicates("event_id")

    val exposure = inWindow.filter(col("event_type") === "exposure_batch")
      .select(explode(col("items")).as("it"))
      .select(col("it.item_id").as("item_id"))
    val clicks = inWindow.filter(col("event_type") === "enter_item").select(col("item_id"))
    val interactions = inWindow.filter(col("event_type").isin("like", "fav", "comment", "share", "dislike"))
      .select(col("item_id"), col("event_type"))
    val dwells = inWindow.filter(col("event_type") === "leave_item" && col("duration_sec").isNotNull)
      .select(col("item_id"), least(col("duration_sec"), lit(Config.heatDwellCapSeconds)).as("dwell_capped"))

    val e = exposure.groupBy("item_id").agg(count(lit(1)).as("exposure_cnt"))
    val c = clicks.groupBy("item_id").agg(count(lit(1)).as("click_cnt"))
    val i = interactions.groupBy("item_id").agg(
      sum(when(col("event_type") === "like", 1).otherwise(0)).as("like_cnt"),
      sum(when(col("event_type") === "fav", 1).otherwise(0)).as("fav_cnt"),
      sum(when(col("event_type") === "comment", 1).otherwise(0)).as("comment_cnt"),
      sum(when(col("event_type") === "share", 1).otherwise(0)).as("share_cnt"))
    val d = dwells.groupBy("item_id").agg(sum(col("dwell_capped")).as("dwell_sec"))

    e.join(c, Seq("item_id"), "full_outer")
      .join(i, Seq("item_id"), "full_outer")
      .join(d, Seq("item_id"), "full_outer")
      .na.fill(0)
      .withColumn("heat",
        col("exposure_cnt") * Config.heatWeight("exposure") +
          col("click_cnt") * Config.heatWeight("click") +
          col("like_cnt") * Config.heatWeight("like") +
          col("fav_cnt") * Config.heatWeight("fav") +
          col("comment_cnt") * Config.heatWeight("comment") +
          col("share_cnt") * Config.heatWeight("share") +
          col("dwell_sec") * Config.heatDwellFactor)
      .withColumn("window_minutes", lit(minutes))
  }
}
