package com.hub.verify

import com.hub.common._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * V-01 归因对账：**数仓重建的 last-touch 来源** vs **生成器真值**。
 *
 * 比什么：`event_id` 是两边的主键（真值里携带它就是为了回连）。
 * 判定"命中"= **渠道 + 来源内容** 都一致；`batch_id` 只作辅助（关系表没有该字段）。
 *
 * 为什么这条指标独有：真实环境拿不到"分发系统当时到底因为哪条内容/哪个渠道产生了转化"，
 * 模拟器知道，所以可以量化"我们的归因口径有多准"。
 */
object AttributionVerify {

  def run(spark: SparkSession, sinceDt: String): DataFrame = {
    val truth = DeltaIO.read(spark, Lake.odsTruth)
      .filter(col("dt") >= sinceDt)
      .select(col("event_id"), col("conversion_type"),
        col("channel").as("t_channel"), col("item_id").as("t_item"), col("dt"))

    val inter = DeltaIO.read(spark, Lake.dwdInteraction)
      .filter(col("dt") >= sinceDt)
      .select(col("event_id"), col("action").as("conversion_type"),
        col("channel"), col("item_id"), lit(true).as("w_exists"))

    val rel = DeltaIO.read(spark, Lake.dwdRelation)
      .filter(col("dt") >= sinceDt && col("action") === "follow")
      .select(col("event_id"), col("action").as("conversion_type"),
        col("channel"), col("source_item_id").as("item_id"), lit(true).as("w_exists"))

    val warehouse = inter.unionByName(rel)

    // 从 truth 出发左连接：joined=数仓能找到该转化；missing=truth 有、数仓没找到。
    truth.join(warehouse.withColumnRenamed("channel", "w_channel")
      .withColumnRenamed("item_id", "w_item"), Seq("event_id", "conversion_type"), "left")
      .withColumn("joined", col("w_exists").isNotNull)
      .withColumn("channel_ok", col("t_channel") === col("w_channel"))
      .withColumn("item_ok", col("t_item") <=> col("w_item"))
      .withColumn("matched", col("channel_ok") && col("item_ok"))
      .groupBy("dt", "conversion_type")
      .agg(
        count(lit(1)).as("total_cnt"),
        sum(when(col("joined"), 1).otherwise(0)).as("joined_cnt"),
        sum(when(col("matched"), 1).otherwise(0)).as("matched_cnt"),
        sum(when(not(col("joined")), 1).otherwise(0)).as("missing_cnt"),
        sum(when(col("channel_ok"), 1).otherwise(0)).as("channel_match_cnt"),
        sum(when(col("item_ok"), 1).otherwise(0)).as("item_match_cnt"))
      .withColumn("match_rate",
        when(col("joined_cnt") > 0, col("matched_cnt") / col("joined_cnt")).otherwise(lit(null)))
      .withColumn("recall",
        when(col("total_cnt") > 0, col("joined_cnt") / col("total_cnt")).otherwise(lit(null)))
  }

  def write(spark: SparkSession, sinceDt: String): Unit = {
    val out = run(spark, sinceDt)
    if (DeltaIO.exists(spark, Lake.verifyAttribution)) {
      DeltaIO.overwriteWhere(out, Lake.verifyAttribution, s"dt >= '$sinceDt'")
    } else {
      DeltaIO.write(out, Lake.verifyAttribution, Seq("dt"), "overwrite")
    }
    println(s"[V-01] rows=${out.count()}")
  }
}
