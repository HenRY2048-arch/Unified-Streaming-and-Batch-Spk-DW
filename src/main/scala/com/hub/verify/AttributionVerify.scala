// V-01 归因对账所在包。
package com.hub.verify

// 配置、Delta IO、Lake 路径等公共能力。
import com.hub.common._
// count/sum/when/lit/col 等 DataFrame 函数。
import org.apache.spark.sql.functions._
// DataFrame 和 SparkSession 类型。
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * V-01：数仓重建的 last-touch 来源 vs 生成器真值。
 *
 * 关键原则：
 *   - truth 是“标准答案”，必须从 truth 出发左连接；
 *   - 不能用 inner join，否则 truth 有、DWD 没找到的转化会静默消失；
 *   - event_id 是主键，conversion_type 用来区分互动/关注类型；
 *   - 精确率看 joined 集合内渠道+来源内容是否一致；
 *   - 召回率看 truth 是否被数仓找到。
 */
object AttributionVerify {

  def run(spark: SparkSession, sinceDt: String): DataFrame = {
    // 读取真相层，只保留本次窗口。
    val truth = DeltaIO.read(spark, Lake.odsTruth)
      .filter(col("dt") >= sinceDt)
      // truth 侧字段：渠道和 item 加 t_ 前缀，避免与仓库侧同名冲突。
      .select(col("event_id"), col("conversion_type"),
        col("channel").as("t_channel"), col("item_id").as("t_item"), col("dt"))

    // 互动类仓库侧：like/fav/share/comment/dislike 都在 dwd/interaction。
    val inter = DeltaIO.read(spark, Lake.dwdInteraction)
      .filter(col("dt") >= sinceDt)
      // 把 action 改名为 conversion_type，和 truth 对齐。
      .select(col("event_id"), col("action").as("conversion_type"),
        col("channel"), col("item_id"), lit(true).as("w_exists"))

    // 关注类仓库侧：follow 在 dwd/relation；来源内容字段是 source_item_id。
    val rel = DeltaIO.read(spark, Lake.dwdRelation)
      .filter(col("dt") >= sinceDt && col("action") === "follow")
      .select(col("event_id"), col("action").as("conversion_type"),
        col("channel"), col("source_item_id").as("item_id"), lit(true).as("w_exists"))

    // 把互动和关注拼成统一的仓库侧集合。
    val warehouse = inter.unionByName(rel)

    // 从 truth 出发做 left join：
    //   w_exists=true  -> 仓库找到了这条转化（joined）
    //   w_exists=null  -> 仓库没找到（missing）
    truth.join(warehouse.withColumnRenamed("channel", "w_channel")
      .withColumnRenamed("item_id", "w_item"), Seq("event_id", "conversion_type"), "left")
      // joined：仓库是否找到这条 truth。
      .withColumn("joined", col("w_exists").isNotNull)
      // channel_ok：truth 渠道是否等于仓库渠道；混沌事件可能两边都无来源，null=null 也算一致。
      .withColumn("channel_ok", col("t_channel") <=> col("w_channel"))
      // item_ok：使用 <=> 处理 null=null（例如主页关注没有来源内容）。
      .withColumn("item_ok", col("t_item") <=> col("w_item"))
      // matched：渠道和来源内容都一致才算命中。
      .withColumn("matched", col("channel_ok") && col("item_ok"))
      // 按日期和转化类型汇总。
      .groupBy("dt", "conversion_type")
      .agg(
        // truth 行数：分母是标准答案总数。
        count(lit(1)).as("total_cnt"),
        // joined：仓库能找到的 truth 数。
        sum(when(col("joined"), 1).otherwise(0)).as("joined_cnt"),
        // matched：渠道+来源内容都正确的数。
        sum(when(col("matched"), 1).otherwise(0)).as("matched_cnt"),
        // missing：truth 有、仓库没找到的数。
        sum(when(not(col("joined")), 1).otherwise(0)).as("missing_cnt"),
        // 渠道单独命中数，用于定位“渠道错还是来源内容错”。
        sum(when(col("channel_ok"), 1).otherwise(0)).as("channel_match_cnt"),
        // 来源内容单独命中数。
        sum(when(col("item_ok"), 1).otherwise(0)).as("item_match_cnt"))
      // 精确率：命中的 / 仓库找到的。
      .withColumn("match_rate",
        when(col("joined_cnt") > 0, col("matched_cnt") / col("joined_cnt")).otherwise(lit(null)))
      // 召回率：仓库找到的 / truth 总数。
      .withColumn("recall",
        when(col("total_cnt") > 0, col("joined_cnt") / col("total_cnt")).otherwise(lit(null)))
  }

  def write(spark: SparkSession, sinceDt: String): Unit = {
    // 先计算本次窗口的对账 DataFrame。
    val out = run(spark, sinceDt)
    // 已有 verify/attribution 时，只覆盖 sinceDt 之后的分区。
    if (DeltaIO.exists(spark, Lake.verifyAttribution)) {
      DeltaIO.overwriteWhere(out, Lake.verifyAttribution, s"dt >= '$sinceDt'")
    } else {
      // 第一次运行时创建按 dt 分区的 Delta 表。
      DeltaIO.write(out, Lake.verifyAttribution, Seq("dt"), "overwrite")
    }
    // 打印输出行数，便于确认是否为空。
    println(s"[V-01] rows=${out.count()}")
  }
}
