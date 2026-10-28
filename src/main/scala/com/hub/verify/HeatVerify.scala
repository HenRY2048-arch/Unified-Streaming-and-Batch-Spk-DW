// V-02 热度对账所在包。
package com.hub.verify

// 配置、Delta IO、Lake 路径等公共能力。
import com.hub.common._
// DataFrame 表达式函数。
import org.apache.spark.sql.functions._
// DataFrame 和 SparkSession 类型。
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * V-02：流侧热度快照 vs 批侧同锚点重算。
 *
 * 对账口径：
 *   1) 只读 tier0/heat_snapshot，不读 Redis；
 *   2) 同一 (dt,hour,item,window) 取 computed_at 最新一条；
 *   3) 用同一个 computed_at 作为事件时间锚点，调 HeatMath 重算批侧值。
 */
object HeatVerify {

  def run(spark: SparkSession, sinceDt: String): DataFrame = {
    // 读取流侧热度快照，只取本次窗口。
    val snap = DeltaIO.read(spark, Lake.tier0Heat).filter(col("dt") >= sinceDt)

    // 如果没有快照，返回一个带固定 schema 的空 DataFrame。
    // 这样调用方/写出逻辑不会因为“无列”而报错。
    if (snap.limit(1).count() == 0) {
      return spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
        org.apache.spark.sql.types.StructType(Seq(
          org.apache.spark.sql.types.StructField("dt", org.apache.spark.sql.types.StringType),
          org.apache.spark.sql.types.StructField("hour", org.apache.spark.sql.types.StringType),
          org.apache.spark.sql.types.StructField("item_id", org.apache.spark.sql.types.LongType),
          org.apache.spark.sql.types.StructField("window_minutes", org.apache.spark.sql.types.IntegerType),
          org.apache.spark.sql.types.StructField("heat_stream", org.apache.spark.sql.types.DoubleType),
          org.apache.spark.sql.types.StructField("heat_batch", org.apache.spark.sql.types.DoubleType),
          org.apache.spark.sql.types.StructField("abs_diff", org.apache.spark.sql.types.DoubleType),
          org.apache.spark.sql.types.StructField("rel_diff", org.apache.spark.sql.types.DoubleType),
          org.apache.spark.sql.types.StructField("computed_at", org.apache.spark.sql.types.TimestampType))))
    }

    // 每个 dt/hour/item/window 只保留 computed_at 最大的一条。
    val latest = snap.groupBy("dt", "hour", "item_id", "window_minutes")
      .agg(max("computed_at").as("computed_at"))

    // 用 latest 回连 snap，得到真正的流侧值。
    val streamSide = snap
      .join(latest, Seq("dt", "hour", "item_id", "window_minutes", "computed_at"))
      .select(col("dt"), col("hour"), col("item_id"), col("window_minutes"),
        col("heat").as("heat_stream"), col("computed_at"))

    // 收集所有不同的计算锚点；锚点数量等于微批数量，通常很少。
    val anchors = streamSide.select("computed_at").distinct().collect()
      .map(_.getTimestamp(0)).sortBy(_.getTime)

    // 读取 ODS 事件，作为批侧重算的输入。
    val ods = DeltaIO.read(spark, Lake.odsEvents)

    // 对每个锚点、每个热度窗口，调用与流侧共用的 HeatMath。
    val batchSide = anchors.flatMap { anchor =>
      // 批侧只读锚点前 24h；HeatMath 内部再按窗口截取。
      val from = new java.sql.Timestamp(anchor.getTime - 24L * 3600 * 1000)
      val recent = ods.filter(col("event_ts") > lit(from) && col("event_ts") <= lit(anchor))
      Config.heatWindowsMinutes.map { w =>
        HeatMath.heatFor(recent, anchor, w)
          .select(col("item_id"), col("window_minutes"), col("heat").as("heat_batch"),
            lit(anchor).as("computed_at"))
      }
    }

    // 如果没有任何锚点/结果，返回流侧行 + 空的批侧字段。
    if (batchSide.isEmpty) {
      return streamSide.withColumn("heat_batch", lit(null).cast("double"))
        .withColumn("abs_diff", lit(null).cast("double"))
        .withColumn("rel_diff", lit(null).cast("double"))
    }

    // 流侧和批侧按 item/window/锚点对齐；inner join 只比较双方都有的内容。
    streamSide.join(batchSide.reduce(_ unionByName _), Seq("item_id", "window_minutes", "computed_at"), "inner")
      // 绝对差。
      .withColumn("abs_diff", abs(col("heat_stream") - col("heat_batch")))
      // 相对差：批侧为 0 时留空，避免除零。
      .withColumn("rel_diff",
        when(col("heat_batch") > 0, col("abs_diff") / col("heat_batch")).otherwise(lit(null)))
      // 统一输出 schema。
      .select("dt", "hour", "item_id", "window_minutes",
        "heat_stream", "heat_batch", "abs_diff", "rel_diff", "computed_at")
  }

  def write(spark: SparkSession, sinceDt: String): Unit = {
    // 先计算对账 DataFrame。
    val out = run(spark, sinceDt)
    // 已有表时，只覆盖 sinceDt 之后的分区。
    if (DeltaIO.exists(spark, Lake.verifyHeat)) {
      DeltaIO.overwriteWhere(out, Lake.verifyHeat, s"dt >= '$sinceDt'")
    } else {
      // 第一次创建按 dt/hour 分区的表。
      DeltaIO.write(out, Lake.verifyHeat, Seq("dt", "hour"), "overwrite")
    }
    // 输出行数，便于判断是否有快照。
    println(s"[V-02] rows=${out.count()}")
  }
}
